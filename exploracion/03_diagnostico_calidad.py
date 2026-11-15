
from pathlib import Path
import polars as pl
import time

# ==========================================================
# 01. CONFIGURACIÓN
# ==========================================================

BASE = Path(__file__).resolve().parent
DATOS = BASE / "Datos"
SALIDA = BASE / "Resultados" / "03_diagnostico"

SALIDA.mkdir(parents=True, exist_ok=True)

ARCHIVOS = sorted(DATOS.glob("*.csv"))

if not ARCHIVOS:
    raise FileNotFoundError(
        f"No se encontraron CSV en {DATOS}"
    )

TIPOS = {
    "event_time": pl.String,
    "event_type": pl.String,
    "product_id": pl.Int64,
    "category_id": pl.Int64,
    "category_code": pl.String,
    "brand": pl.String,
    "price": pl.Float64,
    "user_id": pl.Int64,
    "user_session": pl.String,
}

COLUMNAS = list(TIPOS)
inicio = time.time()


def leer(archivo):
    return pl.scan_csv(
        archivo,
        schema_overrides=TIPOS,
        null_values=[""],
        low_memory=True,
    )


def guardar(nombre, consulta):
    ruta = SALIDA / f"{nombre}.csv"

    resultado = consulta.collect(engine="streaming")
    resultado.write_csv(ruta)

    print(f"  Guardado: {nombre}.csv")
    print(f"  Filas: {resultado.height:,}")

    return resultado


print("=" * 70)
print("03 - DIAGNÓSTICO DE CALIDAD")
print("=" * 70)

# ==========================================================
# 02. DUPLICADOS EXACTOS POR MES Y EVENTO
# ==========================================================

print("\n[1/6] DUPLICADOS EXACTOS")

resultados_duplicados = []

for archivo in ARCHIVOS:
    print(f"  Procesando {archivo.name}...")

    df = leer(archivo)

    grupos = (
        df.group_by(COLUMNAS)
        .agg(pl.len().alias("repeticiones"))
        .filter(pl.col("repeticiones") > 1)
    )

    resumen = (
        grupos.group_by("event_type")
        .agg(
            pl.len().alias("grupos_duplicados"),
            (pl.col("repeticiones") - 1)
            .sum().alias("filas_excedentes"),
        )
        .collect(engine="streaming")
    )

    for fila in resumen.iter_rows(named=True):
        resultados_duplicados.append({
            "mes": archivo.stem,
            **fila,
        })

pl.DataFrame(
    resultados_duplicados,
    schema={
        "mes": pl.String,
        "event_type": pl.String,
        "grupos_duplicados": pl.Int64,
        "filas_excedentes": pl.Int64,
    },
).write_csv(
    SALIDA / "01_duplicados_por_evento.csv"
)

print("  Guardado: 01_duplicados_por_evento.csv")

# ==========================================================
# 03. PRECIOS NO POSITIVOS
# ==========================================================

print("\n[2/6] PRECIOS ANÓMALOS")

precios = []

for archivo in ARCHIVOS:
    df = leer(archivo)

    resumen = (
        df.filter(pl.col("price") <= 0)
        .group_by("event_type", "price")
        .agg(
            pl.len().alias("eventos"),
            pl.col("product_id")
            .n_unique().alias("productos"),
        )
        .collect(engine="streaming")
    )

    if resumen.height:
        resumen = resumen.with_columns(
            pl.lit(archivo.stem).alias("mes")
        )
        precios.append(resumen)

if precios:
    pl.concat(precios).select(
        "mes", "event_type", "price",
        "eventos", "productos"
    ).write_csv(
        SALIDA / "02_precios_anomalos.csv"
    )

print("  Guardado: 02_precios_anomalos.csv")

# Productos con precios anómalos en compras
consultas = [
    leer(a).filter(
        (pl.col("event_type") == "purchase")
        & (pl.col("price") <= 0)
    )
    .select(
        pl.lit(a.stem).alias("mes"),
        "event_time",
        "product_id",
        "category_id",
        "price",
        "user_id",
        "user_session",
    )
    for a in ARCHIVOS
]

guardar(
    "03_compras_precios_anomalos",
    pl.concat(consultas, how="vertical")
)

# ==========================================================
# 04. CONSISTENCIA DE CATEGORÍAS
# ==========================================================

print("\n[3/6] CATEGORÍAS POR PRODUCTO")

datos = pl.concat(
    [
        leer(a).select(
            "product_id",
            "category_id",
            "brand",
            "price",
            "event_type",
            "user_id",
            "user_session",
            "event_time",
        )
        for a in ARCHIVOS
    ],
    how="vertical",
)

categorias = (
    datos.group_by("product_id")
    .agg(
        pl.col("category_id")
        .n_unique().alias("categorias_distintas"),
        pl.col("category_id")
        .unique().alias("categorias"),
        pl.len().alias("eventos"),
    )
    .filter(pl.col("categorias_distintas") > 1)
)

guardar(
    "04_productos_multicategoria",
    categorias.sort(
        "categorias_distintas",
        descending=True,
    )
)

# ==========================================================
# 05. RECUPERACIÓN POTENCIAL DE MARCAS
# ==========================================================

print("\n[4/6] RECUPERACIÓN DE MARCAS")

marcas = (
    datos.group_by("product_id")
    .agg(
        pl.col("brand")
        .drop_nulls()
        .n_unique().alias("marcas_conocidas"),
        pl.col("brand")
        .drop_nulls()
        .first().alias("marca_candidata"),
        pl.col("brand")
        .null_count().alias("eventos_sin_marca"),
        pl.len().alias("eventos"),
    )
)

guardar(
    "05_recuperacion_marcas",
    marcas.filter(
        (pl.col("marcas_conocidas") == 1)
        & (pl.col("eventos_sin_marca") > 0)
    )
    .select(
        "product_id",
        "marca_candidata",
        "eventos_sin_marca",
        "eventos",
    )
    .sort("eventos_sin_marca", descending=True)
)

guardar(
    "06_resumen_marcas",
    marcas.select(
        pl.len().alias("productos"),
        (pl.col("marcas_conocidas") == 0)
        .sum().alias("productos_sin_marca"),
        (pl.col("marcas_conocidas") == 1)
        .sum().alias("productos_marca_unica"),
        (pl.col("marcas_conocidas") > 1)
        .sum().alias("productos_multimarca"),
        pl.when(pl.col("marcas_conocidas") == 1)
        .then(pl.col("eventos_sin_marca"))
        .otherwise(0)
        .sum().alias("eventos_recuperables"),
    )
)

# ==========================================================
# 06. SESIONES CON MÚLTIPLES USUARIOS
# ==========================================================

print("\n[5/6] SESIONES MULTIUSUARIO")

sesiones = (
    datos.filter(
        pl.col("user_session").is_not_null()
    )
    .group_by("user_session")
    .agg(
        pl.col("user_id")
        .n_unique().alias("usuarios_distintos"),
        pl.col("user_id")
        .unique().alias("usuarios"),
        pl.len().alias("eventos"),
        pl.col("event_type")
        .eq("purchase")
        .sum().alias("compras"),
    )
    .filter(pl.col("usuarios_distintos") > 1)
)

guardar(
    "07_sesiones_multiusuario",
    sesiones.sort(
        "usuarios_distintos",
        descending=True,
    )
)

# ==========================================================
# 07. COMPRAS POR SESIÓN
# ==========================================================

print("\n[6/6] COMPRAS POR SESIÓN")

compras = (
    datos.filter(
        (pl.col("event_type") == "purchase")
        & pl.col("user_session").is_not_null()
    )
    .group_by("user_session")
    .agg(
        pl.len().alias("eventos_compra"),
        pl.col("product_id")
        .n_unique().alias("productos_distintos"),
        pl.col("price").sum().alias(
            "suma_precios"
        ),
        pl.col("event_time")
        .min().alias("primera_compra"),
        pl.col("event_time")
        .max().alias("ultima_compra"),
    )
)

guardar(
    "08_resumen_compras_sesion",
    compras.select(
        pl.len().alias("sesiones_compradoras"),
        pl.col("eventos_compra")
        .mean().alias("eventos_promedio"),
        pl.col("eventos_compra")
        .median().alias("eventos_mediana"),
        pl.col("eventos_compra")
        .max().alias("eventos_maximo"),
        pl.col("suma_precios")
        .mean().alias("suma_precios_promedio"),
    )
)

guardar(
    "09_distribucion_compras_sesion",
    compras.group_by("eventos_compra")
    .agg(
        pl.len().alias("sesiones")
    )
    .sort("eventos_compra")
)

guardar(
    "10_ejemplos_sesiones_compradoras",
    compras.sort(
        "eventos_compra",
        descending=True
    ).limit(100)
)

# ==========================================================
# 08. FINALIZACIÓN
# ==========================================================

print("\n" + "=" * 70)
print("DIAGNÓSTICO FINALIZADO")
print("=" * 70)

print(f"Archivos analizados: {len(ARCHIVOS)}")
print(f"Tiempo: {time.time() - inicio:.1f} segundos")
print(f"Resultados: {SALIDA}")
print("Los archivos originales no fueron modificados.")
