
from pathlib import Path
from collections import Counter
import polars as pl
import time
import gc

# ==========================================================
# 1. CONFIGURACIÓN
# ==========================================================

BASE = Path(__file__).resolve().parent
DATOS = BASE / "Datos"
SALIDA = BASE / "Resultados" / "02_integridad"
TEMP = SALIDA / "_temporales"

SALIDA.mkdir(parents=True, exist_ok=True)
TEMP.mkdir(parents=True, exist_ok=True)

ARCHIVOS = sorted(DATOS.glob("*.csv"))
LOTE = 250_000

if not ARCHIVOS:
    raise FileNotFoundError("No hay archivos CSV en Datos.")

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

print("=" * 70)
print("02 - INTEGRIDAD Y RELACIONES")
print("=" * 70)


# ==========================================================
# 2. PREPARAR DATOS CON POLARS 2.0
# ==========================================================

rutas = []
conteo_eventos = Counter()
total = 0

for archivo in ARCHIVOS:

    print(f"\nProcesando {archivo.name}...")

    # Lectura perezosa compatible con Polars 2.0
    datos_mes = pl.scan_csv(
        archivo,
        schema_overrides=TIPOS,
        null_values=[""],
        low_memory=True,
    )

    # Contar registros y tipos de eventos sin
    # cargar el archivo completo en memoria
    estadisticas = (
        datos_mes
        .group_by("event_type")
        .agg(pl.len().alias("cantidad"))
        .collect(engine="streaming")
    )

    cantidad_mes = 0

    for fila in estadisticas.iter_rows(named=True):
        evento = fila["event_type"]
        cantidad = fila["cantidad"]

        conteo_eventos[evento] += cantidad
        cantidad_mes += cantidad

    total += cantidad_mes

    print(f"  Registros: {cantidad_mes:,}")

    # Convertir directamente de CSV a Parquet.
    # sink_parquet escribe sin materializar todo
    # el archivo en un DataFrame de Python.
    destino = TEMP / f"{archivo.stem}.parquet"

    datos_mes.sink_parquet(
        destino,
        compression="zstd",
        mkdir=True,
    )

    rutas.append(destino)

    print(f"  Parquet generado: {destino.name}")

print(f"\nTotal procesado: {total:,}")
print(f"Archivos temporales: {len(rutas)}")


# ==========================================================
# 3. BASE PEREZOSA CONSOLIDADA
# ==========================================================

datos = pl.scan_parquet(
    str(TEMP / "*.parquet")
)

def guardar(nombre, consulta):
    resultado = consulta.collect(engine="streaming")
    ruta = SALIDA / f"{nombre}.csv"
    resultado.write_csv(ruta)
    print(f"Guardado: {ruta.name}")
    return resultado

# ==========================================================
# 4. PRODUCTOS Y CATEGORÍAS
# ==========================================================

print("\nAnalizando productos y categorías...")

productos = datos.group_by("product_id").agg(
    pl.len().alias("eventos"),
    pl.col("category_id").n_unique().alias(
        "categorias_distintas"
    ),
    pl.col("brand").drop_nulls().n_unique().alias(
        "marcas_distintas"
    ),
    pl.col("brand").null_count().alias(
        "eventos_sin_marca"
    ),
    pl.col("price").min().alias("precio_min"),
    pl.col("price").max().alias("precio_max"),
    pl.col("price").n_unique().alias(
        "precios_distintos"
    ),
)

resumen_productos = productos.select(
    pl.len().alias("productos"),
    (pl.col("categorias_distintas") > 1)
        .sum().alias("productos_multicategoria"),
    (pl.col("marcas_distintas") > 1)
        .sum().alias("productos_multimarca"),
    (pl.col("precios_distintos") > 1)
        .sum().alias("productos_precio_variable"),
    (pl.col("marcas_distintas") == 1)
        .sum().alias("productos_marca_unica"),
    (pl.col("marcas_distintas") == 0)
        .sum().alias("productos_sin_marca"),
)

guardar("01_resumen_productos", resumen_productos)

guardar(
    "02_productos_multicategoria",
    productos.filter(
        pl.col("categorias_distintas") > 1
    ).sort(
        "categorias_distintas",
        descending=True
    ).limit(1000)
)

guardar(
    "03_productos_multimarca",
    productos.filter(
        pl.col("marcas_distintas") > 1
    ).sort(
        "marcas_distintas",
        descending=True
    ).limit(1000)
)

guardar(
    "04_productos_precios_variables",
    productos.filter(
        pl.col("precios_distintos") > 1
    ).sort(
        "eventos",
        descending=True
    ).limit(1000)
)

# ==========================================================
# 5. PRECIOS ANÓMALOS
# ==========================================================

print("\nAnalizando precios anómalos...")

guardar(
    "05_precios_anomalos_por_evento",
    datos.group_by("event_type").agg(
        pl.len().alias("total"),
        (pl.col("price") == 0).sum().alias(
            "precios_cero"
        ),
        (pl.col("price") < 0).sum().alias(
            "precios_negativos"
        ),
        pl.col("price").min().alias("precio_min"),
        pl.col("price").max().alias("precio_max"),
    ).sort("total", descending=True)
)

guardar(
    "06_productos_precios_anomalos",
    datos.filter(
        pl.col("price") <= 0
    ).group_by("product_id").agg(
        pl.len().alias("eventos_anomalos"),
        pl.col("price").min().alias("precio_min"),
        pl.col("price").max().alias("precio_max"),
        pl.col("event_type").n_unique().alias(
            "tipos_evento"
        ),
    ).sort(
        "eventos_anomalos",
        descending=True
    ).limit(1000)
)

# ==========================================================
# 6. RELACIÓN SESIONES Y USUARIOS
# ==========================================================

print("\nAnalizando sesiones y usuarios...")

sesiones = (
    datos.filter(
        pl.col("user_session").is_not_null()
    )
    .group_by("user_session")
    .agg(
        pl.col("user_id").n_unique().alias(
            "usuarios_distintos"
        ),
        pl.len().alias("eventos"),
        pl.col("event_type")
          .eq("purchase")
          .sum().alias("compras"),
    )
)

guardar(
    "07_resumen_sesiones",
    sesiones.select(
        pl.len().alias("sesiones"),
        (pl.col("usuarios_distintos") > 1)
            .sum().alias("sesiones_multiusuario"),
        (pl.col("compras") > 0)
            .sum().alias("sesiones_con_compra"),
    )
)

guardar(
    "08_sesiones_multiusuario",
    sesiones.filter(
        pl.col("usuarios_distintos") > 1
    ).sort(
        "usuarios_distintos",
        descending=True
    ).limit(1000)
)

# ==========================================================
# 7. DUPLICADOS EXACTOS
# ==========================================================

print("\nAnalizando duplicados exactos...")

duplicados = (
    datos.group_by(COLUMNAS)
    .agg(pl.len().alias("repeticiones"))
    .filter(pl.col("repeticiones") > 1)
)

guardar(
    "09_resumen_duplicados",
    duplicados.select(
        pl.len().alias("grupos_duplicados"),
        (pl.col("repeticiones") - 1)
            .sum().alias("filas_excedentes")
    )
)

guardar(
    "10_ejemplos_duplicados",
    duplicados.sort(
        "repeticiones",
        descending=True
    ).limit(100)
)

# ==========================================================
# 8. COMPRAS Y PRECIOS
# ==========================================================

print("\nAnalizando eventos de compra...")

compras = datos.filter(
    pl.col("event_type") == "purchase"
)

guardar(
    "11_resumen_compras",
    compras.select(
        pl.len().alias("eventos_compra"),
        pl.col("product_id").n_unique().alias(
            "productos_comprados"
        ),
        pl.col("user_id").n_unique().alias(
            "usuarios_compradores"
        ),
        pl.col("user_session").n_unique().alias(
            "sesiones_con_compra"
        ),
        pl.col("price").mean().alias(
            "precio_promedio_evento"
        ),
        pl.col("price").median().alias(
            "precio_mediano_evento"
        ),
        pl.col("price").sum().alias(
            "suma_precios_eventos"
        ),
        (pl.col("price") <= 0).sum().alias(
            "compras_precio_no_positivo"
        ),
    )
)

# ==========================================================
# 9. RESUMEN FINAL
# ==========================================================

print("\n" + "=" * 70)
print("RESUMEN FINAL")
print("=" * 70)

print(f"Registros analizados: {total:,}")
print(f"Tipos de evento: {dict(conteo_eventos)}")
print(f"Tiempo: {time.time() - inicio:.1f} segundos")
print(f"Resultados: {SALIDA}")

# ==========================================================
# 10. ELIMINAR TEMPORALES
# ==========================================================

print("\nEliminando archivos temporales...")

for ruta in rutas:
    ruta.unlink()

try:
    TEMP.rmdir()
except OSError:
    pass

print("ANÁLISIS FINALIZADO CORRECTAMENTE")
