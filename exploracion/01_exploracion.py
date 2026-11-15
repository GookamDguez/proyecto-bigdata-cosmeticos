
from pathlib import Path
import polars as pl
import csv
import json
import time

# ==========================================================
# 1. CONFIGURACIÓN
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent
CARPETA_DATOS = BASE_DIR / "Datos"
CARPETA_RESULTADOS = BASE_DIR / "Resultados"
CARPETA_RESULTADOS.mkdir(parents=True, exist_ok=True)

ARCHIVOS = sorted(CARPETA_DATOS.glob("*.csv"))

if not ARCHIVOS:
    raise FileNotFoundError(
        f"No se encontraron archivos CSV en {CARPETA_DATOS}"
    )

COLUMNAS_ESPERADAS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]

# Todos los archivos deben interpretarse con el mismo esquema.
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

RESUMEN = []
NULOS = []
EVENTOS = []
PRECIOS = []
INTEGRIDAD = []

inicio = time.time()

print("=" * 70)
print("EXPLORACIÓN DE DATOS - ECOMMERCE COSMETICS SHOP")
print("=" * 70)
print(f"Archivos encontrados: {len(ARCHIVOS)}")

# ==========================================================
# 2. PROCESAR CADA ARCHIVO
# ==========================================================

for archivo in ARCHIVOS:

    print("\n" + "=" * 70)
    print(f"PROCESANDO: {archivo.name}")
    print("=" * 70)

    # Revisar encabezados antes de procesar.
    with archivo.open("r", encoding="utf-8-sig", newline="") as f:
        columnas = next(csv.reader(f))

    faltantes = set(COLUMNAS_ESPERADAS) - set(columnas)
    adicionales = set(columnas) - set(COLUMNAS_ESPERADAS)

    if faltantes or adicionales:
        raise ValueError(
            f"Esquema inesperado en {archivo.name}. "
            f"Faltantes: {faltantes}. "
            f"Adicionales: {adicionales}"
        )

    datos = pl.scan_csv(
        archivo,
        schema_overrides=TIPOS,
        infer_schema_length=10000,
        null_values=[""],
        low_memory=True,
    )

    # ------------------------------------------------------
    # 2.1 VOLUMEN Y FECHAS
    # ------------------------------------------------------

    general = datos.select(
        pl.len().alias("registros"),
        pl.col("event_time").min().alias("fecha_min"),
        pl.col("event_time").max().alias("fecha_max"),
        pl.col("product_id").n_unique().alias("productos"),
        pl.col("user_id").n_unique().alias("usuarios"),
        pl.col("user_session").n_unique().alias("sesiones"),
    ).collect(engine="streaming")

    g = general.row(0, named=True)

    RESUMEN.append({
        "archivo": archivo.name,
        "registros": g["registros"],
        "productos_unicos": g["productos"],
        "usuarios_unicos": g["usuarios"],
        "sesiones_unicas": g["sesiones"],
        "fecha_min": g["fecha_min"],
        "fecha_max": g["fecha_max"],
        "tamano_gib": round(
            archivo.stat().st_size / 1024**3, 4
        ),
    })

    print(f"Registros: {g['registros']:,}")
    print(f"Productos únicos: {g['productos']:,}")
    print(f"Usuarios únicos: {g['usuarios']:,}")
    print(f"Sesiones únicas: {g['sesiones']:,}")
    print(f"Desde: {g['fecha_min']}")
    print(f"Hasta: {g['fecha_max']}")

    # ------------------------------------------------------
    # 2.2 VALORES NULOS
    # ------------------------------------------------------

    resultado_nulos = datos.select(
        pl.all().null_count()
    ).collect(engine="streaming")

    print("\nVALORES NULOS:")

    for columna, cantidad in resultado_nulos.row(
        0, named=True
    ).items():

        porcentaje = (
            cantidad / g["registros"] * 100
            if g["registros"] else 0
        )

        NULOS.append({
            "archivo": archivo.name,
            "variable": columna,
            "nulos": cantidad,
            "porcentaje": round(porcentaje, 4),
        })

        print(
            f"{columna:20s} "
            f"{cantidad:>12,} "
            f"({porcentaje:6.2f}%)"
        )

    # ------------------------------------------------------
    # 2.3 DISTRIBUCIÓN DE EVENTOS
    # ------------------------------------------------------

    eventos_mes = (
        datos.group_by("event_type")
        .agg(pl.len().alias("cantidad"))
        .collect(engine="streaming")
        .sort("cantidad", descending=True)
    )

    print("\nTIPOS DE EVENTO:")
    print(eventos_mes)

    for fila in eventos_mes.iter_rows(named=True):
        EVENTOS.append({
            "archivo": archivo.name,
            "evento": fila["event_type"],
            "cantidad": fila["cantidad"],
        })

    # ------------------------------------------------------
    # 2.4 ESTADÍSTICAS DE PRECIOS
    # ------------------------------------------------------

    precios_mes = datos.select(
        pl.col("price").count().alias("validos"),
        pl.col("price").null_count().alias("nulos"),
        (pl.col("price") == 0).sum().alias("ceros"),
        (pl.col("price") < 0).sum().alias("negativos"),
        pl.col("price").min().alias("minimo"),
        pl.col("price").mean().alias("promedio"),
        pl.col("price").median().alias("mediana"),
        pl.col("price").max().alias("maximo"),
    ).collect(engine="streaming")

    p = precios_mes.row(0, named=True)

    PRECIOS.append({
        "archivo": archivo.name,
        **p,
    })

    print("\nESTADÍSTICAS DE PRECIOS:")
    for clave, valor in p.items():
        print(f"{clave}: {valor}")

    # ------------------------------------------------------
    # 2.5 INTEGRIDAD BÁSICA
    # ------------------------------------------------------

    integridad_mes = datos.select(
        pl.col("event_type").is_null()
          .sum().alias("evento_nulo"),

        pl.col("product_id").is_null()
          .sum().alias("producto_nulo"),

        pl.col("user_id").is_null()
          .sum().alias("usuario_nulo"),

        pl.col("user_session").is_null()
          .sum().alias("sesion_nula"),

        pl.col("event_time").is_null()
          .sum().alias("fecha_nula"),

        pl.col("price").is_not_null()
          .and_(pl.col("price") <= 0)
          .sum().alias("precio_no_positivo"),
    ).collect(engine="streaming")

    INTEGRIDAD.append({
        "archivo": archivo.name,
        **integridad_mes.row(0, named=True),
    })

    print("\nINTEGRIDAD:")
    print(integridad_mes)

# ==========================================================
# 3. GUARDAR RESULTADOS
# ==========================================================

tablas = {
    "resumen_archivos": RESUMEN,
    "valores_nulos": NULOS,
    "tipos_eventos": EVENTOS,
    "estadisticas_precios": PRECIOS,
    "integridad_basica": INTEGRIDAD,
}

for nombre, registros in tablas.items():

    salida = CARPETA_RESULTADOS / f"{nombre}.csv"

    pl.DataFrame(registros).write_csv(salida)

    print(f"Guardado: {salida.name}")

# ==========================================================
# 4. RESUMEN GENERAL
# ==========================================================

total_registros = sum(
    fila["registros"] for fila in RESUMEN
)

print("\n" + "=" * 70)
print("RESUMEN GENERAL")
print("=" * 70)

print(f"Archivos analizados: {len(ARCHIVOS)}")
print(f"Total de eventos: {total_registros:,}")
print(f"Tiempo de ejecución: {time.time() - inicio:.1f} segundos")

print("\nResultados guardados en:")
print(CARPETA_RESULTADOS)

print("\nEXPLORACIÓN FINALIZADA")
