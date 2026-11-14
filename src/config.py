"""
Configuración común del proyecto.

Todas las rutas se calculan a partir de la carpeta raíz del repositorio,
así los scripts funcionan igual en Windows, Linux o Google Colab sin
editar nada. Ningún script escribe dentro de data/raw/.
"""

from pathlib import Path

import polars as pl

# ----------------------------------------------------------------------
# Rutas
# ----------------------------------------------------------------------

RAIZ = Path(__file__).resolve().parents[1]

DATA = RAIZ / "data"
RAW = DATA / "raw"                       # CSV originales (no se modifican)
PARQUET = DATA / "interim" / "parquet"   # copia fiel de los CSV en Parquet
MUESTRA = DATA / "interim" / "sample"    # muestra del 5 % de usuarios
PROCESSED = DATA / "processed"           # dataset preparado final

REPORTES = RAIZ / "reports"
TABLAS = REPORTES / "tablas"             # tablas de resultados (CSV)
FIGURAS = REPORTES / "figures"

# ----------------------------------------------------------------------
# Fuente de datos
# ----------------------------------------------------------------------

DATASET_KAGGLE = "mkechinov/ecommerce-events-history-in-cosmetics-shop"
URL_KAGGLE = (
    "https://www.kaggle.com/datasets/"
    "mkechinov/ecommerce-events-history-in-cosmetics-shop"
)

# Orden cronológico de los archivos mensuales.
MESES = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb"]

# Esquema de lectura de los CSV. Es el mismo que usan los scripts de
# exploración del equipo, para que todas las cifras sean comparables.
# event_time se lee como texto ("2019-10-01 00:00:00 UTC"); su
# conversión a fecha-hora se hace en la etapa de limpieza (actividad 9).
ESQUEMA_CSV = {
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
COLUMNAS_ORIGINALES = list(ESQUEMA_CSV)

# Columnas de trazabilidad que se agregan al convertir a Parquet.
# Permiten volver desde cualquier fila procesada a su fila exacta en el
# CSV original (archivo + número de fila, contando desde 0 sin encabezado).
COLUMNAS_TRAZABILIDAD = ["archivo_origen", "fila_origen"]

# ----------------------------------------------------------------------
# Muestra de trabajo
# ----------------------------------------------------------------------

SEMILLA = 42
FRACCION_MUESTRA = 0.05

# ----------------------------------------------------------------------
# Fase 2: valores y decisiones de limpieza
# ----------------------------------------------------------------------

TIPOS_EVENTO = ["view", "cart", "remove_from_cart", "purchase"]
DESCONOCIDO = "unknown"

# Decisiones de la actividad 8 (las define Rodrigo). Por defecto NO se
# elimina ninguna fila: cada problema queda marcado con una columna
# bandera, para que el efecto de eliminarlo se pueda medir antes de
# decidir. Si el equipo decide eliminar, basta cambiar a True y volver a
# ejecutar el proceso; el resumen de limpieza registra cuántas filas se
# eliminaron.
DECISIONES = {
    "eliminar_duplicados_exactos": False,
    "eliminar_sesion_nula": False,
    "recuperar_marca_desde_producto": True,
}

# Las entradas y salidas de la fase 2 se separan según se trabaje con la
# muestra o con los datos completos, para no mezclar resultados.
FUENTES = ("muestra", "completo")


def carpeta_entrada(fuente: str) -> Path:
    return MUESTRA if fuente == "muestra" else PARQUET


def carpeta_limpio(fuente: str) -> Path:
    return DATA / "interim" / "limpio" / fuente


def carpeta_procesado(fuente: str) -> Path:
    return PROCESSED / fuente


def relativa(ruta: Path) -> Path:
    """Ruta relativa a la raíz del proyecto, para mensajes más legibles."""
    try:
        return Path(ruta).resolve().relative_to(RAIZ)
    except ValueError:
        return Path(ruta)


def csv_de_mes(mes: str, carpeta: Path = RAW) -> Path:
    return carpeta / f"{mes}.csv"


def parquet_de_mes(mes: str, carpeta: Path = PARQUET) -> Path:
    return carpeta / f"{mes}.parquet"


def leer_csv(ruta: Path) -> pl.LazyFrame:
    """Lectura perezosa de un CSV original con el esquema fijo."""
    return pl.scan_csv(
        ruta,
        schema_overrides=ESQUEMA_CSV,
        null_values=[""],
        low_memory=True,
    )
