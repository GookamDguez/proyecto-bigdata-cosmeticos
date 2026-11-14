# Carpeta de datos

Los datos **no se suben a GitHub**: la licencia de la fuente no permite
redistribuirlos. Se comparten solo entre los integrantes, por Google Drive.

| Carpeta | Contenido | Quién la genera |
|---|---|---|
| `raw/` | Los cinco CSV originales de Kaggle (2019-Oct a 2020-Feb). **No se modifican nunca.** | Descarga (`src/00_descarga.py`) o copia manual |
| `interim/parquet/` | Copia fiel de cada CSV en Parquet, más las columnas `archivo_origen` y `fila_origen` | `src/01_ingesta.py` |
| `interim/sample/` | Muestra del 5 % de usuarios (semilla 42) con todos sus eventos | `src/02_muestra.py` |
| `interim/limpio/<fuente>/` | Eventos estandarizados y con banderas de calidad, por mes | `src/03_limpieza.py` |
| `processed/<fuente>/` | Dataset preparado: `eventos.parquet` y `productos.parquet` | `src/04_integracion.py` |

`<fuente>` es `muestra` o `completo`.

Para comprobar que los CSV originales no cambiaron, ejecute
`python src/05_verificar_raw.py`, que compara sus huellas SHA-256 con las de
`reports/tablas/01_huellas_sha256_raw.csv`.
