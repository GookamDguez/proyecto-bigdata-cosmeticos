# Proyecto de Big Data: comportamiento de compra en una tienda de cosméticos en línea

## Universidad del Salvador
 - Asignatura: Big Data
 - Docente: PhD. Alexander Ramírez Camargo
 - Metodología: CRISP-DM adaptada en cuatro fases (entrega actual: fases 1 y 2).

**Equipo:** 
- Domínguez Hernández, Gabriela Marisol | dh25008
- Sorto Choto, Carlos Alejandro | sc16034
- Francia Quezada, Rodrigo Jesús | fq25003

## Fuente de datos

*eCommerce events history in cosmetics shop*, publicado en Kaggle por mkechinov con
datos de REES46 (Open CDP):
<https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop>

- Cinco CSV mensuales, de octubre de 2019 a febrero de 2020 (2.27 GiB en total).
- 20,692,840 eventos con nueve columnas: `event_time`, `event_type`, `product_id`,
  `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`.
- Licencia declarada: «Data files © Original Authors». No es una licencia abierta,
  así que los datos se usan solo con fines académicos y **no se redistribuyen**.
  Ver [`docs/06_licencia_privacidad_sesgos.md`](docs/06_licencia_privacidad_sesgos.md).

Los datos no se incluyen en este repositorio: ver [`data/README.md`](data/README.md).

## Estructura

```
data/                 datos (no se suben; ver data/README.md)
  raw/                  CSV originales, sin modificar
  interim/parquet/      copia fiel en Parquet
  interim/sample/       muestra del 5 % de usuarios
  interim/limpio/       eventos estandarizados, por mes
  processed/<fuente>/   dataset preparado: eventos.parquet y productos.parquet
src/                  scripts del proceso, numerados en orden de ejecución
notebooks/            02_preparacion.ipynb: explica y ejecuta la fase 2
docs/                 matriz de fuentes, licencia y sesgos, reglas de preparación
reports/tablas/       resultados y verificaciones que generan los scripts
reports/figures/      gráficos
```

## Instalación

Requiere Python 3.11 o superior (probado con 3.13).

```bash
python -m pip install -r requirements.txt
```

## Ejecución

### Todo el proceso, desde cero

Con los cinco CSV en `data/raw/`:

```bash
python src/run_pipeline.py
```

Se ejecutan en orden, sin pasos manuales:

| Paso | Script | Qué hace | Guía |
|---|---|---|---|
| 1 | `src/01_ingesta.py` | CSV → Parquet sin cambiar valores; mide tamaño, filas, tiempos y memoria; registra la huella SHA-256 de cada CSV | F1, paso previo |
| 2 | `src/02_muestra.py` | Muestra del 5 % de usuarios con semilla 42, más su comparación con el total | F2, paso previo |
| 3 | `src/03_limpieza.py` | Fechas UTC, fecha/hora/día, niveles de categoría, textos, tipos y banderas de calidad | F2, act. 9 |
| 4 | `src/04_integracion.py` | Une los 5 meses, crea la tabla de productos, recupera marcas y marca sesiones; verifica la cardinalidad en cada paso | F2, act. 10 |
| 5 | `src/05_verificar_raw.py` | Comprueba que los CSV originales no cambiaron | F2, act. 14 |

### Prueba rápida con la muestra

```bash
python src/run_pipeline.py --fuente muestra
```

Ejecuta solo los pasos 3 y 4 sobre la muestra (`data/interim/sample/`), en segundos.
Todo cambio al código se prueba así primero.

### Opciones

- `--sin-pandas`: omite la prueba de carga con pandas de la ingesta.
- Los scripts también se pueden ejecutar uno por uno, por ejemplo
  `python src/03_limpieza.py --fuente completo`.
- Si los CSV están en otra carpeta: `python src/01_ingesta.py --raw "ruta/a/la/carpeta"`.

### Notebook

`notebooks/02_preparacion.ipynb` explica cada paso de la fase 2 y lo ejecuta mostrando
sus verificaciones. Por defecto usa la muestra; para el dataset final se cambia
`FUENTE = "completo"`. Se abre con VS Code o con `python -m notebook`.

## Dataset preparado

| Archivo | Una fila por | Contenido |
|---|---|---|
| `data/processed/completo/eventos.parquet` | evento | columnas originales estandarizadas, fecha derivada, niveles de categoría, banderas de calidad y trazabilidad |
| `data/processed/completo/productos.parquet` | producto | categoría y marca principales, precios, fechas y banderas del producto |

Las reglas de limpieza e integración, las llaves y la lista de columnas están en
[`docs/fase2_preparacion.md`](docs/fase2_preparacion.md). Las decisiones de la
actividad 8 se cambian en un solo lugar: `DECISIONES`, en `src/config.py`.

## Reglas del repositorio

- Nunca se modifican los archivos de `data/raw/`. Todo cambio a los datos se hace con
  un script guardado en `src/`.
- No se suben datos ni credenciales (`kaggle.json`); `.gitignore` los excluye. Los
  datos se comparten entre los integrantes por Google Drive.
- Cada integrante sube sus propios cambios, como evidencia de su contribución.
