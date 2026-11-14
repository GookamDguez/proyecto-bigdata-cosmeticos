"""
01 - INGESTA: CSV originales -> Parquet, con mediciones.

Responsable: Gabriela (Fase 1, paso previo «Repositorio, descarga de los
datos y conversión a Parquet»).

Qué hace
--------
1. Verifica que estén los cinco CSV mensuales en data/raw/ y que cada uno
   tenga exactamente las 9 columnas esperadas.
2. Calcula la huella SHA-256 de cada CSV. Sirve para demostrar después
   (actividad 14) que los originales no se modificaron.
3. Convierte cada CSV a Parquet (compresión zstd) en data/interim/parquet/
   SIN cambiar ningún valor. Solo agrega dos columnas de trazabilidad:
   archivo_origen (mes) y fila_origen (número de fila en el CSV).
4. Mide, para cada mes y para cada formato: tamaño en disco, número de
   filas, tiempo de conteo y tiempo de una agregación típica.
5. Intenta cargar cada mes completo con pandas y anota el tiempo y la
   memoria que ocupa (o el error si no alcanza la memoria).

Las mediciones justifican el uso de herramientas Big Data (Polars en modo
streaming + Parquet), como pide la sección 6 de la guía.

Salidas
-------
data/interim/parquet/<mes>.parquet
reports/tablas/01_mediciones_ingesta.csv
reports/tablas/01_huellas_sha256_raw.csv
reports/tablas/01_entorno_ingesta.csv

Uso
---
    python src/01_ingesta.py                # todo
    python src/01_ingesta.py --sin-pandas   # omite la prueba con pandas
    python src/01_ingesta.py --raw "C:/ruta/a/Datos"   # CSV en otra carpeta
"""

import argparse
import csv
import gc
import hashlib
import platform
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (  # noqa: E402
    COLUMNAS_ORIGINALES,
    MESES,
    PARQUET,
    RAW,
    TABLAS,
    csv_de_mes,
    leer_csv,
    parquet_de_mes,
    relativa,
)

GIB = 1024 ** 3
MIB = 1024 ** 2


def huella_sha256(ruta: Path, bloque: int = 8 * MIB) -> str:
    h = hashlib.sha256()
    with ruta.open("rb") as f:
        while True:
            datos = f.read(bloque)
            if not datos:
                break
            h.update(datos)
    return h.hexdigest()


def verificar_encabezado(ruta: Path) -> None:
    with ruta.open("r", encoding="utf-8-sig", newline="") as f:
        columnas = next(csv.reader(f))
    if columnas != COLUMNAS_ORIGINALES:
        raise ValueError(
            f"Esquema inesperado en {ruta.name}.\n"
            f"  Esperado:   {COLUMNAS_ORIGINALES}\n"
            f"  Encontrado: {columnas}"
        )


def cronometrar(funcion):
    inicio = time.perf_counter()
    resultado = funcion()
    return resultado, round(time.perf_counter() - inicio, 2)


def contar(lazy: pl.LazyFrame) -> int:
    return lazy.select(pl.len()).collect(engine="streaming").item()


def agregar(lazy: pl.LazyFrame) -> pl.DataFrame:
    # Agregación típica del proyecto: eventos y usuarios por tipo de evento.
    return (
        lazy.group_by("event_type")
        .agg(
            pl.len().alias("eventos"),
            pl.col("user_id").n_unique().alias("usuarios"),
        )
        .collect(engine="streaming")
    )


def memoria_total_gib():
    """RAM total del equipo, si psutil está instalado (opcional)."""
    try:
        import psutil  # type: ignore

        return round(psutil.virtual_memory().total / GIB, 2)
    except Exception:
        return None


def probar_pandas(ruta: Path) -> dict:
    """Carga un mes completo con pandas, como lo haría un análisis clásico."""
    try:
        import pandas as pd
    except ImportError:
        return {"pandas_estado": "pandas no instalado"}

    try:
        inicio = time.perf_counter()
        df = pd.read_csv(ruta)
        segundos = round(time.perf_counter() - inicio, 2)
        memoria = round(df.memory_usage(deep=True).sum() / GIB, 3)
        del df
        gc.collect()
        return {
            "pandas_estado": "ok",
            "pandas_tiempo_carga_s": segundos,
            "pandas_memoria_gib": memoria,
        }
    except MemoryError:
        gc.collect()
        return {"pandas_estado": "MemoryError: no alcanzó la memoria"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--raw", type=Path, default=RAW,
                        help="Carpeta con los CSV originales")
    parser.add_argument("--sin-pandas", action="store_true",
                        help="No ejecutar la prueba de carga con pandas")
    args = parser.parse_args()

    carpeta_raw = args.raw.resolve()
    PARQUET.mkdir(parents=True, exist_ok=True)
    TABLAS.mkdir(parents=True, exist_ok=True)

    faltantes = [m for m in MESES if not csv_de_mes(m, carpeta_raw).exists()]
    if faltantes:
        raise FileNotFoundError(
            f"Faltan CSV en {carpeta_raw}: "
            + ", ".join(f"{m}.csv" for m in faltantes)
        )

    print("=" * 72)
    print("01 - INGESTA: CSV -> PARQUET")
    print("=" * 72)
    print(f"Origen:  {relativa(carpeta_raw)}")
    print(f"Destino: {relativa(PARQUET)}")

    mediciones, huellas = [], []
    inicio_total = time.perf_counter()

    for mes in MESES:
        ruta_csv = csv_de_mes(mes, carpeta_raw)
        ruta_pq = parquet_de_mes(mes)
        print(f"\n[{mes}]")

        verificar_encabezado(ruta_csv)

        huella, t_huella = cronometrar(lambda: huella_sha256(ruta_csv))
        huellas.append({
            "archivo": ruta_csv.name,
            "bytes": ruta_csv.stat().st_size,
            "sha256": huella,
        })
        print(f"  SHA-256 ({t_huella} s): {huella[:16]}...")

        # --- CSV -------------------------------------------------------
        filas_csv, t_contar_csv = cronometrar(lambda: contar(leer_csv(ruta_csv)))
        _, t_agregar_csv = cronometrar(lambda: agregar(leer_csv(ruta_csv)))
        print(f"  CSV:     {filas_csv:>12,} filas | conteo {t_contar_csv} s"
              f" | agregación {t_agregar_csv} s")

        # --- Conversión --------------------------------------------------
        # Los valores se copian tal cual. Solo se añaden dos columnas
        # de trazabilidad al final.
        consulta = (
            leer_csv(ruta_csv)
            .with_row_index("fila_origen")
            .with_columns(pl.lit(mes).alias("archivo_origen"))
            .select(COLUMNAS_ORIGINALES + ["archivo_origen", "fila_origen"])
        )
        _, t_convertir = cronometrar(
            lambda: consulta.sink_parquet(ruta_pq, compression="zstd")
        )

        # --- Parquet -----------------------------------------------------
        filas_pq, t_contar_pq = cronometrar(lambda: contar(pl.scan_parquet(ruta_pq)))
        _, t_agregar_pq = cronometrar(lambda: agregar(pl.scan_parquet(ruta_pq)))
        print(f"  Parquet: {filas_pq:>12,} filas | conteo {t_contar_pq} s"
              f" | agregación {t_agregar_pq} s | conversión {t_convertir} s")

        rango = pl.scan_parquet(ruta_pq).select(
            pl.col("event_time").min().alias("fecha_min"),
            pl.col("event_time").max().alias("fecha_max"),
        ).collect().row(0, named=True)

        if filas_csv != filas_pq:
            raise RuntimeError(
                f"{mes}: el Parquet tiene {filas_pq:,} filas y el CSV "
                f"{filas_csv:,}. La conversión no es fiel; revisar."
            )

        bytes_csv = ruta_csv.stat().st_size
        bytes_pq = ruta_pq.stat().st_size
        fila = {
            "archivo": ruta_csv.name,
            "fecha_min": rango["fecha_min"],
            "fecha_max": rango["fecha_max"],
            "filas": filas_csv,
            "csv_bytes": bytes_csv,
            "parquet_bytes": bytes_pq,
            "csv_mib": round(bytes_csv / MIB, 1),
            "parquet_mib": round(bytes_pq / MIB, 1),
            "reduccion_tamano_pct": round(100 * (1 - bytes_pq / bytes_csv), 1),
            "csv_conteo_s": t_contar_csv,
            "parquet_conteo_s": t_contar_pq,
            "csv_agregacion_s": t_agregar_csv,
            "parquet_agregacion_s": t_agregar_pq,
            "conversion_s": t_convertir,
        }

        if not args.sin_pandas:
            print("  pandas: cargando el mes completo (puede tardar)...")
            fila.update(probar_pandas(ruta_csv))
            print(f"  pandas: {fila.get('pandas_estado')}"
                  f" | {fila.get('pandas_tiempo_carga_s', '-')} s"
                  f" | {fila.get('pandas_memoria_gib', '-')} GiB en memoria")

        mediciones.append(fila)

    # --- Totales ----------------------------------------------------------
    tabla = pl.DataFrame(mediciones)
    no_sumables = {"archivo", "pandas_estado", "reduccion_tamano_pct",
                   "fecha_min", "fecha_max"}
    sumables = [c for c in tabla.columns if c not in no_sumables]
    total = {c: tabla[c].sum() for c in sumables}
    for c in sumables:
        if tabla[c].dtype.is_float():
            total[c] = round(total[c], 3)
    total["fecha_min"] = tabla["fecha_min"].min()
    total["fecha_max"] = tabla["fecha_max"].max()
    total["archivo"] = "TOTAL"
    total["csv_mib"] = round(total["csv_bytes"] / MIB, 1)
    total["parquet_mib"] = round(total["parquet_bytes"] / MIB, 1)
    total["reduccion_tamano_pct"] = round(
        100 * (1 - total["parquet_bytes"] / total["csv_bytes"]), 1
    )
    tabla = pl.concat([tabla, pl.DataFrame([total])], how="diagonal_relaxed")

    tabla.write_csv(TABLAS / "01_mediciones_ingesta.csv")
    pl.DataFrame(huellas).write_csv(TABLAS / "01_huellas_sha256_raw.csv")

    entorno = {
        "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
        "sistema": f"{platform.system()} {platform.release()}",
        "procesador": platform.processor() or platform.machine(),
        "ram_total_gib": memoria_total_gib(),
        "python": platform.python_version(),
        "polars": pl.__version__,
        "duracion_total_s": round(time.perf_counter() - inicio_total, 1),
    }
    try:
        import pandas as pd
        entorno["pandas"] = pd.__version__
    except ImportError:
        pass
    pl.DataFrame([entorno]).write_csv(TABLAS / "01_entorno_ingesta.csv")

    print("\n" + "=" * 72)
    print(f"Total de filas:  {total['filas']:,}")
    print(f"CSV:     {total['csv_mib']:,.1f} MiB")
    print(f"Parquet: {total['parquet_mib']:,.1f} MiB "
          f"({total['reduccion_tamano_pct']} % menos)")
    if "pandas_memoria_gib" in total and total["pandas_memoria_gib"]:
        print(f"Memoria que necesitaría pandas para los 5 meses juntos "
              f"(suma de los meses): {total['pandas_memoria_gib']:.2f} GiB"
              f" | RAM del equipo: {entorno['ram_total_gib'] or 'no medida'} GiB")
    print(f"Duración total: {entorno['duracion_total_s']} s")
    print(f"\nTablas en: {relativa(TABLAS)}")
    print("Los CSV originales no fueron modificados.")


if __name__ == "__main__":
    main()
