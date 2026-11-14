"""
02 - MUESTRA DE TRABAJO: 5 % de los usuarios, semilla 42.

Responsable: Gabriela (Fase 2, paso previo «Crear una muestra de trabajo»).

Por qué por usuario y no por filas
----------------------------------
Si se tomaran filas al azar, una misma sesión quedaría partida (unas
vistas sí, el carrito no) y las tablas de sesiones y usuarios saldrían
falsas. Por eso se eligen usuarios y se conservan TODOS sus eventos de
los cinco meses.

Cómo se elige a un usuario (reproducible en cualquier equipo)
-------------------------------------------------------------
Para cada user_id se calcula SHA-256 del texto "42:<user_id>" y se
convierte a un número entre 0 y 1. Si es menor que 0.05, el usuario entra
en la muestra. El resultado no depende de la versión de Polars, de NumPy
ni del sistema operativo: con la misma semilla siempre salen los mismos
usuarios. La lista queda guardada en usuarios_muestra.csv.

Los eventos sin user_id (si los hubiera) no entran en la muestra.

Entradas:  data/interim/parquet/<mes>.parquet   (salida de 01_ingesta.py)
Salidas:   data/interim/sample/<mes>.parquet
           data/interim/sample/usuarios_muestra.csv
           reports/tablas/02_representatividad_muestra.csv

Uso
---
    python src/02_muestra.py
"""

import hashlib
import sys
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (  # noqa: E402
    FRACCION_MUESTRA,
    MESES,
    MUESTRA,
    PARQUET,
    SEMILLA,
    TABLAS,
    parquet_de_mes,
    relativa,
)

ESCALA = 2 ** 64


def entra_en_muestra(user_id: int) -> bool:
    digest = hashlib.sha256(f"{SEMILLA}:{user_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / ESCALA < FRACCION_MUESTRA


def perfil(lazy: pl.LazyFrame, etiqueta: str) -> pl.DataFrame:
    """Indicadores para comparar la muestra con el total."""
    return lazy.select(
        pl.lit(etiqueta).alias("conjunto"),
        pl.len().alias("eventos"),
        pl.col("user_id").drop_nulls().n_unique().alias("usuarios"),
        pl.col("user_session").drop_nulls().n_unique().alias("sesiones"),
        pl.col("product_id").n_unique().alias("productos"),
        (pl.col("event_type") == "view").mean().mul(100).alias("pct_view"),
        (pl.col("event_type") == "cart").mean().mul(100).alias("pct_cart"),
        (pl.col("event_type") == "remove_from_cart").mean().mul(100)
        .alias("pct_remove_from_cart"),
        (pl.col("event_type") == "purchase").mean().mul(100).alias("pct_purchase"),
        pl.col("brand").is_null().mean().mul(100).alias("pct_brand_nulo"),
        pl.col("category_code").is_null().mean().mul(100)
        .alias("pct_category_code_nulo"),
        pl.col("price").mean().alias("precio_medio"),
        pl.col("price").median().alias("precio_mediano"),
    ).collect(engine="streaming")


def main() -> None:
    faltantes = [m for m in MESES if not parquet_de_mes(m).exists()]
    if faltantes:
        raise FileNotFoundError(
            "Faltan Parquet en data/interim/parquet/: "
            + ", ".join(faltantes)
            + ". Ejecute primero: python src/01_ingesta.py"
        )

    MUESTRA.mkdir(parents=True, exist_ok=True)
    TABLAS.mkdir(parents=True, exist_ok=True)
    inicio = time.perf_counter()

    print("=" * 72)
    print(f"02 - MUESTRA: {FRACCION_MUESTRA:.0%} de usuarios, semilla {SEMILLA}")
    print("=" * 72)

    completo = pl.scan_parquet([parquet_de_mes(m) for m in MESES])

    usuarios = (
        completo.select("user_id")
        .drop_nulls()
        .unique()
        .collect(engine="streaming")
        .sort("user_id")["user_id"]
        .to_list()
    )
    elegidos = [u for u in usuarios if entra_en_muestra(u)]
    lista = pl.DataFrame({"user_id": elegidos}, schema={"user_id": pl.Int64})
    lista.write_csv(MUESTRA / "usuarios_muestra.csv")

    print(f"Usuarios totales:      {len(usuarios):,}")
    print(f"Usuarios en muestra:   {len(elegidos):,} "
          f"({len(elegidos) / len(usuarios):.2%})")

    filtro = pl.col("user_id").is_in(lista["user_id"].implode())
    for mes in MESES:
        destino = MUESTRA / f"{mes}.parquet"
        (
            pl.scan_parquet(parquet_de_mes(mes))
            .filter(filtro)
            .sink_parquet(destino, compression="zstd")
        )
        filas = pl.scan_parquet(destino).select(pl.len()).collect().item()
        print(f"  {mes}: {filas:>10,} eventos -> {destino.name}")

    muestra = pl.scan_parquet([MUESTRA / f"{m}.parquet" for m in MESES])
    comparacion = pl.concat(
        [perfil(completo, "completo"), perfil(muestra, "muestra")]
    )
    comparacion.write_csv(TABLAS / "02_representatividad_muestra.csv")

    print("\nComparación muestra vs. completo:")
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=200,
                   float_precision=2):
        print(comparacion.transpose(include_header=True,
                                    column_names="conjunto"))
    print(f"\nDuración: {time.perf_counter() - inicio:.1f} s")
    print(f"Muestra en: {relativa(MUESTRA)}")


if __name__ == "__main__":
    main()
