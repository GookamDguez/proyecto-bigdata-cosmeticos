"""
03 - LIMPIEZA Y ESTANDARIZACIÓN (actividad 9 de la guía).

Responsable: Gabriela (Fase 2). Es el script «src/clean.py» del plan.

Qué hace, mes por mes
---------------------
1. Fechas: convierte event_time de texto ("2019-10-01 00:00:04 UTC") a
   fecha-hora con zona UTC y crea fecha, hora y dia_semana (1 = lunes,
   7 = domingo). La fuente no indica el país de la tienda, por lo que la
   hora es la de UTC y no la hora local del cliente.
2. Categorías: divide category_code en tres niveles. Si falta (98 % de
   los eventos), los niveles quedan en "unknown"; si el código tiene solo
   dos niveles, el tercero queda vacío (no aplica).
3. Textos: pasa a minúsculas y quita espacios sobrantes en event_type,
   category_code y brand, y cuenta cuántos valores cambiaron.
4. Nombres de columnas: las nueve originales ya están en snake_case y se
   conservan tal cual para mantener la trazabilidad con la fuente; las
   columnas nuevas también se nombran en snake_case.
5. Tipos: usa tipos más livianos (Enum, enteros de 32 bits, Categorical)
   y mide cuánta memoria se ahorra.
6. Decisiones de la actividad 8: marca cada problema con una columna
   bandera (duplicado exacto, precio cero, precio negativo, sesión nula,
   marca nula, categoría nula). Solo se eliminan filas si así se define
   en config.DECISIONES; por defecto no se elimina ninguna.

La recuperación de la marca y la tabla de productos se hacen en
04_integracion.py, porque necesitan ver los cinco meses juntos.

Entradas:  data/interim/parquet/<mes>.parquet (completo)
           o data/interim/sample/<mes>.parquet (muestra)
Salidas:   data/interim/limpio/<fuente>/<mes>.parquet
           reports/tablas/03_resumen_limpieza_<fuente>.csv

Uso
---
    python src/03_limpieza.py --fuente muestra
    python src/03_limpieza.py --fuente completo
"""

import argparse
import sys
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (  # noqa: E402
    COLUMNAS_ORIGINALES,
    DECISIONES,
    DESCONOCIDO,
    FUENTES,
    MESES,
    TABLAS,
    TIPOS_EVENTO,
    carpeta_entrada,
    carpeta_limpio,
    relativa,
)

MIB = 1024 ** 2
TEXTOS = ["event_type", "category_code", "brand"]

# Orden final de columnas del evento limpio.
COLUMNAS_SALIDA = [
    # originales (estandarizadas)
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
    # derivadas de la fecha
    "fecha", "hora", "dia_semana",
    # niveles de categoría
    "categoria_nivel_1", "categoria_nivel_2", "categoria_nivel_3",
    # banderas de calidad
    "es_duplicado_exacto", "repeticiones_exactas",
    "precio_cero", "precio_negativo",
    "sesion_nula", "marca_nula_original", "categoria_code_nula",
    # trazabilidad
    "archivo_origen", "fila_origen",
]


def normalizar_texto(col: str) -> pl.Expr:
    return pl.col(col).str.strip_chars().str.to_lowercase()


def limpiar_mes(ruta: Path) -> tuple[pl.DataFrame, dict]:
    original = pl.read_parquet(ruta)
    filas_entrada = original.height
    # Memoria de las 9 columnas originales, para medir el efecto de los
    # tipos sin contar las columnas nuevas que se agregan.
    memoria_antes = original.select(COLUMNAS_ORIGINALES).estimated_size() / MIB

    # 3. Cuántos textos cambian al normalizar (antes de transformar).
    cambios_texto = original.select(
        [
            (pl.col(c).is_not_null() & (pl.col(c) != normalizar_texto(c)))
            .sum()
            .alias(c)
            for c in TEXTOS
        ]
    ).row(0, named=True)

    # 6a. Duplicado exacto = misma fila en las 9 columnas originales. Se
    # calcula sobre los valores tal como vienen del CSV, antes de cualquier
    # transformación. La primera aparición (según el orden del CSV) no se
    # marca; las repeticiones siguientes sí.
    df = original.with_columns(
        (pl.int_range(pl.len()).over(COLUMNAS_ORIGINALES) > 0)
        .alias("es_duplicado_exacto"),
        pl.len().over(COLUMNAS_ORIGINALES).cast(pl.UInt32)
        .alias("repeticiones_exactas"),
        pl.col("user_session").is_null().alias("sesion_nula"),
        pl.col("brand").is_null().alias("marca_nula_original"),
        pl.col("category_code").is_null().alias("categoria_code_nula"),
    )

    df = df.with_columns([normalizar_texto(c) for c in TEXTOS])

    # 1. Fechas en UTC. strict=False para poder contar las que fallen.
    df = df.with_columns(
        pl.col("event_time")
        .str.strip_suffix(" UTC")
        .str.to_datetime("%Y-%m-%d %H:%M:%S", time_zone="UTC", strict=False)
        .alias("event_time")
    )
    fechas_invalidas = df["event_time"].null_count()
    if fechas_invalidas:
        raise ValueError(
            f"{ruta.name}: {fechas_invalidas} fechas no se pudieron convertir."
        )

    df = df.with_columns(
        pl.col("event_time").dt.date().alias("fecha"),
        pl.col("event_time").dt.hour().cast(pl.Int8).alias("hora"),
        pl.col("event_time").dt.weekday().cast(pl.Int8).alias("dia_semana"),
    )

    # 2. Niveles de categoría.
    partes = pl.col("category_code").str.split(".")
    df = df.with_columns(
        *[
            pl.when(pl.col("category_code").is_null())
            .then(pl.lit(DESCONOCIDO))
            .otherwise(partes.list.get(i, null_on_oob=True))
            .alias(f"categoria_nivel_{i + 1}")
            for i in range(3)
        ],
    ).with_columns(pl.col("category_code").fill_null(DESCONOCIDO))

    # 6b. Banderas de precio (decisiones de la actividad 8).
    df = df.with_columns(
        (pl.col("price") == 0).alias("precio_cero"),
        (pl.col("price") < 0).alias("precio_negativo"),
    )

    # 5. Tipos más livianos.
    df = df.with_columns(
        pl.col("event_type").cast(pl.Enum(TIPOS_EVENTO)),
        pl.col("product_id").cast(pl.Int32),
        pl.col("user_id").cast(pl.Int32),
        pl.col("brand").cast(pl.Categorical),
        pl.col("category_code").cast(pl.Categorical),
        pl.col("categoria_nivel_1").cast(pl.Categorical),
        pl.col("categoria_nivel_2").cast(pl.Categorical),
        pl.col("categoria_nivel_3").cast(pl.Categorical),
        pl.col("archivo_origen").cast(pl.Enum(MESES)),
    ).select(COLUMNAS_SALIDA)

    resumen = {
        "archivo": ruta.stem,
        "filas_entrada": filas_entrada,
        "fechas_invalidas": fechas_invalidas,
        "textos_modificados_event_type": cambios_texto["event_type"],
        "textos_modificados_category_code": cambios_texto["category_code"],
        "textos_modificados_brand": cambios_texto["brand"],
        "duplicados_exactos_marcados": int(df["es_duplicado_exacto"].sum()),
        "precio_cero": int(df["precio_cero"].sum()),
        "precio_negativo": int(df["precio_negativo"].sum()),
        "sesion_nula": int(df["sesion_nula"].sum()),
        "marca_nula": int(df["marca_nula_original"].sum()),
        "category_code_nulo": int(df["categoria_code_nula"].sum()),
    }

    # Eliminaciones, solo si el equipo así lo decidió.
    if DECISIONES["eliminar_duplicados_exactos"]:
        df = df.filter(~pl.col("es_duplicado_exacto"))
    if DECISIONES["eliminar_sesion_nula"]:
        df = df.filter(~pl.col("sesion_nula"))

    resumen["filas_eliminadas"] = filas_entrada - df.height
    resumen["filas_salida"] = df.height
    resumen["memoria_originales_antes_mib"] = round(memoria_antes, 1)
    resumen["memoria_originales_despues_mib"] = round(
        df.select(COLUMNAS_ORIGINALES).estimated_size() / MIB, 1
    )
    resumen["memoria_total_salida_mib"] = round(df.estimated_size() / MIB, 1)
    return df, resumen


def main() -> None:
    parser = argparse.ArgumentParser(description="Limpieza y estandarización")
    parser.add_argument("--fuente", choices=FUENTES, default="muestra")
    args = parser.parse_args()

    entrada = carpeta_entrada(args.fuente)
    salida = carpeta_limpio(args.fuente)
    salida.mkdir(parents=True, exist_ok=True)
    TABLAS.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print(f"03 - LIMPIEZA Y ESTANDARIZACIÓN ({args.fuente})")
    print("=" * 72)
    print(f"Decisiones aplicadas: {DECISIONES}")
    inicio = time.perf_counter()

    resumenes = []
    for mes in MESES:
        ruta = entrada / f"{mes}.parquet"
        if not ruta.exists():
            raise FileNotFoundError(f"No existe {relativa(ruta)}")
        df, resumen = limpiar_mes(ruta)
        df.write_parquet(salida / f"{mes}.parquet", compression="zstd")
        resumenes.append(resumen)
        print(f"  {mes}: {resumen['filas_salida']:>10,} filas | "
              f"duplicados marcados {resumen['duplicados_exactos_marcados']:,} | "
              f"memoria (9 columnas) {resumen['memoria_originales_antes_mib']} -> "
              f"{resumen['memoria_originales_despues_mib']} MiB")

    tabla = pl.DataFrame(resumenes)
    total = {c: tabla[c].sum() for c in tabla.columns if c != "archivo"}
    total["archivo"] = "TOTAL"
    for c in ("memoria_originales_antes_mib", "memoria_originales_despues_mib",
              "memoria_total_salida_mib"):
        total[c] = round(total[c], 1)
    tabla = pl.concat([tabla, pl.DataFrame([total])], how="diagonal_relaxed")
    tabla.write_csv(TABLAS / f"03_resumen_limpieza_{args.fuente}.csv")

    print(f"\nFilas: {total['filas_entrada']:,} -> {total['filas_salida']:,} "
          f"(eliminadas: {total['filas_eliminadas']:,})")
    ahorro = 100 * (1 - total["memoria_originales_despues_mib"]
                    / total["memoria_originales_antes_mib"])
    print(f"Memoria de las 9 columnas originales: "
          f"{total['memoria_originales_antes_mib']} -> "
          f"{total['memoria_originales_despues_mib']} MiB ({ahorro:.0f} % menos)")
    print(f"Duración: {time.perf_counter() - inicio:.1f} s")
    print(f"Salida: {relativa(salida)}")


if __name__ == "__main__":
    main()
