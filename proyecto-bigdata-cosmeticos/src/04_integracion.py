"""
04 - INTEGRACIÓN (actividad 10 de la guía).

Responsable: Gabriela (Fase 2).

Qué hace
--------
1. Verifica que los cinco meses limpios tengan exactamente las mismas
   columnas y tipos.
2. Verifica las fechas: cada archivo solo trae eventos de su mes y los
   rangos de fechas de los archivos no se superponen.
3. Une los cinco meses en una sola tabla de eventos y comprueba que el
   número de filas sea la suma exacta de los meses.
4. Construye la tabla de productos (una fila por product_id) con reglas
   explícitas para los productos con varias categorías o marcas:
      - categoría y marca = la más frecuente en los eventos del producto;
      - empate: la que aparece en el evento más reciente;
      - si aún empatan: el valor menor (orden alfabético o numérico).
   Se comprueba que product_id quede único.
5. Recupera la marca de los eventos sin marca cuando su producto tiene
   UNA sola marca conocida en todos sus eventos. Si el producto tiene
   varias marcas, no se recupera (no habría forma segura de elegir). Lo
   que sigue sin marca queda como "unknown".
6. Marca las sesiones que tienen más de un user_id (sesion_multiusuario)
   y cuenta las sesiones que cruzan de un mes a otro.
7. Antes y después de cada unión cuenta las filas: deben coincidir. Si no
   coinciden, el script se detiene.

Llaves
------
eventos.product_id   -> productos.product_id   (muchos a uno)
eventos.user_session -> sesión                 (muchos a uno; nulas excluidas)
eventos.user_id      -> usuario                (muchos a uno)

Entradas:  data/interim/limpio/<fuente>/<mes>.parquet
Salidas:   data/processed/<fuente>/eventos.parquet
           data/processed/<fuente>/productos.parquet
           reports/tablas/04_verificaciones_integracion_<fuente>.csv
           reports/tablas/04_rangos_fechas_<fuente>.csv

Uso
---
    python src/04_integracion.py --fuente muestra
    python src/04_integracion.py --fuente completo
"""

import argparse
import sys
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (  # noqa: E402
    DECISIONES,
    DESCONOCIDO,
    FUENTES,
    MESES,
    TABLAS,
    carpeta_limpio,
    carpeta_procesado,
    relativa,
)

MES_A_NUMERO = {"Oct": 10, "Nov": 11, "Dec": 12, "Jan": 1, "Feb": 2}

VERIFICACIONES: list[dict] = []


def registrar(paso: str, valor, esperado=None, detalle: str = "") -> None:
    ok = None if esperado is None else (valor == esperado)
    VERIFICACIONES.append({
        "paso": paso,
        "valor": str(valor),
        "esperado": "" if esperado is None else str(esperado),
        "resultado": "" if ok is None else ("OK" if ok else "ERROR"),
        "detalle": detalle,
    })
    marca = "" if ok is None else ("  [OK]" if ok else "  [ERROR]")
    print(f"  {paso}: {valor}{marca}")
    if ok is False:
        raise RuntimeError(f"Falló la verificación «{paso}»: {valor} != {esperado}")


def mes_esperado(archivo: str) -> tuple[int, int]:
    anio, mes = archivo.split("-")
    return int(anio), MES_A_NUMERO[mes]


def valor_mas_frecuente(eventos: pl.LazyFrame, columna: str) -> pl.LazyFrame:
    """Valor más frecuente de `columna` por producto, con desempate."""
    return (
        eventos.filter(pl.col(columna).is_not_null())
        .group_by("product_id", columna)
        .agg(
            pl.len().alias("_n"),
            pl.col("event_time").max().alias("_ultimo"),
        )
        .sort(["product_id", "_n", "_ultimo", columna],
              descending=[False, True, True, False])
        .group_by("product_id", maintain_order=True)
        .agg(
            pl.col(columna).first().alias(columna),
            pl.col(columna).n_unique().alias("_distintos"),
        )
    )


def construir_productos(eventos: pl.LazyFrame) -> pl.DataFrame:
    base = eventos.group_by("product_id").agg(
        pl.len().alias("eventos"),
        pl.col("event_time").min().alias("primera_aparicion"),
        pl.col("event_time").max().alias("ultima_aparicion"),
        pl.col("price").filter(pl.col("price") > 0).median()
        .alias("precio_mediano_positivo"),
        pl.col("price").filter(pl.col("price") > 0).min()
        .alias("precio_min_positivo"),
        pl.col("price").max().alias("precio_max"),
        pl.col("price").n_unique().alias("precios_distintos"),
        pl.col("brand").is_null().sum().alias("eventos_sin_marca"),
    )

    categoria = valor_mas_frecuente(eventos, "category_id").rename(
        {"_distintos": "categorias_distintas"}
    )
    marca = valor_mas_frecuente(
        eventos.with_columns(pl.col("brand").cast(pl.String)), "brand"
    ).rename(
        {"_distintos": "marcas_distintas"}
    )
    codigo = valor_mas_frecuente(
        eventos.with_columns(
            pl.when(pl.col("categoria_code_nula")).then(None)
            .otherwise(pl.col("category_code").cast(pl.String))
            .alias("category_code")
        ),
        "category_code",
    ).drop("_distintos")

    productos = (
        base.join(categoria, on="product_id", how="left", validate="1:1")
        .join(marca, on="product_id", how="left", validate="1:1")
        .join(codigo, on="product_id", how="left", validate="1:1")
        .with_columns(
            pl.col("marcas_distintas").fill_null(0),
            pl.col("brand").cast(pl.String),
        )
        .with_columns(
            (pl.col("categorias_distintas") > 1).alias("producto_multicategoria"),
            (pl.col("marcas_distintas") > 1).alias("producto_multimarca"),
            (pl.col("marcas_distintas") == 0).alias("producto_sin_marca"),
            ((pl.col("marcas_distintas") == 1) & (pl.col("eventos_sin_marca") > 0))
            .alias("producto_marca_recuperable"),
            # Productos que solo aparecen con precio negativo (probables
            # descuentos o cupones, según el diagnóstico).
            (pl.col("precio_max") < 0).alias("producto_precio_negativo"),
            pl.col("brand").fill_null(DESCONOCIDO),
            pl.col("category_code").fill_null(DESCONOCIDO),
        )
        .rename({"brand": "marca_principal",
                 "category_id": "categoria_principal",
                 "category_code": "category_code_principal"})
        .collect()
    )
    return productos.select(
        "product_id", "categoria_principal", "categorias_distintas",
        "category_code_principal", "marca_principal", "marcas_distintas",
        "eventos", "eventos_sin_marca", "precio_mediano_positivo",
        "precio_min_positivo", "precio_max", "precios_distintos",
        "primera_aparicion", "ultima_aparicion",
        "producto_multicategoria", "producto_multimarca", "producto_sin_marca",
        "producto_marca_recuperable", "producto_precio_negativo",
    ).sort("product_id")


def main() -> None:
    parser = argparse.ArgumentParser(description="Integración de fuentes")
    parser.add_argument("--fuente", choices=FUENTES, default="muestra")
    args = parser.parse_args()

    entrada = carpeta_limpio(args.fuente)
    salida = carpeta_procesado(args.fuente)
    salida.mkdir(parents=True, exist_ok=True)
    TABLAS.mkdir(parents=True, exist_ok=True)
    VERIFICACIONES.clear()
    inicio = time.perf_counter()

    print("=" * 72)
    print(f"04 - INTEGRACIÓN ({args.fuente})")
    print("=" * 72)

    rutas = [entrada / f"{m}.parquet" for m in MESES]
    for r in rutas:
        if not r.exists():
            raise FileNotFoundError(f"No existe {relativa(r)}. Ejecute 03_limpieza.py")

    # 1. Mismas columnas y tipos en los cinco meses.
    print("\n[1] Esquemas")
    esquemas = [pl.read_parquet_schema(r) for r in rutas]
    iguales = all(e == esquemas[0] for e in esquemas)
    registrar("meses con columnas y tipos idénticos", iguales, True,
              f"{len(esquemas[0])} columnas")

    # 2. Fechas: cada archivo en su mes y sin superposición.
    print("\n[2] Rangos de fechas")
    rangos = []
    for mes, ruta in zip(MESES, rutas):
        anio, numero = mes_esperado(mes)
        r = pl.scan_parquet(ruta).select(
            pl.lit(mes).alias("archivo"),
            pl.len().alias("filas"),
            pl.col("event_time").min().alias("fecha_min"),
            pl.col("event_time").max().alias("fecha_max"),
            ((pl.col("fecha").dt.year() != anio)
             | (pl.col("fecha").dt.month() != numero)).sum()
            .alias("eventos_fuera_de_su_mes"),
        ).collect()
        rangos.append(r)
    rangos = pl.concat(rangos)
    rangos.write_csv(TABLAS / f"04_rangos_fechas_{args.fuente}.csv")
    registrar("eventos fuera del mes de su archivo",
              int(rangos["eventos_fuera_de_su_mes"].sum()), 0)
    superpuestos = int(
        (rangos["fecha_min"].slice(1) <= rangos["fecha_max"].slice(0, len(MESES) - 1))
        .sum()
    )
    registrar("pares de meses con fechas superpuestas", superpuestos, 0)

    # 3. Unión de los cinco meses.
    print("\n[3] Unión")
    filas_meses = int(rangos["filas"].sum())
    eventos = pl.scan_parquet(rutas)
    filas_union = eventos.select(pl.len()).collect().item()
    registrar("filas tras unir los 5 meses", filas_union, filas_meses,
              "debe ser la suma de los meses")

    # 4. Tabla de productos.
    print("\n[4] Tabla de productos")
    productos = construir_productos(eventos)
    productos.write_parquet(salida / "productos.parquet", compression="zstd")
    distintos = eventos.select(pl.col("product_id").n_unique()).collect().item()
    registrar("filas en productos = product_id distintos",
              productos.height, distintos)
    registrar("product_id único en productos",
              productos["product_id"].is_duplicated().sum(), 0)
    for bandera in ("producto_multicategoria", "producto_multimarca",
                    "producto_sin_marca", "producto_marca_recuperable",
                    "producto_precio_negativo"):
        registrar(bandera, int(productos[bandera].sum()))

    # 5. Recuperar marca desde el producto (relación muchos a uno).
    print("\n[5] Recuperación de marca")
    marca_unica = productos.lazy().select(
        "product_id",
        pl.when(pl.col("marcas_distintas") == 1)
        .then(pl.col("marca_principal"))
        .otherwise(None)
        .alias("_marca_unica"),
    )
    recuperar = DECISIONES["recuperar_marca_desde_producto"]
    eventos = (
        eventos.join(marca_unica, on="product_id", how="left", validate="m:1")
        .with_columns(
            (pl.col("marca_nula_original") & pl.col("_marca_unica").is_not_null()
             & pl.lit(recuperar)).alias("marca_recuperada"),
        )
        .with_columns(
            pl.when(pl.col("marca_recuperada"))
            .then(pl.col("_marca_unica"))
            .otherwise(pl.col("brand").cast(pl.String))
            .fill_null(DESCONOCIDO)
            .cast(pl.Categorical)
            .alias("brand"),
        )
        .with_columns((pl.col("brand") == DESCONOCIDO).alias("marca_desconocida"))
        .drop("_marca_unica")
    )
    filas_tras_marca = eventos.select(pl.len()).collect().item()
    registrar("filas tras unir con productos", filas_tras_marca, filas_union)
    conteos = eventos.select(
        pl.col("marca_nula_original").sum().alias("nulas"),
        pl.col("marca_recuperada").sum().alias("recuperadas"),
        pl.col("marca_desconocida").sum().alias("desconocidas"),
    ).collect().row(0, named=True)
    registrar("eventos con marca nula (original)", conteos["nulas"])
    registrar("eventos con marca recuperada", conteos["recuperadas"])
    registrar("eventos que quedan con marca 'unknown'", conteos["desconocidas"],
              conteos["nulas"] - conteos["recuperadas"])

    # 6. Sesiones con varios usuarios y sesiones entre meses.
    print("\n[6] Sesiones")
    por_sesion = (
        eventos.filter(pl.col("user_session").is_not_null())
        .group_by("user_session")
        .agg(
            pl.col("user_id").n_unique().alias("_usuarios"),
            pl.col("archivo_origen").n_unique().alias("_meses"),
        )
        .collect()
    )
    multiusuario = por_sesion.filter(pl.col("_usuarios") > 1)["user_session"]
    registrar("sesiones distintas (sin nulas)", por_sesion.height)
    registrar("sesiones con más de un user_id", multiusuario.len())
    registrar("sesiones que cruzan de un mes a otro",
              int((por_sesion["_meses"] > 1).sum()),
              detalle="las tablas de sesiones deben construirse con los 5 meses unidos")

    eventos = eventos.with_columns(
        pl.col("user_session").is_in(multiusuario.implode())
        .fill_null(False).alias("sesion_multiusuario")
    )

    # 7. Guardar la tabla de eventos integrada.
    print("\n[7] Guardado")
    eventos.sink_parquet(salida / "eventos.parquet", compression="zstd")
    filas_guardadas = pl.scan_parquet(salida / "eventos.parquet").select(
        pl.len()).collect().item()
    registrar("filas guardadas en eventos.parquet", filas_guardadas, filas_union)

    pl.DataFrame(VERIFICACIONES).write_csv(
        TABLAS / f"04_verificaciones_integracion_{args.fuente}.csv"
    )
    print(f"\nDuración: {time.perf_counter() - inicio:.1f} s")
    print(f"Salida: {relativa(salida)}")
    print("Todas las verificaciones de cardinalidad pasaron.")


if __name__ == "__main__":
    main()
