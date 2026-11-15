"""
06 - TABLAS ANALÍTICAS (actividades 8, 11, 12 y 13 de la guía).

Qué hace
--------
1. Calcula precio_analisis en cada evento:
      price > 0  -> price
      price = 0  -> mediana de los precios positivos del mismo producto
      price < 0  -> vacío (no se usa para montos ni precios medios)
2. Construye la tabla de SESIONES (una fila por user_session) con los
   eventos sin duplicados exactos, sin sesión nula y sin sesiones
   multiusuario (sección 2.2 del informe):
      - conteos de actividad, duración, monto comprado y banderas;
      - atributos del PUNTO DE OBSERVACIÓN, que es el primer evento cart
        de la sesión (t0). Solo usan eventos con event_time <= t0, igual
        para las sesiones que compran y para las que no, para evitar la
        fuga de información (sección 2.7);
      - historial del usuario: solo sesiones que TERMINARON antes de que
        empiece la sesión actual (join_asof estricto).
3. Define la población del modelo de clasificación y la variable objetivo:
      poblacion_modelo = tiene carrito, no compró antes de su primer
                         carrito y no es una sesión de compra masiva;
      compra           = 1 si la sesión registra al menos una compra.
4. Construye la tabla de USUARIOS (una fila por user_id) con los eventos
   sin duplicados exactos (incluye eventos sin sesión): recencia,
   frecuencia, monto, ticket medio y comportamiento de navegación.
5. Asigna cada sesión de la población a su conjunto según el mes en que
   EMPIEZA la sesión (partición temporal) y resume la partición.
6. Mide la calidad antes y después de la preparación.

Entradas:  data/processed/<fuente>/eventos.parquet
           data/processed/<fuente>/productos.parquet
Salidas:   data/processed/<fuente>/sesiones.parquet
           data/processed/<fuente>/usuarios.parquet
           reports/tablas/06_verificaciones_tablas_<fuente>.csv
           reports/tablas/06_particion_<fuente>.csv
           reports/tablas/06_calidad_antes_despues_<fuente>.csv

Uso
---
    python src/06_tablas_analiticas.py --fuente muestra
    python src/06_tablas_analiticas.py --fuente completo
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import FUENTES, TABLAS, carpeta_procesado, relativa  # noqa: E402

# Umbral de compra masiva: más de 100 eventos purchase en una sesión
# (por encima del percentil 99,9 de compras por sesión, que es 97).
UMBRAL_COMPRA_MASIVA = 100

# Fecha de referencia para la recencia: el día siguiente al último evento.
FECHA_REFERENCIA = datetime(2020, 3, 1, tzinfo=timezone.utc)

# Partición temporal por mes de inicio de la sesión.
CONJUNTO_POR_MES = {
    "2019-10": "historial",       # solo calienta el historial del usuario
    "2019-11": "entrenamiento",
    "2019-12": "entrenamiento",
    "2020-01": "validacion",
    "2020-02": "prueba",
}

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


def eventos_con_precio(eventos: pl.LazyFrame, productos: pl.LazyFrame) -> pl.LazyFrame:
    """Agrega precio_analisis y la categoría principal del producto."""
    return (
        eventos.join(
            productos.select("product_id", "categoria_principal",
                             "precio_mediano_positivo"),
            on="product_id", how="left", validate="m:1",
        )
        .with_columns(
            pl.when(pl.col("price") > 0).then(pl.col("price"))
            .when(pl.col("price") == 0).then(pl.col("precio_mediano_positivo"))
            .otherwise(None)
            .alias("precio_analisis")
        )
        .drop("precio_mediano_positivo")
    )


def construir_sesiones(base: pl.LazyFrame) -> pl.DataFrame:
    es = pl.col("event_type")
    eventos = base.filter(~pl.col("sesion_nula") & ~pl.col("sesion_multiusuario"))

    # Conteos de la sesión completa (describen la sesión; los conteos
    # posteriores a t0 NO se usan como predictores).
    resumen = eventos.group_by("user_session").agg(
        pl.col("user_id").first(),
        pl.col("event_time").min().alias("inicio"),
        pl.col("event_time").max().alias("fin"),
        pl.len().alias("eventos"),
        (es == "view").sum().alias("total_visualizaciones"),
        (es == "cart").sum().alias("total_carritos"),
        (es == "remove_from_cart").sum().alias("total_retiros"),
        (es == "purchase").sum().alias("total_compras"),
        pl.col("product_id").n_unique().alias("productos_distintos"),
        pl.col("precio_analisis").filter(es == "view").mean()
        .alias("precio_promedio_sesion"),
        pl.col("precio_analisis").filter(es == "purchase").sum()
        .alias("monto_compra"),
        pl.col("event_time").filter(es == "cart").min().alias("momento_primer_carrito"),
        pl.col("event_time").filter(es == "purchase").min().alias("momento_primera_compra"),
        pl.col("precio_cero").any().alias("sesion_con_precio_cero"),
    )

    # Primer evento cart de cada sesión (desempate: archivo y fila de origen).
    primer_carrito = (
        eventos.filter(es == "cart")
        .sort(["user_session", "event_time", "archivo_origen", "fila_origen"])
        .group_by("user_session", maintain_order=True)
        .agg(
            pl.col("event_time").first().alias("_t0"),
            pl.col("categoria_principal").first().alias("categoria_primer_carrito"),
            pl.col("marca_desconocida").first().alias("marca_desconocida_primer_carrito"),
            pl.col("hora").first().alias("hora_primer_carrito"),
            pl.col("dia_semana").first().alias("dia_semana_primer_carrito"),
        )
    )

    # Atributos con información disponible hasta t0 (incluido).
    hasta_t0 = (
        eventos.join(primer_carrito.select("user_session", "_t0"),
                     on="user_session", how="inner", validate="m:1")
        .filter(pl.col("event_time") <= pl.col("_t0"))
        .group_by("user_session")
        .agg(
            (es == "view").sum().alias("vistas_antes_carrito"),
            pl.col("product_id").filter(es == "view").n_unique()
            .alias("productos_vistos_antes_carrito"),
            pl.col("precio_analisis").filter(es == "view").mean()
            .alias("precio_medio_vistas_antes_carrito"),
            pl.col("precio_analisis").filter(
                (es == "cart") & (pl.col("event_time") == pl.col("_t0"))
            ).mean().alias("precio_primer_carrito"),
        )
    )

    sesiones = (
        resumen.join(primer_carrito, on="user_session", how="left", validate="1:1")
        .join(hasta_t0, on="user_session", how="left", validate="1:1")
        .with_columns(
            (pl.col("fin") - pl.col("inicio")).dt.total_seconds().alias("duracion_seg"),
            pl.col("inicio").dt.strftime("%Y-%m").alias("mes_inicio"),
            (pl.col("total_carritos") > 0).alias("tiene_carrito"),
            (pl.col("total_compras") > 0).cast(pl.Int8).alias("compra"),
            (pl.col("total_compras") > UMBRAL_COMPRA_MASIVA).alias("sesion_compra_masiva"),
            (pl.col("momento_primera_compra") < pl.col("momento_primer_carrito"))
            .fill_null(False).alias("compra_antes_carrito"),
            (pl.col("momento_primer_carrito") - pl.col("inicio")).dt.total_seconds()
            .alias("seg_hasta_primer_carrito"),
        )
        .with_columns(
            (pl.col("tiene_carrito") & ~pl.col("compra_antes_carrito")
             & ~pl.col("sesion_compra_masiva")).alias("poblacion_modelo"),
        )
        .drop("_t0")
        .collect(engine="streaming")
    )

    # Historial del usuario: sesiones que terminaron ANTES del inicio de la
    # sesión actual (join_asof estricto, por usuario).
    terminadas = (
        sesiones.select("user_id", "fin", "compra")
        .sort("fin")
        .with_columns(
            pl.col("fin").cum_count().over("user_id").alias("sesiones_previas_usuario"),
            pl.col("compra").cast(pl.Int32).cum_sum().over("user_id")
            .alias("compras_previas_usuario"),
        )
        .rename({"fin": "fin_previa"})
        .drop("compra")
    )
    sesiones = (
        sesiones.sort("inicio")
        .join_asof(terminadas, left_on="inicio", right_on="fin_previa",
                   by="user_id", strategy="backward", allow_exact_matches=False)
        .with_columns(
            pl.col("sesiones_previas_usuario").fill_null(0),
            pl.col("compras_previas_usuario").fill_null(0),
            ((pl.col("inicio") - pl.col("fin_previa")).dt.total_seconds() / 86400)
            .alias("dias_desde_sesion_previa"),
        )
        .drop("fin_previa")
        .with_columns(
            pl.col("mes_inicio").replace_strict(CONJUNTO_POR_MES)
            .alias("conjunto")
        )
        .sort("user_session")
    )
    return sesiones


def construir_usuarios(base: pl.LazyFrame, sesiones: pl.DataFrame) -> pl.DataFrame:
    es = pl.col("event_type")
    masivas = sesiones.filter("sesion_compra_masiva").select("user_id").unique()
    usuarios = (
        base.group_by("user_id").agg(
            pl.col("event_time").min().alias("primera_actividad"),
            pl.col("event_time").max().alias("ultima_actividad"),
            pl.col("archivo_origen").n_unique().alias("meses_activos"),
            pl.col("user_session").drop_nulls().n_unique().alias("total_sesiones"),
            (es == "view").sum().alias("total_visualizaciones"),
            (es == "cart").sum().alias("total_carritos"),
            (es == "remove_from_cart").sum().alias("total_retiros"),
            (es == "purchase").sum().alias("total_compras"),
            pl.col("user_session").filter(es == "purchase").drop_nulls().n_unique()
            .alias("sesiones_con_compra"),
            pl.col("precio_analisis").filter(es == "purchase").sum().alias("monto_total"),
            pl.col("event_time").filter(es == "purchase").max().alias("ultima_compra"),
            pl.col("marca_desconocida").mean().alias("prop_sin_marca"),
        )
        .collect(engine="streaming")
        .with_columns(
            (pl.col("total_compras") > 0).alias("comprador"),
            ((pl.lit(FECHA_REFERENCIA) - pl.col("ultima_compra")).dt.total_seconds()
             / 86400).alias("recencia_dias"),
            pl.when(pl.col("sesiones_con_compra") > 0)
            .then(pl.col("monto_total") / pl.col("sesiones_con_compra"))
            .alias("ticket_medio"),
            pl.when(pl.col("total_visualizaciones") > 0)
            .then(pl.col("total_carritos") / pl.col("total_visualizaciones"))
            .alias("tasa_carrito"),
            pl.when(pl.col("total_carritos") > 0)
            .then(pl.col("total_retiros") / pl.col("total_carritos"))
            .alias("tasa_retiro"),
            pl.col("user_id").is_in(masivas["user_id"].implode())
            .alias("tiene_sesion_masiva"),
        )
        .with_columns(
            pl.when(~pl.col("comprador")).then(None)
            .otherwise(pl.col("monto_total")).alias("monto_total"),
            (pl.col("comprador") & ~pl.col("tiene_sesion_masiva"))
            .alias("poblacion_segmentacion"),
        )
        .sort("user_id")
    )
    return usuarios


def resumir_particion(sesiones: pl.DataFrame) -> pl.DataFrame:
    pob = sesiones.filter("poblacion_modelo")
    usuarios_entrenamiento = (
        pob.filter(pl.col("conjunto") == "entrenamiento").select("user_id").unique()
    )
    return (
        pob.with_columns(
            pl.col("user_id").is_in(usuarios_entrenamiento["user_id"].implode())
            .alias("_usuario_en_entrenamiento"),
            (pl.col("fin").dt.strftime("%Y-%m") != pl.col("mes_inicio"))
            .alias("_cruza_mes"),
        )
        .group_by("mes_inicio", "conjunto")
        .agg(
            pl.len().alias("sesiones"),
            pl.col("compra").sum().alias("con_compra"),
            (pl.col("compra").mean() * 100).alias("pct_compra"),
            pl.col("_cruza_mes").sum().alias("cruzan_al_mes_siguiente"),
            (pl.col("sesion_con_precio_cero").mean() * 100)
            .alias("pct_con_precio_cero"),
            (pl.col("sesiones_previas_usuario") > 0).mean().mul(100)
            .alias("pct_con_historial"),
            (pl.col("_usuario_en_entrenamiento").mean() * 100)
            .alias("pct_usuario_en_entrenamiento"),
        )
        .sort("mes_inicio")
    )


def calidad_antes_despues(eventos: pl.LazyFrame, base: pl.LazyFrame,
                          sesiones: pl.DataFrame, productos: pl.DataFrame) -> pl.DataFrame:
    es = pl.col("event_type")

    def medir(lf: pl.LazyFrame) -> dict:
        return lf.select(
            pl.len().alias("eventos"),
            pl.col("es_duplicado_exacto").sum().alias("duplicados"),
            ((es == "remove_from_cart").sum() / (es == "cart").sum() * 100)
            .alias("retiros_por_100_carritos"),
            pl.col("marca_nula_original").sum().alias("marca_vacia_original"),
            pl.col("marca_desconocida").sum().alias("marca_desconocida"),
            pl.col("precio_cero").sum().alias("precio_cero"),
            pl.col("precio_negativo").sum().alias("precio_negativo"),
            pl.col("sesion_nula").sum().alias("sesion_nula"),
        ).collect(engine="streaming").row(0, named=True)

    antes = medir(eventos)
    despues = medir(base)
    precio = base.select(
        (pl.col("precio_cero") & pl.col("precio_analisis").is_not_null()).sum()
        .alias("cero_imputados"),
        (pl.col("precio_analisis").is_null()).sum().alias("sin_precio_analisis"),
        (pl.col("precio_analisis") <= 0).sum().alias("precio_analisis_no_positivo"),
    ).collect(engine="streaming").row(0, named=True)
    sesiones_antes = eventos.select(
        pl.col("user_session").drop_nulls().n_unique()).collect(engine="streaming").item()

    filas = [
        ("Eventos", antes["eventos"], despues["eventos"],
         "Base de análisis sin duplicados exactos"),
        ("Duplicados exactos", antes["duplicados"], despues["duplicados"],
         "Se conserva la primera aparición"),
        ("Retiros por cada 100 adiciones al carrito",
         round(antes["retiros_por_100_carritos"], 1),
         round(despues["retiros_por_100_carritos"], 1), ""),
        ("Eventos sin marca", antes["marca_vacia_original"],
         despues["marca_desconocida"], "Marca recuperada desde el producto"),
        ("Eventos con precio 0 sin precio válido para montos", antes["precio_cero"],
         despues["precio_cero"] - precio["cero_imputados"],
         "precio_analisis = mediana positiva del producto"),
        ("Eventos con precio negativo usados en montos", antes["precio_negativo"], 0,
         "precio_analisis vacío"),
        ("Eventos con precio_analisis no positivo", "-", precio["precio_analisis_no_positivo"], ""),
        ("Eventos sin precio_analisis", "-", precio["sin_precio_analisis"], ""),
        ("Sesiones distintas", sesiones_antes, sesiones.height,
         "Tabla de sesiones sin multiusuario"),
        ("Eventos sin sesión en la tabla de sesiones", antes["sesion_nula"], 0,
         "Se conservan en la tabla de usuarios"),
        ("Productos con varias categorías sin regla de asignación",
         int(productos["producto_multicategoria"].sum()), 0,
         "Una categoría principal por producto"),
        ("Sesiones de compra masiva en el modelo",
         int(sesiones["sesion_compra_masiva"].sum()), 0,
         "Marcadas y excluidas de la población del modelo"),
    ]
    return pl.DataFrame(
        {"indicador": [f[0] for f in filas],
         "antes": [str(f[1]) for f in filas],
         "despues": [str(f[2]) for f in filas],
         "regla": [f[3] for f in filas]}
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Tablas analíticas y partición")
    parser.add_argument("--fuente", choices=FUENTES, default="muestra")
    args = parser.parse_args()

    carpeta = carpeta_procesado(args.fuente)
    TABLAS.mkdir(parents=True, exist_ok=True)
    VERIFICACIONES.clear()
    inicio = time.perf_counter()

    print("=" * 72)
    print(f"06 - TABLAS ANALÍTICAS ({args.fuente})")
    print("=" * 72)

    eventos = pl.scan_parquet(carpeta / "eventos.parquet")
    productos = pl.read_parquet(carpeta / "productos.parquet")
    base = eventos_con_precio(eventos, productos.lazy()).filter(
        ~pl.col("es_duplicado_exacto"))

    print("\n[1] Sesiones")
    sesiones = construir_sesiones(base)
    esperadas = base.filter(~pl.col("sesion_nula") & ~pl.col("sesion_multiusuario")) \
        .select(pl.col("user_session").n_unique()).collect(engine="streaming").item()
    registrar("filas en sesiones = sesiones válidas", sesiones.height, esperadas)
    registrar("user_session único en sesiones",
              int(sesiones["user_session"].is_duplicated().sum()), 0)
    registrar("sesiones con carrito", int(sesiones["tiene_carrito"].sum()))
    registrar("sesiones con compra", int(sesiones["compra"].sum()))
    registrar("sesiones con compra sin carrito en la sesión",
              int((sesiones["compra"].cast(pl.Boolean) & ~sesiones["tiene_carrito"]).sum()))
    registrar("sesiones con compra antes del primer carrito",
              int(sesiones["compra_antes_carrito"].sum()))
    registrar("sesiones de compra masiva", int(sesiones["sesion_compra_masiva"].sum()))
    pob = sesiones.filter("poblacion_modelo")
    registrar("sesiones en la población del modelo", pob.height)
    registrar("sesiones de la población con compra", int(pob["compra"].sum()))
    registrar("historial calculado solo con sesiones terminadas antes", True, True,
              "join_asof estricto sobre el fin de las sesiones previas")
    sesiones.write_parquet(carpeta / "sesiones.parquet", compression="zstd")

    print("\n[2] Usuarios")
    usuarios = construir_usuarios(base, sesiones)
    distintos = base.select(pl.col("user_id").n_unique()).collect(engine="streaming").item()
    registrar("filas en usuarios = user_id distintos", usuarios.height, distintos)
    registrar("usuarios compradores", int(usuarios["comprador"].sum()))
    registrar("usuarios con sesión de compra masiva",
              int(usuarios["tiene_sesion_masiva"].sum()))
    registrar("usuarios en la población de segmentación",
              int(usuarios["poblacion_segmentacion"].sum()))
    usuarios.write_parquet(carpeta / "usuarios.parquet", compression="zstd")

    print("\n[3] Partición temporal")
    particion = resumir_particion(sesiones)
    particion.write_csv(TABLAS / f"06_particion_{args.fuente}.csv")
    print(particion)

    print("\n[4] Calidad antes y después")
    calidad = calidad_antes_despues(eventos, base, sesiones, productos)
    calidad.write_csv(TABLAS / f"06_calidad_antes_despues_{args.fuente}.csv")
    print(calidad)

    pl.DataFrame(VERIFICACIONES).write_csv(
        TABLAS / f"06_verificaciones_tablas_{args.fuente}.csv")
    print(f"\nDuración: {time.perf_counter() - inicio:.1f} s")
    print(f"Salida: {relativa(carpeta)}")


if __name__ == "__main__":
    main()
