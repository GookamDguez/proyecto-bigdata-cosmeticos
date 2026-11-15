"""
07 - PERFIL EXPLORATORIO (actividad 5 de la guía, fase 1).

Genera las tablas y los gráficos de la sección 1.4 del informe a partir
del dataset preparado, para que el perfil exploratorio sea reproducible.

- Los gráficos de actividad mensual y de precios por mes usan TODOS los
  eventos originales (como en la exploración inicial).
- El embudo, la conversión y las tablas por categoría, marca y precio
  usan los eventos sin duplicados exactos (sección 2.2) y la tabla de
  sesiones de 06_tablas_analiticas.py.

Entradas:  data/processed/<fuente>/eventos.parquet, productos.parquet,
           sesiones.parquet
Salidas:   reports/tablas/07_*.csv
           reports/figures/fig_*.png

Uso
---
    python src/07_perfil_exploratorio.py --fuente completo
    python src/07_perfil_exploratorio.py --fuente completo --solo-tablas
    python src/07_perfil_exploratorio.py --fuente completo --solo-graficos
"""

import argparse
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import FIGURAS, FUENTES, TABLAS, carpeta_procesado  # noqa: E402

MESES_ETIQUETA = {"2019-10": "Oct 2019", "2019-11": "Nov 2019", "2019-12": "Dic 2019",
                  "2020-01": "Ene 2020", "2020-02": "Feb 2020"}
ARCHIVO_A_MES = {"2019-Oct": "2019-10", "2019-Nov": "2019-11", "2019-Dec": "2019-12",
                 "2020-Jan": "2020-01", "2020-Feb": "2020-02"}
CORTES_PRECIO = [2, 4, 6, 10, 20, 50]
ETIQUETAS_PRECIO = ["< 2", "2–4", "4–6", "6–10", "10–20", "20–50", "≥ 50"]


# ----------------------------------------------------------------------
# Tablas
# ----------------------------------------------------------------------

def embudo(lf: pl.LazyFrame, grupo: str) -> pl.LazyFrame:
    es = pl.col("event_type")
    return (
        lf.group_by(grupo).agg(
            pl.len().alias("eventos"),
            (es == "view").sum().alias("vistas"),
            (es == "cart").sum().alias("carritos"),
            (es == "purchase").sum().alias("compras"),
        )
        .with_columns(
            (pl.col("carritos") / pl.col("vistas") * 100).alias("carrito_por_100_vistas"),
            (pl.col("compras") / pl.col("carritos") * 100).alias("compras_por_100_carritos"),
        )
    )


def calcular_tablas(fuente: str) -> None:
    carpeta = carpeta_procesado(fuente)
    eventos = pl.scan_parquet(carpeta / "eventos.parquet")
    productos = pl.scan_parquet(carpeta / "productos.parquet")
    sesiones = pl.read_parquet(carpeta / "sesiones.parquet")
    es = pl.col("event_type")

    # Actividad y precios por mes con todos los eventos originales.
    (eventos.group_by("archivo_origen", "event_type").agg(pl.len().alias("eventos"))
     .with_columns(pl.col("archivo_origen").cast(pl.String).replace(ARCHIVO_A_MES)
                   .alias("mes"), pl.col("event_type").cast(pl.String))
     .drop("archivo_origen").sort("mes", "event_type")
     .collect(engine="streaming").write_csv(TABLAS / "07_eventos_mes_tipo.csv"))
    (eventos.group_by("archivo_origen").agg(
        pl.col("price").mean().alias("promedio"),
        pl.col("price").median().alias("mediana"),
        pl.col("price").min().alias("minimo"),
        pl.col("price").max().alias("maximo"),
        (pl.col("price") == 0).sum().alias("precios_cero"),
        (pl.col("price") < 0).sum().alias("precios_negativos"))
     .with_columns(pl.col("archivo_origen").cast(pl.String).replace(ARCHIVO_A_MES).alias("mes"))
     .drop("archivo_origen").sort("mes")
     .collect(engine="streaming").write_csv(TABLAS / "07_precios_mes.csv"))

    # Eventos sin duplicados, con categoría principal y precio de análisis.
    base = (
        eventos.filter(~pl.col("es_duplicado_exacto"))
        .join(productos.select("product_id", "categoria_principal",
                               "category_code_principal", "precio_mediano_positivo"),
              on="product_id", how="left", validate="m:1")
        .with_columns(
            pl.when(pl.col("price") > 0).then(pl.col("price"))
            .when(pl.col("price") == 0).then(pl.col("precio_mediano_positivo"))
            .otherwise(None).alias("precio_analisis"))
    )

    # Embudo de sesiones.
    s = sesiones
    pd_embudo = pl.DataFrame({
        "etapa": ["Sesiones", "Con al menos una vista", "Con al menos un carrito",
                  "Con al menos una compra", "Con carrito y compra",
                  "Con compra sin carrito en la sesión"],
        "sesiones": [s.height, int((s["total_visualizaciones"] > 0).sum()),
                     int(s["tiene_carrito"].sum()), int(s["compra"].sum()),
                     int((s["tiene_carrito"] & (s["compra"] == 1)).sum()),
                     int((~s["tiene_carrito"] & (s["compra"] == 1)).sum())],
    }).with_columns((pl.col("sesiones") / s.height * 100).alias("pct_sesiones"))
    pd_embudo.write_csv(TABLAS / "07_embudo_sesiones.csv")

    # Conversión mensual de las sesiones con carrito.
    (s.group_by("mes_inicio").agg(
        pl.len().alias("sesiones"),
        pl.col("tiene_carrito").sum().alias("con_carrito"),
        pl.col("compra").filter("tiene_carrito").sum().alias("con_carrito_y_compra"))
     .with_columns(
        (pl.col("con_carrito") / pl.col("sesiones") * 100).alias("pct_con_carrito"),
        (pl.col("con_carrito_y_compra") / pl.col("con_carrito") * 100)
        .alias("conversion_carrito_pct"))
     .sort("mes_inicio").write_csv(TABLAS / "07_conversion_mensual.csv"))

    # Conversión por día y hora del primer carrito (hora UTC).
    (s.filter("tiene_carrito").group_by("dia_semana_primer_carrito", "hora_primer_carrito")
     .agg(pl.len().alias("sesiones_carrito"), (pl.col("compra").mean() * 100)
          .alias("conversion_pct"))
     .sort("dia_semana_primer_carrito", "hora_primer_carrito")
     .write_csv(TABLAS / "07_conversion_dia_hora.csv"))

    # Embudo por categoría principal (10 con más eventos).
    total = base.select(pl.len()).collect(engine="streaming").item()
    cat = (embudo(base, "categoria_principal")
           .join(productos.group_by("categoria_principal").agg(
               pl.col("category_code_principal").filter(
                   pl.col("category_code_principal") != "unknown").first()
               .alias("category_code")), on="categoria_principal", how="left")
           .with_columns((pl.col("eventos") / total * 100).alias("pct_eventos"))
           .sort("eventos", descending=True).collect(engine="streaming"))
    cat.head(10).write_csv(TABLAS / "07_embudo_categorias_top10.csv")
    pl.DataFrame({"categorias": [cat.height],
                  "pct_eventos_top10": [cat.head(10)["pct_eventos"].sum()]}) \
        .write_csv(TABLAS / "07_resumen_categorias.csv")

    # Embudo por marca: 10 marcas con más eventos y «unknown».
    marca = (embudo(base.with_columns(pl.col("brand").cast(pl.String)), "brand")
             .with_columns((pl.col("eventos") / total * 100).alias("pct_eventos"))
             .sort("eventos", descending=True).collect(engine="streaming"))
    top = marca.filter(pl.col("brand") != "unknown").head(10)
    conocidas = (embudo(base.with_columns(
        pl.when(pl.col("marca_desconocida")).then(pl.lit("Marca desconocida"))
        .otherwise(pl.lit("Marca conocida")).alias("grupo_marca")), "grupo_marca")
        .with_columns((pl.col("eventos") / total * 100).alias("pct_eventos"))
        .collect(engine="streaming").rename({"grupo_marca": "brand"}))
    pl.concat([top, conocidas.select(top.columns)]).write_csv(
        TABLAS / "07_embudo_marcas.csv")

    # Embudo por rango de precio (precio_analisis).
    (embudo(base.filter(pl.col("precio_analisis").is_not_null()).with_columns(
        pl.col("precio_analisis").cut(CORTES_PRECIO, labels=ETIQUETAS_PRECIO,
                                      left_closed=True).alias("rango_precio")),
        "rango_precio")
     .with_columns((pl.col("eventos") / total * 100).alias("pct_eventos"))
     .sort("rango_precio").collect(engine="streaming")
     .with_columns(pl.col("rango_precio").cast(pl.String))
     .write_csv(TABLAS / "07_embudo_precio.csv"))

    # Histograma de precios (escala logarítmica) por tipo de evento.
    bordes = [round(0.1 * (10 ** (i / 12)), 4) for i in range(0, 46)]
    (base.filter((pl.col("precio_analisis") > 0) & es.is_in(["view", "cart", "purchase"]))
     .with_columns(pl.col("precio_analisis").cut(bordes, include_breaks=True)
                   .struct.field("breakpoint").alias("hasta"),
                   pl.col("event_type").cast(pl.String))
     .group_by("event_type", "hasta").agg(pl.len().alias("eventos"))
     .sort("event_type", "hasta").collect(engine="streaming")
     .write_csv(TABLAS / "07_histograma_precios.csv"))
    (base.filter(pl.col("precio_analisis") > 0)
     .group_by(pl.col("event_type").cast(pl.String)).agg(
        pl.col("precio_analisis").quantile(q).alias(f"p{int(q * 100)}")
        for q in (0.25, 0.5, 0.75, 0.9, 0.99))
     .sort("event_type").collect(engine="streaming")
     .write_csv(TABLAS / "07_percentiles_precio.csv"))

    # Distribuciones de cola larga.
    sesiones_por_usuario = s.group_by("user_id").len()["len"]
    filas = []
    for nombre, serie in [
        ("eventos por sesión", s["eventos"]),
        ("compras por sesión con compra", s.filter(pl.col("compra") == 1)["total_compras"]),
        ("sesiones por usuario", sesiones_por_usuario),
    ]:
        filas.append({"distribucion": nombre, "media": serie.mean(),
                      **{f"p{q}": serie.quantile(q / 100, "nearest") for q in (25, 50, 75, 90, 99)},
                      "maximo": serie.max()})
    pl.DataFrame(filas).write_csv(TABLAS / "07_distribuciones_cola_larga.csv")
    print("Tablas guardadas en", TABLAS)


# ----------------------------------------------------------------------
# Gráficos (mismo estilo que los gráficos de la exploración inicial)
# ----------------------------------------------------------------------

def graficar() -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    FIGURAS.mkdir(parents=True, exist_ok=True)
    AZUL = "#1f77b4"
    plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False,
                         "font.size": 10, "axes.titlesize": 12})

    def millones(x, _):
        return f"{x / 1e6:.1f} M".replace(".", ",")

    def pct_eje(x, _):
        return f"{x:.0f} %"

    def miles_es(n):
        return f"{n:,.0f}".replace(",", ".")

    def pct_es(x, decimales=1):
        return f"{x:.{decimales}f} %".replace(".", ",")

    def guardar(fig, nombre):
        fig.tight_layout()
        fig.savefig(FIGURAS / nombre, dpi=200)
        plt.close(fig)

    # 1. Embudo de sesiones.
    e = pl.read_csv(TABLAS / "07_embudo_sesiones.csv").head(4)
    fig, ax = plt.subplots(figsize=(9, 4.4))
    etiquetas = ["Sesiones", "Con al menos\nuna vista", "Con al menos\nun carrito",
                 "Con al menos\nuna compra"]
    valores = e["sesiones"].to_list()
    ax.barh(etiquetas[::-1], valores[::-1], color=AZUL, height=0.6)
    for i, v in enumerate(valores[::-1]):
        pct = v / valores[0] * 100
        ax.text(v + valores[0] * 0.01, i, f"{miles_es(v)}  ({pct_es(pct)})", va="center")
    ax.xaxis.set_major_formatter(FuncFormatter(millones))
    ax.set_xlim(0, valores[0] * 1.3)
    ax.set_xlabel("Número de sesiones")
    ax.set_title("Embudo de sesiones: vista → carrito → compra")
    guardar(fig, "fig_embudo_sesiones.png")

    # 2. Evolución mensual de la actividad.
    m = pl.read_csv(TABLAS / "07_eventos_mes_tipo.csv")
    tot = m.group_by("mes").agg(pl.col("eventos").sum()).sort("mes")
    x = [MESES_ETIQUETA[v] for v in tot["mes"]]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(x, tot["eventos"], marker="o", color=AZUL, linewidth=2.5)
    for xi, yi in zip(x, tot["eventos"]):
        ax.annotate(f"{yi / 1e6:.2f} M".replace(".", ","), (xi, yi), textcoords="offset points",
                    xytext=(0, 10), ha="center")
    ax.set_ylim(0, tot["eventos"].max() * 1.22)
    ax.yaxis.set_major_formatter(FuncFormatter(millones))
    ax.set_ylabel("Cantidad de eventos")
    ax.grid(axis="y", alpha=0.3)
    ax.set_title("Evolución mensual de la actividad")
    guardar(fig, "fig_evolucion_mensual.png")

    # 3. Composición de las interacciones por mes.
    orden = ["view", "cart", "remove_from_cart", "purchase"]
    nombres = {"view": "Visualización", "cart": "Carrito",
               "remove_from_cart": "Retiro del carrito", "purchase": "Compra"}
    colores = {"view": "#2a5d8f", "cart": "#3aa6a0", "remove_from_cart": "#f1b55b",
               "purchase": "#dc6b67"}
    piv = m.pivot(on="event_type", index="mes", values="eventos").sort("mes")
    totales = sum(piv[c] for c in orden)
    fig, ax = plt.subplots(figsize=(9, 5))
    abajo = [0.0] * piv.height
    for c in orden:
        pct = (piv[c] / totales * 100).to_list()
        ax.bar(x, pct, bottom=abajo, color=colores[c], width=0.7, label=nombres[c],
               edgecolor="white", linewidth=1)
        for i, (b, p) in enumerate(zip(abajo, pct)):
            if p > 8:
                ax.text(i, b + p / 2, pct_es(p), ha="center",
                        va="center", color="white", fontweight="bold")
        abajo = [b + p for b, p in zip(abajo, pct)]
    ax.set_ylabel("Porcentaje de eventos")
    ax.yaxis.set_major_formatter(FuncFormatter(pct_eje))
    ax.legend(ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.08), frameon=False)
    ax.set_title("Composición de las interacciones por mes")
    guardar(fig, "fig_composicion_mensual.png")

    # 4. Precios promedio y mediana por mes.
    p = pl.read_csv(TABLAS / "07_precios_mes.csv").sort("mes")
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(x, p["promedio"], marker="o", linewidth=2.5, color=AZUL, label="Promedio")
    ax.plot(x, p["mediana"], marker="s", linewidth=2.5, color="#ff7f0e", label="Mediana")
    ax.set_ylim(0, 11)
    ax.set_ylabel("Precio (unidad monetaria de la base)")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(frameon=True)
    ax.set_title("Precios registrados: promedio y mediana por mes")
    guardar(fig, "fig_precios_mes.png")

    # 5. Distribución de precios por tipo de evento (escala logarítmica).
    h = pl.read_csv(TABLAS / "07_histograma_precios.csv")
    fig, ax = plt.subplots(figsize=(9, 5))
    estilos = {"view": (AZUL, "-", "Visualización"), "cart": ("#3aa6a0", "--", "Carrito"),
               "purchase": ("#dc6b67", "-", "Compra")}
    for tipo, (color, linea, nombre) in estilos.items():
        d = h.filter(pl.col("event_type") == tipo).sort("hasta")
        d = d.filter(pl.col("hasta").is_finite())
        ax.step(d["hasta"], d["eventos"] / d["eventos"].sum() * 100, where="pre",
                color=color, linestyle=linea, linewidth=2, label=nombre)
    ax.set_xscale("log")
    ax.set_xticks([0.1, 0.5, 1, 2, 5, 10, 20, 50, 100, 300])
    ax.set_xticklabels(["0,1", "0,5", "1", "2", "5", "10", "20", "50", "100", "300"])
    ax.set_xlabel("Precio de análisis (escala logarítmica; unidad monetaria de la base)")
    ax.set_ylabel("Porcentaje de eventos del tipo")
    ax.yaxis.set_major_formatter(FuncFormatter(pct_eje))
    ax.grid(axis="y", alpha=0.3)
    ax.legend(frameon=False)
    ax.set_title("Distribución de precios por tipo de evento")
    guardar(fig, "fig_distribucion_precios.png")

    # 6. Embudo por rango de precio (dos paneles, misma escala de categorías).
    r = pl.read_csv(TABLAS / "07_embudo_precio.csv")
    r = r.with_columns(pl.col("rango_precio").replace_strict(
        {v: i for i, v in enumerate(ETIQUETAS_PRECIO)}, return_dtype=pl.Int8)
        .alias("_o")).sort("_o")
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.6), sharey=False)
    for ax, col, titulo in [
        (axes[0], "carrito_por_100_vistas", "Adiciones al carrito por cada 100 vistas"),
        (axes[1], "compras_por_100_carritos", "Compras por cada 100 adiciones al carrito"),
    ]:
        ax.bar(r["rango_precio"], r[col], color=AZUL, width=0.7)
        for i, v in enumerate(r[col]):
            ax.text(i, v + r[col].max() * 0.015, f"{v:.0f}", ha="center")
        ax.set_title(titulo, fontsize=11)
        ax.set_xlabel("Rango de precio de análisis")
        ax.set_ylim(0, r[col].max() * 1.15)
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle("Embudo por rango de precio", fontsize=12)
    guardar(fig, "fig_embudo_precio.png")

    # 7. Conversión de las sesiones con carrito por día y hora (UTC).
    c = pl.read_csv(TABLAS / "07_conversion_dia_hora.csv")
    mat = (c.pivot(on="hora_primer_carrito", index="dia_semana_primer_carrito",
                   values="conversion_pct").sort("dia_semana_primer_carrito"))
    horas = [str(hh) for hh in range(24)]
    valores = mat.select(horas).to_numpy()
    fig, ax = plt.subplots(figsize=(10, 4.2))
    im = ax.imshow(valores, aspect="auto", cmap="Blues")
    ax.set_yticks(range(7))
    ax.set_yticklabels(["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado",
                        "Domingo"])
    ax.set_xticks(range(24))
    ax.set_xticklabels(horas)
    ax.set_xlabel("Hora del primer carrito (UTC)")
    for spine in ax.spines.values():
        spine.set_visible(False)
    barra = fig.colorbar(im, ax=ax, pad=0.01)
    barra.set_label("Sesiones con carrito que compran (%)")
    ax.set_title("Conversión de las sesiones con carrito por día y hora")
    guardar(fig, "fig_conversion_dia_hora.png")
    print("Gráficos guardados en", FIGURAS)


def main() -> None:
    parser = argparse.ArgumentParser(description="Perfil exploratorio")
    parser.add_argument("--fuente", choices=FUENTES, default="muestra")
    parser.add_argument("--solo-tablas", action="store_true")
    parser.add_argument("--solo-graficos", action="store_true")
    args = parser.parse_args()
    TABLAS.mkdir(parents=True, exist_ok=True)
    if not args.solo_graficos:
        calcular_tablas(args.fuente)
    if not args.solo_tablas:
        graficar()


if __name__ == "__main__":
    main()
