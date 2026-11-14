# Actividad 6: permisos de uso, licencia, privacidad, anonimización y sesgos

Responsable: Gabriela · Fase 1 · Insumos: matriz de fuentes (actividad 4) y
diagnóstico de calidad del equipo (`02_integridad_relaciones.py`,
`03_diagnostico_calidad.py`).

## 1. Licencia y condiciones de uso

| Aspecto | Situación |
|---|---|
| Fuente | *eCommerce events history in cosmetics shop*, Kaggle, publicado por mkechinov con datos de REES46 (Open CDP) |
| Enlace | https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop |
| Licencia declarada | «Data files © Original Authors» |
| Qué significa | No es una licencia abierta (no es Creative Commons ni ODbL). Los derechos siguen siendo de los autores originales y el acceso está sujeto a los términos de uso de Kaggle. No hay un permiso explícito para redistribuir los archivos. |

Decisiones del equipo para respetar esas condiciones:

- Los datos se usan solo con fines académicos y sin fines comerciales.
- Se cita la fuente (Kaggle, mkechinov, REES46/Open CDP) en el informe y en el repositorio.
- Los archivos no se redistribuyen: no se suben a GitHub (`.gitignore` excluye
  `data/`) y solo se comparten entre los integrantes por Google Drive.
- Solo se publican resultados agregados (conteos, tasas, gráficos y modelos).

## 2. Privacidad y anonimización

- El conjunto **no contiene datos personales directos**: no hay nombres, correos,
  teléfonos, direcciones, IP, ubicación ni datos demográficos.
- `user_id` es un número permanente que identifica al comprador de forma
  **seudónima**, y `user_session` es un identificador temporal (UUID) por visita.
  Ninguno de los dos permite saber quién es la persona.
- `brand` contiene marcas comerciales, que no son datos personales.
- Riesgo residual: un identificador seudónimo no equivale a anonimización total.
  Un historial de navegación muy detallado podría, en teoría, vincularse con otra
  fuente. Para evitarlo, el equipo:
  1. no intenta reidentificar a ningún usuario ni cruza los datos con otras fuentes;
  2. no publica listados de `user_id` ni de `user_session`, de modo que las tablas
     de diagnóstico con identificadores individuales quedan fuera del repositorio;
  3. presenta los resultados de forma agregada.

## 3. Posibles sesgos y limitaciones de los datos

| # | Sesgo o limitación | Evidencia | Efecto posible | Cómo se maneja |
|---|---|---|---|---|
| 1 | Una sola tienda y país desconocido | La fuente describe una tienda en línea de cosméticos y no indica el país | Los resultados no se pueden generalizar a otras tiendas ni a otros mercados | Las conclusiones se limitan a esta tienda |
| 2 | Periodo corto y estacional | Solo cinco meses (octubre de 2019 a febrero de 2020), con temporadas de ofertas y fiestas (Black Friday en noviembre, fin de año en diciembre) | Las tasas de conversión pueden reflejar la temporada más que el comportamiento habitual | Comparar por mes y tenerlo en cuenta en la partición temporal |
| 3 | Faltantes concentrados en ciertas columnas | `category_code` está vacío en el 98.3 % de los eventos y `brand` en el 42.3 %. 23,473 de 54,571 productos (43 %) nunca tienen marca | Un análisis por marca representaría solo a los productos que sí la tienen | Reemplazar por «unknown» con una columna bandera, recuperar la marca cuando el producto tiene una sola conocida (98,300 eventos) y usar `category_id` como categoría principal |
| 4 | Duplicados exactos | 1,109,098 filas sobrantes (5.4 % del total). El 89.4 % son `remove_from_cart` | Si se cuentan tal cual, inflan el abandono del carrito | Se marcan con una bandera y la decisión de conservarlos o eliminarlos se justifica en la actividad 8 |
| 5 | Precios anómalos | 127 compras con precio ≤ 0. 126 corresponden a solo 5 productos de una categoría, con montos negativos fijos, lo que sugiere descuentos o cupones. También hay precios 0 en vistas y carritos, concentrados en febrero de 2020 (43,423 carritos con precio 0, frente a entre 540 y 2,528 en los otros meses) | Distorsionan el gasto, el precio promedio y la segmentación por monto | Se marcan con banderas y no se usan para calcular montos; la anomalía de febrero se documenta |
| 6 | Actividad anómala (posibles bots o registros técnicos) | 272 sesiones tienen más de un `user_id`; en muchas, los usuarios tienen identificadores muy cercanos entre sí y un solo evento cada uno. Hay sesiones con hasta 429 compras registradas en el mismo segundo | Pueden alterar promedios, conversiones y segmentos | Se marcan con banderas y se revisa si conviene excluirlas del modelado |
| 7 | Variables que no existen en la fuente | No hay datos demográficos, devoluciones, márgenes ni canal de adquisición | No se puede evaluar la equidad por grupos de personas ni la rentabilidad | Se declara como limitación del alcance |

## 4. Párrafo para el documento de formulación

> **Uso responsable de los datos.** El proyecto usa el conjunto público
> *eCommerce events history in cosmetics shop* (Kaggle; REES46/Open CDP). La
> licencia declarada («Data files © Original Authors») no es abierta, por lo que
> los datos se usan solo con fines académicos, se cita la fuente y no se
> redistribuyen: no se publican en el repositorio y solo se comparten dentro del
> equipo. El conjunto no contiene datos personales directos; los usuarios aparecen
> con identificadores seudónimos (`user_id`, `user_session`), que el equipo no
> intentará reidentificar ni cruzar con otras fuentes, y los resultados se
> presentan solo de forma agregada. Los datos tienen limitaciones que pueden sesgar
> los hallazgos: provienen de una sola tienda de país desconocido; cubren solo
> cinco meses con temporadas de ofertas; la categoría textual falta en el 98.3 % de
> los eventos y la marca en el 42.3 %; el 5.4 % de las filas son
> duplicados exactos, sobre todo de retiros del carrito; hay precios nulos o
> negativos y actividad anómala, como sesiones compartidas por varios usuarios o
> cientos de compras en un mismo segundo. Estas situaciones se marcan y se tratan
> de forma documentada en la fase 2, y las conclusiones se limitarán a esta tienda
> y a este periodo.
