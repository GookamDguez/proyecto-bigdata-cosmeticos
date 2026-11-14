"""
EJECUTAR TODO EL PROCESO DE PREPARACIÓN EN ORDEN (actividad 14).

Va de los CSV originales al dataset preparado sin pasos manuales.

Uso
---
Desde cero, con los datos completos (requiere los CSV en data/raw/):

    python src/run_pipeline.py

    Ejecuta: 01_ingesta -> 02_muestra -> 03_limpieza -> 04_integracion
             -> 05_verificar_raw

Prueba rápida con la muestra (requiere data/interim/sample/):

    python src/run_pipeline.py --fuente muestra

    Ejecuta: 03_limpieza -> 04_integracion

Opciones:
    --sin-pandas   omite la prueba de carga con pandas en la ingesta
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

SRC = Path(__file__).resolve().parent


def ejecutar(script: str, *argumentos: str) -> None:
    comando = [sys.executable, str(SRC / script), *argumentos]
    print(f"\n>>> {' '.join([Path(comando[0]).name, *comando[1:]])}\n", flush=True)
    subprocess.run(comando, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Proceso completo de preparación")
    parser.add_argument("--fuente", choices=("muestra", "completo"),
                        default="completo")
    parser.add_argument("--sin-pandas", action="store_true")
    args = parser.parse_args()

    inicio = time.perf_counter()
    if args.fuente == "completo":
        ejecutar("01_ingesta.py", *(["--sin-pandas"] if args.sin_pandas else []))
        ejecutar("02_muestra.py")
    ejecutar("03_limpieza.py", "--fuente", args.fuente)
    ejecutar("04_integracion.py", "--fuente", args.fuente)
    if args.fuente == "completo":
        ejecutar("05_verificar_raw.py")

    minutos = (time.perf_counter() - inicio) / 60
    print(f"\nProceso terminado ({args.fuente}) en {minutos:.1f} minutos.")


if __name__ == "__main__":
    main()
