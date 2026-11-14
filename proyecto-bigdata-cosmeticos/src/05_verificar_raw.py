"""
05 - VERIFICAR QUE LOS DATOS ORIGINALES SIGUEN INTACTOS (actividad 14).

Responsable: Gabriela (Fase 2).

Vuelve a calcular la huella SHA-256 de cada CSV de data/raw/ y la compara
con la que se registró en la ingesta (reports/tablas/01_huellas_sha256_raw.csv).
Si un solo byte de un archivo cambió, la huella es distinta y el script
lo informa como ERROR.

Salida: reports/tablas/05_verificacion_raw.csv

Uso
---
    python src/05_verificar_raw.py
    python src/05_verificar_raw.py --raw "C:/ruta/a/Datos"
"""

import argparse
import hashlib
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import RAW, TABLAS, relativa  # noqa: E402


def huella_sha256(ruta: Path, bloque: int = 8 * 1024 ** 2) -> str:
    h = hashlib.sha256()
    with ruta.open("rb") as f:
        while datos := f.read(bloque):
            h.update(datos)
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="Verificar data/raw")
    parser.add_argument("--raw", type=Path, default=RAW)
    args = parser.parse_args()

    registro = TABLAS / "01_huellas_sha256_raw.csv"
    if not registro.exists():
        raise FileNotFoundError(
            f"No existe {relativa(registro)}. Ejecute primero src/01_ingesta.py"
        )

    print("=" * 72)
    print("05 - VERIFICACIÓN DE LOS CSV ORIGINALES")
    print("=" * 72)

    esperadas = pl.read_csv(registro)
    filas, errores = [], 0
    for fila in esperadas.iter_rows(named=True):
        ruta = args.raw / fila["archivo"]
        if not ruta.exists():
            estado, actual = "ERROR: no existe", ""
        else:
            actual = huella_sha256(ruta)
            estado = "OK" if actual == fila["sha256"] else "ERROR: cambió"
        errores += estado != "OK"
        filas.append({
            "archivo": fila["archivo"],
            "sha256_registrada": fila["sha256"],
            "sha256_actual": actual,
            "resultado": estado,
            "fecha_verificacion": datetime.now().isoformat(timespec="seconds"),
        })
        print(f"  {fila['archivo']}: {estado}")

    pl.DataFrame(filas).write_csv(TABLAS / "05_verificacion_raw.csv")
    if errores:
        print(f"\n{errores} archivo(s) no coinciden con el registro.")
        sys.exit(1)
    print("\nLos cinco CSV originales están intactos.")


if __name__ == "__main__":
    main()
