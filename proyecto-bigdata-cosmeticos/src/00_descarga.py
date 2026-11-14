"""
00 - DESCARGA de los datos originales desde Kaggle.

Responsable: Gabriela (Fase 1, paso previo).
Basado en el script descarga.py del equipo; ahora guarda en data/raw/.

Requisitos
----------
- pip install kagglehub
- Credenciales de Kaggle del propio usuario (kaggle.json en ~/.kaggle/ o
  variables KAGGLE_USERNAME y KAGGLE_KEY). El token es personal: nunca se
  sube al repositorio (está en .gitignore).

Si ya tiene los cinco CSV descargados, no necesita este script: basta con
copiarlos a data/raw/.

Uso
---
    python src/00_descarga.py
"""

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATASET_KAGGLE, MESES, RAW  # noqa: E402


def main() -> None:
    import kagglehub

    RAW.mkdir(parents=True, exist_ok=True)
    print(f"Descargando {DATASET_KAGGLE} ...")
    ruta = Path(kagglehub.dataset_download(DATASET_KAGGLE))

    # kagglehub puede dejar los archivos en su caché o en subcarpetas;
    # se copian a data/raw/ solo los cinco CSV esperados.
    for mes in MESES:
        encontrados = list(ruta.rglob(f"{mes}.csv"))
        if not encontrados:
            raise FileNotFoundError(f"No se encontró {mes}.csv en {ruta}")
        destino = RAW / f"{mes}.csv"
        if not destino.exists():
            shutil.copy2(encontrados[0], destino)
        print(f"  {destino.name}: {destino.stat().st_size / 1024**2:,.1f} MiB")

    print(f"\nCSV originales en: {RAW}")
    print("No modifique estos archivos; todo el proceso trabaja sobre copias.")


if __name__ == "__main__":
    main()
