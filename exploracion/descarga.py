
from pathlib import Path
import kagglehub

# ==================================================
# 1. CONFIGURACIÓN DE RUTAS
# ==================================================

# Carpeta donde está guardado descarga.py
BASE_DIR = Path(__file__).resolve().parent

# Crear la carpeta Datos dentro de Prueba 3
CARPETA_DATOS = BASE_DIR / "Datos"
CARPETA_DATOS.mkdir(parents=True, exist_ok=True)

# Dataset de Kaggle
DATASET = "mkechinov/ecommerce-events-history-in-cosmetics-shop"

# ==================================================
# 2. DESCARGAR DATASET
# ==================================================

print("=" * 60)
print("DESCARGA DE DATOS - COSMETICS SHOP")
print("=" * 60)

print(f"\nCarpeta de destino: {CARPETA_DATOS}")

ruta = kagglehub.dataset_download(
    DATASET,
    output_dir=str(CARPETA_DATOS)
)

# ==================================================
# 3. VERIFICAR ARCHIVOS
# ==================================================

print("\nDESCARGA COMPLETADA")
print(f"Ubicación: {ruta}")

print("\nARCHIVOS ENCONTRADOS:")

total_gb = 0

for archivo in sorted(CARPETA_DATOS.rglob("*")):
    if archivo.is_file():
        tamano_gb = archivo.stat().st_size / (1024 ** 3)
        total_gb += tamano_gb

        print(f"\nArchivo: {archivo.name}")
        print(f"Tamaño: {tamano_gb:.3f} GiB")

print("\n" + "=" * 60)
print(f"TAMAÑO TOTAL: {total_gb:.3f} GiB")
print("=" * 60)
