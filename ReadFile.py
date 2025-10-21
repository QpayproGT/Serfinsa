import os
import glob
import pandas as pd

def buscar_y_leer_excel():
    base_path = os.path.join(os.getcwd(), "data")
    pattern = os.path.join(base_path, "**", "Serfinsa*.xlsx")

    files_found = glob.glob(pattern, recursive=True)

    if not files_found:
        print("❌ No se encontró ningún archivo Excel con el patrón 'Serfinsa*.xlsx' dentro de /data/")
        return None

    excel_file = files_found[0]
    print(f"✅ Archivo encontrado: {excel_file}")

    try:
        df = pd.read_excel(excel_file)
        print("✅ Archivo leído correctamente.")
        print(df.head())  # Solo muestra las primeras filas
        return df
    except Exception as e:
        print(f"❌ Error al leer el archivo Excel: {e}")
        return None
