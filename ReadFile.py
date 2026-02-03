import os
import glob
import pandas as pd

def buscar_y_leer_excel():


    if os.path.exists("/var/www/vhosts/serfinsa.qpaypro.com/data"):
        base_path = "/var/www/vhosts/serfinsa.qpaypro.com/data"
    else:
        base_path = os.path.join(os.getcwd(), "data")

    pattern = os.path.join(base_path, "**", "Serfinsa*.xlsx")

    print(f"Buscando archivos en: {base_path}")
    files_found = glob.glob(pattern, recursive=True)

    if not files_found:
        print(f"No se encontró ningún archivo Excel con el patrón 'Serfinsa*.xlsx' dentro de {base_path}/")
        return None, None, base_path

    files_found.sort(key=os.path.getmtime, reverse=True)
    excel_file = files_found[0]

    print(f"Archivo encontrado: {excel_file}")

    try:
        df = pd.read_excel(excel_file, engine="openpyxl")
        print(f"Archivo leído correctamente ({len(df)} filas, {len(df.columns)} columnas).")
        print(df.head())  # muestra solo primeras filas
        
        # DEBUG: Mostrar todos los SEQ_NUM tal como vienen del Excel
        if 'SEQ_NUM' in df.columns:
            print(f"\n🔍 DEBUG ReadFile - SEQ_NUMs encontrados en Excel (raw):")
            for idx, seq in enumerate(df['SEQ_NUM']):
                print(f"   Fila {idx}: SEQ_NUM = {seq} (tipo: {type(seq)}, valor raw: {repr(seq)})")
        
        return df, excel_file, base_path

    except Exception as e:
        print(f"Error al leer el archivo Excel: {e}")
        return None, None, base_path
