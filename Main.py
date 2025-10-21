import pandas as pd
import numpy as np
from conector import create_connection
from ReadFile import buscar_y_leer_excel

def main():
    conn = create_connection()
    if not conn:
        print("No se pudo conectar a la base de datos.")
        return

    cursor = conn.cursor()

    df = buscar_y_leer_excel()
    if df is None:
        print("No se encontró ningún archivo Excel para procesar.")
        return

    print(f"Archivo leído correctamente con {len(df)} filas y {len(df.columns)} columnas.")


    df = df.applymap(lambda x: None if pd.isna(x) or str(x).strip().lower() in ["nan", "none", ""] else x)

    print("🔍 Vista previa de los datos limpios:")
    print(df.head())

    sql = """
        INSERT INTO LiquidacionesSV (
            FECHA_TRAN, HORA_TRAN, ID_PAG, SUCURSAL_I, TERMINAL_I, AFILIADO, NOMBRE_COM,
            EMISOR_ID, PAN, MONTO_TRAN, MONTO_AJUS, MONTO_TEXE, SUBTOTAL, MONTO_IVA,
            COMISIONAB, COM_PORCEN, COM_MONTO, COM_MTOIVA, RETENCION2, RETENIDO,
            MONTO_DEBI, DEPOSITO, CCFNO, DCLNO, TIPO_TRANS, MESES_PLZO, PAGADO,
            BCO_PAGO, NUMCTA, REG_FISCAL, IVA_PORC, APROBAC, TC, TYP, SEQ_NUM,
            INVOIC_NUM, RESP_CDE, MODO_ENTRA, COMPRADOR, ORDEN_ID
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    inserted = 0
    for i, row in df.iterrows():
        try:
            cursor.execute(sql, tuple(row))
            inserted += 1
        except Exception as e:
            print(f"Error al insertar fila {i + 1}: {e}")
            print("➡️ Datos problemáticos:", tuple(row))

    conn.commit()
    print(f"Se insertaron {inserted} filas en la tabla LiquidacionesSV correctamente.")

    conn.close()
    print("Conexión cerrada.")

if __name__ == "__main__":
    main()
