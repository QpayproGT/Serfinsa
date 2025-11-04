def buscar_transaction_id(cursor, conn, seq_num):

    try:
        cursor.execute("""
            SELECT transaction_id, autorizationCode
            FROM transactions
            WHERE referencs = %s
            LIMIT 1
        """, (seq_num,))
        result = cursor.fetchone()
        if result:
            # Verificar si la columna qpay_transac_id ya existe
            cursor.execute("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'LiquidacionesSV' 
                AND COLUMN_NAME = 'qpay_transac_id'
                AND TABLE_SCHEMA = DATABASE()
            """)
            
            column_exists = cursor.fetchone()
            
            # Solo agregar la columna si no existe
            if not column_exists:
                cursor.execute("""
                    ALTER TABLE LiquidacionesSV ADD COLUMN qpay_transac_id VARCHAR(255)
                """)
                print("✅ Columna qpay_transac_id agregada a la tabla LiquidacionesSV")
            
            cursor.execute("""
                UPDATE LiquidacionesSV SET qpay_transac_id = %s WHERE SEQ_NUM = %s
            """, (result["transaction_id"], seq_num))
            conn.commit()

            print(f"Transaction_id encontrado para SEQ_NUM={seq_num}: {result['transaction_id']} (Auth: {result['autorizationCode']})")
            return result["transaction_id"]
        else:
            return None
    except Exception as e:
        print(f"⚠️ Error buscando transaction_id para SEQ_NUM={seq_num}: {e}")
        return None

def buscar_por_authorization_code(cursor, conn, auth_code):
    """
    Busca una transacción por su authorization code
    """
    try:
        cursor.execute("""
            SELECT transaction_id, orderNumber, referencs, amount, autorizationCode, status
            FROM transactions
            WHERE autorizationCode = %s
            LIMIT 1
        """, (auth_code,))
        result = cursor.fetchone()
        if result:
            print(f"Transacción encontrada por Auth Code {auth_code}: {result['transaction_id']} (Order: {result['orderNumber']})")
            return result
        else:
            print(f"No se encontró transacción con Auth Code: {auth_code}")
            return None
    except Exception as e:
        print(f"⚠️ Error buscando por authorization code {auth_code}: {e}")
        return None
