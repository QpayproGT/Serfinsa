import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
from conector import create_connection
from ReadFile import buscar_y_leer_excel
from BuscarTransaccion import buscar_transaction_id
from logger_config import setup_logger, log_separator
from email_sender import EmailSender
from dotenv import load_dotenv

load_dotenv()

def verificar_seq_num_existe(cursor, seq_num):
    """
    Verifica si el SEQ_NUM ya existe en la tabla LiquidacionesSV
    """
    try:
        cursor.execute("""
            SELECT SEQ_NUM FROM LiquidacionesSV WHERE SEQ_NUM = %s LIMIT 1
        """, (seq_num,))
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        # No usar logger aquí para evitar dependencias circulares
        print(f"Error verificando SEQ_NUM {seq_num}: {e}")
        return False 

def main():
    # Registrar tiempo de inicio
    start_time = time.time()
    start_datetime = datetime.now()
    
    # Configurar logging básico para el caso de error
    df, excel_file_path, search_path = buscar_y_leer_excel()
    if df is None:
        print("No se encontró ningún archivo Excel para procesar.")
        
        # Enviar email de alerta
        notification_email = os.getenv("NOTIFICATION_EMAIL")
        if notification_email:
            print("📧 Enviando email de alerta...")
            
            email_sender = EmailSender()
            subject = "Incidencia - No se encontró archivo Excel para procesar"
            alert_message = "No se encontró ningún archivo Excel para procesar."
            
            success, message = email_sender.send_alert_email(
                notification_email, 
                subject, 
                alert_message, 
                search_path
            )
            
            if success:
                print("✅ Email de alerta enviado exitosamente")
            else:
                print(f"❌ Error enviando email de alerta: {message}")
        else:
            print("⚠️ No se configuró NOTIFICATION_EMAIL en variables de entorno")
        
        return
    
    logger, log_file_path = setup_logger(excel_file_path)
    
    log_separator(logger, "=" * 60)
    logger.info(f"🚀 INICIANDO PROCESAMIENTO DE ARCHIVO: {excel_file_path}")
    logger.info(f"📝 Archivo de log: {log_file_path}")
    logger.info(f"⏰ Fecha y hora de inicio: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    log_separator(logger)
    
    conn = create_connection()
    if not conn:
        logger.error("❌ No se pudo conectar a la base de datos.")
        return

    cursor = conn.cursor(dictionary=True)
    logger.info("✅ Conexión a base de datos establecida")

    logger.info(f"📊 Archivo leído correctamente con {len(df)} filas y {len(df.columns)} columnas.")


    df = df.applymap(lambda x: None if pd.isna(x) or str(x).strip().lower() in ["nan", "none", ""] else x)
    logger.info("🧹 Datos limpios aplicados (valores NaN, None y vacíos convertidos a None)")

    logger.info("🔍 Vista previa de los datos limpios:")
    logger.info(f"Primeras 5 filas: {df.head().to_string()}")

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

    logger.info("🔄 Iniciando proceso de inserción en base de datos...")
    inserted = 0
    skipped = 0
    errors = 0
    
    for i, row in df.iterrows():
        seq_num = row["SEQ_NUM"]
        
        # Verificar si el SEQ_NUM ya existe en la base de datos
        if verificar_seq_num_existe(cursor, seq_num):
            logger.warning(f"⚠️ SEQ_NUM {seq_num} ya existe en la base de datos. Omitiendo registro...")
            skipped += 1
            continue
            
        try:
            cursor.execute(sql, tuple(row))
            inserted += 1
            logger.info(f"✅ Registro insertado: SEQ_NUM {seq_num}")
        except Exception as e:
            errors += 1
            logger.error(f"❌ Error al insertar fila {i + 1} (SEQ_NUM: {seq_num}): {e}")
            logger.error(f"➡️ Datos problemáticos: {tuple(row)}")

    conn.commit()
    logger.info("💾 Cambios confirmados en base de datos")
    
    log_separator(logger)
    logger.info("📊 RESUMEN DE LA INSERCIÓN:")
    logger.info(f"✅ Se insertaron {inserted} filas nuevas en la tabla LiquidacionesSV")
    logger.info(f"⚠️ Se omitieron {skipped} filas que ya existían en la base de datos")
    logger.info(f"❌ Se encontraron {errors} errores durante la inserción")
    logger.info(f"📝 Total de registros procesados: {len(df)}")
    log_separator(logger)

    # Buscar transaction_id solo para los registros que se insertaron correctamente
    logger.info("🔍 Iniciando búsqueda de transaction_id para los registros insertados...")
    processed_transactions = 0
    transactions_found = 0
    
    for i, row in df.iterrows():
        seq_num = row["SEQ_NUM"]
        
        # Solo procesar si el registro existe en la base de datos (fue insertado)
        if not verificar_seq_num_existe(cursor, seq_num):
            continue
            
        transaction_id = buscar_transaction_id(cursor, conn, seq_num)
        if transaction_id:
            logger.info(f"✅ Transaction_id encontrado para SEQ_NUM={seq_num}: {transaction_id}")
            transactions_found += 1
        else:
            logger.warning(f"❌ No se encontró transaction_id para SEQ_NUM={seq_num}")
        processed_transactions += 1
    
    logger.info(f"📋 Se procesaron {processed_transactions} registros para buscar transaction_id")
    logger.info(f"🎯 Se encontraron {transactions_found} transaction_id válidos")

    conn.close()
    logger.info("🔌 Conexión a base de datos cerrada")
    
    # Calcular tiempo total de procesamiento
    end_time = time.time()
    processing_time = end_time - start_time
    processing_time_formatted = f"{processing_time:.2f} segundos"
    
    logger.info(f"⏱️ Tiempo total de procesamiento: {processing_time_formatted}")
    
    # Preparar estadísticas para el email
    summary_stats = {
        'inserted': inserted,
        'skipped': skipped,
        'errors': errors,
        'transactions_found': transactions_found,
        'total_processed': len(df)
    }
    
    # Enviar email de notificación
    notification_email = os.getenv("NOTIFICATION_EMAIL")
    if notification_email:
        logger.info("📧 Enviando email de notificación...")
        
        email_sender = EmailSender()
        subject = f"Reporte de Procesamiento Serfinsa - {os.path.basename(excel_file_path)}"
        body = email_sender.create_email_body(
            os.path.basename(excel_file_path), 
            summary_stats, 
            processing_time_formatted
        )
        
        success, message = email_sender.send_notification_email(
            notification_email, 
            subject, 
            body, 
            log_file_path,
            excel_file_path
        )
        
        if success:
            logger.info("✅ Email de notificación enviado exitosamente")
        else:
            logger.error(f"❌ Error enviando email: {message}")
    else:
        logger.warning("⚠️ No se configuró NOTIFICATION_EMAIL en variables de entorno")
    
    log_separator(logger)
    logger.info("🏁 PROCESAMIENTO PRINCIPAL COMPLETADO EXITOSAMENTE")
    log_separator(logger, "=" * 60)
    
    # Ejecutar búsqueda de transacciones faltantes
    logger.info("🔍 Iniciando búsqueda de transacciones faltantes...")
    try:
        from BuscarTransaccionesFaltantes import main as buscar_faltantes
        buscar_faltantes()
        logger.info("✅ Búsqueda de transacciones faltantes completada")
    except Exception as e:
        logger.error(f"❌ Error en búsqueda de transacciones faltantes: {e}")
    
    log_separator(logger)
    logger.info("🏁 PROCESAMIENTO COMPLETO FINALIZADO")
    log_separator(logger, "=" * 60)

if __name__ == "__main__":
    main()
