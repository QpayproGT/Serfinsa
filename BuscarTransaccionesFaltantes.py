#!/usr/bin/env python3
"""
Script para buscar transacciones faltantes en LiquidacionesSV
Busca transacciones donde payment_method_id = 10 que no están en LiquidacionesSV
"""

import os
import pandas as pd
from datetime import datetime
from conector import create_connection
from email_sender import EmailSender
from logger_config import setup_logger, log_separator
from dotenv import load_dotenv

load_dotenv()

def buscar_transacciones_faltantes():
    """
    Busca transacciones donde payment_method_id = 10 que no están en LiquidacionesSV
    """
    conn = create_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos.")
        return None, None
    
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Consulta SQL para buscar transacciones faltantes
        query = """
        SELECT 
            t.transaction_id,
            t.orderNumber,
            t.referencs,
            t.amount,
            t.autorizationCode,
            t.currency,
            t.status,
            t.created_at,
            t.updated_at,
            pg.payment_method_id,
            pm.name as payment_method_name,
            t.email,
            t.bill_to_name
        FROM transactions t
        INNER JOIN payment_gateway pg ON t.payment_gateway_id = pg.payment_gateway_id
        INNER JOIN payment_method pm ON pg.payment_method_id = pm.payment_method_id
        WHERE pg.payment_method_id = 10
        AND t.transaction_id NOT IN (
            SELECT qpay_transac_id 
            FROM LiquidacionesSV
            WHERE qpay_transac_id IS NOT NULL
        )
        AND t.status = 1
        ORDER BY t.created_at DESC
        """
        
        cursor.execute(query)
        transacciones_faltantes = cursor.fetchall()
        
        print(f"🔍 Consulta ejecutada exitosamente")
        print(f"📊 Se encontraron {len(transacciones_faltantes)} transacciones faltantes")
        
        return transacciones_faltantes, conn
        
    except Exception as e:
        print(f"❌ Error ejecutando consulta: {e}")
        conn.close()
        return None, None

def generar_reporte_excel(transacciones_faltantes, archivo_salida):
    """
    Genera un reporte Excel con las transacciones faltantes
    """
    try:
        if not transacciones_faltantes:
            print("⚠️ No hay transacciones para generar reporte")
            return None
        
        # Convertir a DataFrame
        df = pd.DataFrame(transacciones_faltantes)
        
        # Reordenar columnas para mejor presentación
        columnas_ordenadas = [
            'transaction_id',
            'orderNumber',
            'referencs', 
            'amount',
            'autorizationCode',
            'currency',
            'status',
            'payment_method_name',
            'email',
            'bill_to_name',
            'created_at',
            'updated_at'
        ]
        
        df = df[columnas_ordenadas]
        
        # Renombrar columnas para mejor legibilidad
        df.columns = [
            'Transaction ID',
            'Order Number',
            'Referencs',
            'Amount',
            'Authorization Code',
            'Currency',
            'Status',
            'Payment Method',
            'Email',
            'Bill To Name',
            'Created At',
            'Updated At'
        ]
        
        # Guardar en Excel
        df.to_excel(archivo_salida, index=False, engine='openpyxl')
        
        print(f"✅ Reporte Excel generado: {archivo_salida}")
        print(f"📊 Total de transacciones en el reporte: {len(df)}")
        
        return archivo_salida
        
    except Exception as e:
        print(f"❌ Error generando reporte Excel: {e}")
        return None

def enviar_reporte_email(transacciones_faltantes, archivo_excel, logger):
    """
    Envía un email con el reporte de transacciones faltantes
    """
    notification_email = os.getenv("NOTIFICATION_EMAIL")
    if not notification_email:
        logger.warning("⚠️ No se configuró NOTIFICATION_EMAIL en variables de entorno")
        return False
    
    try:
        email_sender = EmailSender()
        
        # Crear asunto y cuerpo del email
        subject = f"Reporte de Transacciones Faltantes - {len(transacciones_faltantes)} transacciones"
        
        # Crear cuerpo HTML del email
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #17a2b8; color: white; padding: 15px; border-radius: 5px; }}
                .info {{ background-color: #d1ecf1; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                .stats {{ background-color: #e9ecef; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>📊 Reporte de Transacciones Faltantes</h2>
                <p><strong>Fecha y hora:</strong> {current_time}</p>
            </div>
            
            <div class="info">
                <h3>🔍 Resumen del Reporte</h3>
                <p>Se encontraron <strong>{len(transacciones_faltantes)}</strong> transacciones que:</p>
                <ul>
                    <li>✅ Tienen payment_method_id = 10 (método de pago específico)</li>
                    <li>✅ Tienen status = 1 (transacciones exitosas)</li>
                    <li>❌ NO están registradas en la tabla LiquidacionesSV</li>
                </ul>
            </div>
            
            <div class="stats">
                <h3>📋 Detalles del Procesamiento</h3>
                <p>Se adjunta el archivo Excel con el detalle completo de las transacciones faltantes.</p>
                <p><strong>Archivo adjunto:</strong> {os.path.basename(archivo_excel) if archivo_excel else 'No generado'}</p>
            </div>
            
            <div style="margin-top: 20px; padding: 10px; background-color: #f8f9fa; border-radius: 5px;">
                <p><strong>Nota:</strong> Este reporte se genera automáticamente para identificar transacciones que deberían estar en las liquidaciones.</p>
            </div>
        </body>
        </html>
        """
        
        # Enviar email con adjunto
        success, message = email_sender.send_notification_email(
            notification_email,
            subject,
            html_body,
            excel_file_path=archivo_excel
        )
        
        if success:
            logger.info("✅ Email de reporte enviado exitosamente")
            return True
        else:
            logger.error(f"❌ Error enviando email de reporte: {message}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error preparando email de reporte: {e}")
        return False

def main():
    """
    Función principal para buscar transacciones faltantes
    """
    start_time = datetime.now()
    print(f"🚀 Iniciando búsqueda de transacciones faltantes - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configurar logging
    logger, log_file_path = setup_logger("transacciones_faltantes")
    
    log_separator(logger, "=" * 60)
    logger.info("🔍 INICIANDO BÚSQUEDA DE TRANSACCIONES FALTANTES")
    logger.info(f"📝 Archivo de log: {log_file_path}")
    log_separator(logger)
    
    # Buscar transacciones faltantes
    logger.info("🔍 Buscando transacciones con payment_method_id = 10...")
    transacciones_faltantes, conn = buscar_transacciones_faltantes()
    
    if conn:
        conn.close()
        logger.info("🔌 Conexión a base de datos cerrada")
    
    if transacciones_faltantes is None:
        logger.error("❌ Error en la búsqueda de transacciones")
        return
    
    # Generar reporte Excel
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archivo_reporte = f"logs/transacciones_faltantes_{timestamp}.xlsx"
    
    logger.info(f"📊 Generando reporte Excel...")
    archivo_generado = generar_reporte_excel(transacciones_faltantes, archivo_reporte)
    
    # Enviar email con reporte
    if len(transacciones_faltantes) > 0:
        logger.info("📧 Enviando reporte por email...")
        enviar_reporte_email(transacciones_faltantes, archivo_generado, logger)
    else:
        logger.info("✅ No se encontraron transacciones faltantes")
    
    # Resumen final
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    
    log_separator(logger)
    logger.info("📊 RESUMEN FINAL:")
    logger.info(f"✅ Transacciones encontradas: {len(transacciones_faltantes)}")
    logger.info(f"⏱️ Tiempo de procesamiento: {processing_time:.2f} segundos")
    logger.info(f"📄 Archivo de reporte: {archivo_generado if archivo_generado else 'No generado'}")
    log_separator(logger, "=" * 60)
    
    print(f"🏁 Procesamiento completado - {len(transacciones_faltantes)} transacciones encontradas")

if __name__ == "__main__":
    main()
