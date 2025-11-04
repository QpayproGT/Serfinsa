#!/usr/bin/env python3
"""
Script para probar el envío de emails de alerta cuando no se encuentra archivo
"""

import os
from dotenv import load_dotenv
from email_sender import EmailSender

load_dotenv()

def test_alert_email():
    """
    Prueba el envío de email de alerta cuando no se encuentra archivo
    """
    # Verificar que las variables de entorno estén configuradas
    required_vars = ['MAIL_HOST', 'MAIL_SENDGRID_USER', 'MAIL_SENDGRID_PDW', 'MAIL_FROM_ADDRESS', 'NOTIFICATION_EMAIL']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Faltan las siguientes variables de entorno: {', '.join(missing_vars)}")
        print("Por favor, configura estas variables en tu archivo .env")
        return False
    
    # Crear instancia del email sender
    email_sender = EmailSender()
    
    # Datos de prueba para alerta
    to_email = os.getenv("NOTIFICATION_EMAIL")
    subject = "Prueba - No se encontró archivo Excel para procesar"
    alert_message = "No se encontró ningún archivo Excel para procesar."
    search_path = "/var/www/vhosts/serfinsa.qpaypro.com/data"
    
    print(f"📧 Enviando email de alerta de prueba a: {to_email}")
    print("⏳ Por favor espera...")
    
    # Enviar email de alerta
    success, message = email_sender.send_alert_email(to_email, subject, alert_message, search_path)
    
    if success:
        print("✅ ¡Email de alerta enviado exitosamente!")
        print(f"📬 Revisa tu bandeja de entrada en: {to_email}")
        return True
    else:
        print(f"❌ Error enviando email de alerta: {message}")
        return False

if __name__ == "__main__":
    print("🚨 Iniciando prueba de email de alerta...")
    test_alert_email()
