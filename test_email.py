#!/usr/bin/env python3
"""
Script para probar el envío de emails con SendGrid
"""

import os
from dotenv import load_dotenv
from email_sender import EmailSender

load_dotenv()

def test_email():
    """
    Prueba el envío de email con SendGrid
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
    
    # Datos de prueba
    to_email = os.getenv("NOTIFICATION_EMAIL")
    subject = "🧪 Prueba de Email - Sistema Serfinsa"
    
    # Crear cuerpo de email de prueba
    body = """
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .header { background-color: #f4f4f4; padding: 15px; border-radius: 5px; }
            .success { color: #28a745; }
        </style>
    </head>
    <body>
        <div class="header">
            <h2>🧪 Prueba de Email - Sistema Serfinsa</h2>
            <p>Este es un email de prueba para verificar que el sistema de notificaciones funciona correctamente.</p>
        </div>
        
        <div>
            <h3>✅ Configuración SMTP Verificada</h3>
            <ul>
                <li><strong>Host:</strong> {}</li>
                <li><strong>Puerto:</strong> {}</li>
                <li><strong>Usuario:</strong> {}</li>
                <li><strong>Remitente:</strong> {}</li>
                <li><strong>Nombre:</strong> {}</li>
                <li><strong>Encriptación:</strong> {}</li>
            </ul>
        </div>
        
        <div>
            <h4>📎 Archivos adjuntos:</h4>
            <ul>
                <li>📊 <strong>Archivo Excel:</strong> Datos de ejemplo (si está disponible)</li>
                <li>📝 <strong>Log del sistema:</strong> Detalles del procesamiento</li>
            </ul>
        </div>
        
        <div style="margin-top: 20px; padding: 10px; background-color: #d4edda; border-radius: 5px;">
            <p class="success"><strong>🎉 ¡Email enviado exitosamente!</strong></p>
            <p>El sistema de notificaciones está funcionando correctamente.</p>
        </div>
    </body>
    </html>
    """.format(
        os.getenv("MAIL_HOST"),
        os.getenv("MAIL_PORT"),
        os.getenv("MAIL_SENDGRID_USER"),
        os.getenv("MAIL_FROM_ADDRESS"),
        os.getenv("MAIL_FROM_NAME"),
        os.getenv("MAIL_ENCRYPTION")
    )
    
    print(f"📧 Enviando email de prueba a: {to_email}")
    print("⏳ Por favor espera...")
    
    # Buscar un archivo Excel para adjuntar como ejemplo
    excel_file_path = None
    if os.path.exists("data/serfinsa"):
        excel_files = [f for f in os.listdir("data/serfinsa") if f.endswith('.xlsx')]
        if excel_files:
            excel_file_path = os.path.join("data/serfinsa", excel_files[0])
    
    # Enviar email
    success, message = email_sender.send_notification_email(to_email, subject, body, excel_file_path=excel_file_path)
    
    if success:
        print("✅ ¡Email enviado exitosamente!")
        print(f"📬 Revisa tu bandeja de entrada en: {to_email}")
        return True
    else:
        print(f"❌ Error enviando email: {message}")
        return False

if __name__ == "__main__":
    print("🚀 Iniciando prueba de email...")
    test_email()
