import smtplib
import os
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
import logging

load_dotenv()

class EmailSender:
    def __init__(self):
        self.smtp_host = os.getenv("MAIL_HOST", "smtp.sendgrid.net")
        self.smtp_port = int(os.getenv("MAIL_PORT", "587"))
        self.username = os.getenv("MAIL_SENDGRID_USER", "apikey")
        self.password = os.getenv("MAIL_SENDGRID_PDW")
        self.encryption = os.getenv("MAIL_ENCRYPTION", "tls")
        self.from_address = os.getenv("MAIL_FROM_ADDRESS", "no-reply@qpaypro.com")
        self.from_name = os.getenv("MAIL_FROM_NAME", "Serfinsa System")
        
    def send_notification_email(self, to_email, subject, body, log_file_path=None, excel_file_path=None):
        """
        Envía un email de notificación con el resumen del procesamiento
        """
        try:
            # Crear mensaje
            msg = MIMEMultipart()
            
            # Usar la configuración de SendGrid
            from_email = f"{self.from_name} <{self.from_address}>"
            msg['From'] = from_email
            msg['To'] = to_email
            msg['Subject'] = subject
            
            # Agregar cuerpo del mensaje
            msg.attach(MIMEText(body, 'html'))
            
            # Agregar archivo de log como adjunto si existe
            if log_file_path and os.path.exists(log_file_path):
                with open(log_file_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(log_file_path)}'
                    )
                    msg.attach(part)
            
            # Agregar archivo Excel como adjunto si existe
            if excel_file_path and os.path.exists(excel_file_path):
                with open(excel_file_path, "rb") as attachment:
                    part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= {os.path.basename(excel_file_path)}'
                    )
                    msg.attach(part)
            
            # Conectar al servidor SMTP
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.starttls()  # Habilitar encriptación TLS
            server.login(self.username, self.password)
            
            # Enviar email
            text = msg.as_string()
            server.sendmail(self.from_address, to_email, text)
            server.quit()
            
            return True, "Email enviado exitosamente"
            
        except Exception as e:
            return False, f"Error enviando email: {str(e)}"
    
    def send_alert_email(self, to_email, subject, alert_message, search_path=None):
        """
        Envía un email de alerta cuando no se encuentra el archivo Excel
        """
        try:
            # Crear mensaje
            msg = MIMEMultipart()
            
            # Usar la configuración de SendGrid
            from_email = f"{self.from_name} <{self.from_address}>"
            msg['From'] = from_email
            msg['To'] = to_email
            msg['Subject'] = subject
            
            # Crear cuerpo HTML del email de alerta
            body = self.create_alert_email_body(alert_message, search_path)
            msg.attach(MIMEText(body, 'html'))
            
            # Conectar al servidor SMTP
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.starttls()
            server.login(self.username, self.password)
            
            # Enviar email
            text = msg.as_string()
            server.sendmail(self.from_address, to_email, text)
            server.quit()
            
            return True, "Email de alerta enviado exitosamente"
            
        except Exception as e:
            return False, f"Error enviando email de alerta: {str(e)}"
    
    def create_alert_email_body(self, alert_message, search_path=None):
        """
        Crea el cuerpo HTML del email de alerta
        """
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #dc3545; color: white; padding: 15px; border-radius: 5px; }}
                .message {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 10px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>Incidencia en el sistema de liquidaciones de Serfinsa</h2>
                <p><strong>Fecha y hora:</strong> {current_time}</p>
            </div>
            
            <div class="message">
                <h3>⚠️ Archivo no encontrado</h3>
                <p>No se encontró ningún archivo Excel (.xlsx) para procesar.</p>
                <p>El archivo no fue cargado, por lo que la carga no fue realizada.</p>
            </div>
        </body>
        </html>
        """
        return html_body
    
    def create_email_body(self, excel_file, summary_stats, processing_time):
        """
        Crea el cuerpo HTML del email con el resumen del procesamiento
        """
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f4f4f4; padding: 15px; border-radius: 5px; }}
                .success {{ color: #28a745; }}
                .warning {{ color: #ffc107; }}
                .error {{ color: #dc3545; }}
                .info {{ color: #17a2b8; }}
                .stats {{ background-color: #e9ecef; padding: 10px; border-radius: 5px; margin: 10px 0; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚀 Reporte de Procesamiento - Serfinsa</h2>
                <p><strong>Archivo procesado:</strong> {excel_file}</p>
                <p><strong>Tiempo de procesamiento:</strong> {processing_time}</p>
            </div>
            
            <div class="stats">
                <h3>📊 Resumen de Estadísticas</h3>
                <table>
                    <tr>
                        <th>Métrica</th>
                        <th>Valor</th>
                        <th>Estado</th>
                    </tr>
                    <tr>
                        <td>Registros insertados</td>
                        <td>{summary_stats['inserted']}</td>
                        <td><span class="success">✅ Exitoso</span></td>
                    </tr>
                    <tr>
                        <td>Registros omitidos (duplicados)</td>
                        <td>{summary_stats['skipped']}</td>
                        <td><span class="warning">⚠️ Duplicado</span></td>
                    </tr>
                    <tr>
                        <td>Errores durante inserción</td>
                        <td>{summary_stats['errors']}</td>
                        <td><span class="error">❌ Error</span></td>
                    </tr>
                    <tr>
                        <td>Transaction IDs encontrados</td>
                        <td>{summary_stats['transactions_found']}</td>
                        <td><span class="info">🔍 Procesado</span></td>
                    </tr>
                    <tr>
                        <td>Total de registros procesados</td>
                        <td>{summary_stats['total_processed']}</td>
                        <td><span class="info">📝 Procesado</span></td>
                    </tr>
                </table>
            </div>
            
            <div>
                
                <h4>📎 Archivos adjuntos:</h4>
                <ul>
                    <li>📊 <strong>Archivo Excel original:</strong> Datos procesados</li>
                    <li>📝 <strong>Archivo de log:</strong> Detalles completos del procesamiento</li>
                </ul>
            </div>
            
            <div style="margin-top: 20px; padding: 10px; background-color: #f8f9fa; border-radius: 5px;">
                <p><strong>Nota:</strong> Este es un mensaje automático generado por el sistema de procesamiento de Serfinsa.</p>
            </div>
        </body>
        </html>
        """
        return html_body
