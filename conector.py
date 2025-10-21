import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

load_dotenv()

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_DATABASE"),
            #unix_socket=os.getenv("DB_SOCKET")
        )
        if connection.is_connected():
            print(f" Conexión exitosa a la base de datos {os.getenv('DB_DATABASE')}")
            return connection
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
