from conector import create_connection

def main():
    conn = create_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        print("Fecha y hora del servidor:", result)
        conn.close()

if __name__ == "__main__":
    main()
