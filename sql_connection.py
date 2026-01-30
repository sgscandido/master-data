"""
Script de conexión a SQL Server
================================
Configura los parámetros de conexión y ejecuta el script.
"""

import pyodbc

# ============================================
# CONFIGURACIÓN - MODIFICA ESTOS VALORES
# ============================================
SERVER = "USE-IDB048"      # Ej: "192.168.1.100" o "servidor\\instancia"
DATABASE = "bdcmc_test"  # Ej: "SYSCXP"
USERNAME = "aura"         # Ej: "sa" o usuario de dominio
PASSWORD = "SGSColombia2025*"        # Tu contraseña

# Driver disponible en tu PC (usa uno de estos):
# - "ODBC Driver 18 for SQL Server"  (recomendado, más nuevo)
# - "SQL Server"                      (legacy, siempre disponible)
DRIVER = "ODBC Driver 18 for SQL Server"

# ============================================
# CONEXIÓN
# ============================================
def conectar():
    """Establece conexión a SQL Server"""
    try:
        # Cadena de conexión
        conn_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={USERNAME};"
            f"PWD={PASSWORD};"
            f"TrustServerCertificate=yes;"  # Para SSL/TLS
        )
        
        conexion = pyodbc.connect(conn_str)
        print("[OK] Conexion exitosa a SQL Server!")
        return conexion
    
    except pyodbc.Error as e:
        print(f"[ERROR] Error de conexion: {e}")
        return None

# ============================================
# EJEMPLO: EJECUTAR CONSULTA
# ============================================
def ejecutar_consulta(conexion, query):
    """Ejecuta una consulta y retorna los resultados"""
    cursor = conexion.cursor()
    cursor.execute(query)
    
    # Obtener nombres de columnas
    columnas = [column[0] for column in cursor.description]
    
    # Obtener filas
    filas = cursor.fetchall()
    
    return columnas, filas

# ============================================
# EJEMPLO DE USO
# ============================================
if __name__ == "__main__":
    # Conectar
    conn = conectar()
    
    if conn:
        # Ejemplo: consulta simple
        query = """
        SELECT TOP 10 
            CiaCod, 
            DocTipCod, 
            DocNum,
            DocSld
        FROM DocCab
        WHERE DocEst >= 4
        ORDER BY DocNum DESC
        """
        
        try:
            columnas, filas = ejecutar_consulta(conn, query)
            
            # Mostrar resultados
            print("\nResultados:")
            print("-" * 60)
            print(" | ".join(columnas))
            print("-" * 60)
            
            for fila in filas:
                print(" | ".join(str(v) for v in fila))
                
        except pyodbc.Error as e:
            print(f"[ERROR] Error en consulta: {e}")
        
        finally:
            conn.close()
            print("\nConexion cerrada")
