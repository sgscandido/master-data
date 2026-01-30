"""
Módulo de conexión a base de datos SQL Server
==============================================
Context manager para conexiones y utilidades de query.
"""
import pyodbc
import pandas as pd
from typing import Optional, Iterator, List
from contextlib import contextmanager

import sys
sys.path.insert(0, '..')
from config.settings import DB_CONFIG, EXPORT_CONFIG


class DatabaseConnection:
    """
    Manejador de conexión a SQL Server con soporte para queries por chunks.
    
    Uso:
        with DatabaseConnection() as db:
            df = db.execute_query("SELECT * FROM tabla")
    """
    
    def __init__(self, config=None):
        """
        Inicializa el manejador de conexión.
        
        Args:
            config: DatabaseConfig opcional. Usa DB_CONFIG por defecto.
        """
        self.config = config or DB_CONFIG
        self._connection: Optional[pyodbc.Connection] = None
    
    def __enter__(self):
        """Abre la conexión al entrar en el context manager."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cierra la conexión al salir del context manager."""
        self.close()
        return False  # No suprimimos excepciones
    
    def connect(self) -> None:
        """Establece la conexión a la base de datos."""
        try:
            conn_str = self.config.get_connection_string()
            self._connection = pyodbc.connect(conn_str)
            print("[OK] Conexión exitosa a SQL Server")
        except pyodbc.Error as e:
            print(f"[ERROR] Error de conexión: {e}")
            raise
    
    def close(self) -> None:
        """Cierra la conexión si está abierta."""
        if self._connection:
            self._connection.close()
            self._connection = None
            print("[OK] Conexión cerrada")
    
    @property
    def is_connected(self) -> bool:
        """Verifica si hay una conexión activa."""
        return self._connection is not None
    
    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Ejecuta una consulta y retorna un DataFrame.
        
        Args:
            query: Consulta SQL a ejecutar
            params: Parámetros opcionales para la consulta
            
        Returns:
            DataFrame con los resultados
        """
        if not self._connection:
            raise RuntimeError("No hay conexión activa. Use 'with DatabaseConnection() as db:'")
        
        return pd.read_sql(query, self._connection, params=params)
    
    def execute_query_chunks(
        self, 
        query: str, 
        chunk_size: int = None
    ) -> Iterator[pd.DataFrame]:
        """
        Ejecuta una consulta y retorna un iterador de DataFrames en chunks.
        Útil para datasets muy grandes para no cargar todo en memoria.
        
        Args:
            query: Consulta SQL a ejecutar
            chunk_size: Tamaño de cada chunk. Usa EXPORT_CONFIG.chunk_size por defecto.
            
        Yields:
            DataFrames con chunk_size filas cada uno
        """
        if not self._connection:
            raise RuntimeError("No hay conexión activa. Use 'with DatabaseConnection() as db:'")
        
        chunk_size = chunk_size or EXPORT_CONFIG.chunk_size
        
        return pd.read_sql(query, self._connection, chunksize=chunk_size)
    
    def test_connection(self) -> bool:
        """
        Prueba la conexión ejecutando una consulta simple.
        
        Returns:
            True si la conexión funciona, False en caso contrario
        """
        try:
            with self:
                result = self.execute_query("SELECT 1 AS test")
                return len(result) == 1
        except Exception as e:
            print(f"[ERROR] Test de conexión fallido: {e}")
            return False


def test_connection() -> bool:
    """Función de utilidad para probar la conexión rápidamente."""
    db = DatabaseConnection()
    return db.test_connection()


if __name__ == "__main__":
    # Test rápido de conexión
    print("Probando conexión a SQL Server...")
    if test_connection():
        print("✓ Conexión exitosa!")
    else:
        print("✗ Conexión fallida. Verifica las credenciales en config/settings.py")
