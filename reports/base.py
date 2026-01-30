"""
Clase base para todos los reportes de Master Data
==================================================
Define la interfaz común para reportes de suppliers, customers, etc.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd

import sys
sys.path.insert(0, '..')
from utils.database import DatabaseConnection
from utils.excel_exporter import ExcelExporter, generate_output_filename


class BaseReport(ABC):
    """
    Clase base abstracta para reportes de Master Data.
    
    Implementaciones concretas deben definir:
    - get_query(): SQL query a ejecutar
    - get_report_name(): Nombre del reporte para archivos
    
    Opcionalmente pueden sobrescribir:
    - transform(): Transformaciones post-query
    - get_column_mapping(): Renombrar columnas
    """
    
    def __init__(self, db_connection: DatabaseConnection = None):
        """
        Inicializa el reporte.
        
        Args:
            db_connection: Conexión a la BD opcional. Si no se provee,
                          se crea una nueva al ejecutar.
        """
        self._db = db_connection
        self._owns_connection = db_connection is None
    
    @abstractmethod
    def get_query(self) -> str:
        """
        Retorna el SQL query para el reporte.
        
        Returns:
            String con el query SQL
        """
        pass
    
    @abstractmethod
    def get_report_name(self) -> str:
        """
        Retorna el nombre base del reporte.
        
        Returns:
            Nombre del reporte (ej: 'supplier_header')
        """
        pass
    
    def get_sheet_name(self) -> str:
        """
        Retorna el nombre de la hoja en Excel.
        Por defecto usa el nombre del reporte.
        """
        return self.get_report_name().replace('_', ' ').title()
    
    def get_column_mapping(self) -> Optional[Dict[str, str]]:
        """
        Retorna un diccionario para renombrar columnas.
        Si retorna None, no se renombran columnas.
        
        Returns:
            Dict {nombre_original: nombre_nuevo} o None
        """
        return None
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformaciones al DataFrame después de la query.
        Por defecto no hace nada. Sobrescribir si se necesitan
        transformaciones específicas.
        
        Args:
            df: DataFrame con datos crudos del SQL
            
        Returns:
            DataFrame transformado
        """
        return df
    
    def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica el mapeo de columnas si está definido."""
        mapping = self.get_column_mapping()
        if mapping:
            df = df.rename(columns=mapping)
        return df
    
    def execute(self, limit: int = None) -> pd.DataFrame:
        """
        Ejecuta el reporte y retorna un DataFrame.
        
        Args:
            limit: Límite opcional de filas (para pruebas)
            
        Returns:
            DataFrame con los datos del reporte
        """
        query = self.get_query()
        
        # Agregar LIMIT si se especifica (para pruebas)
        if limit:
            # Modificar query para agregar TOP
            if 'SELECT' in query.upper():
                query = query.replace('SELECT', f'SELECT TOP {limit}', 1)
        
        # Usar conexión existente o crear nueva
        if self._db and self._db.is_connected:
            df = self._db.execute_query(query)
        else:
            with DatabaseConnection() as db:
                df = db.execute_query(query)
        
        # Aplicar transformaciones
        df = self.transform(df)
        df = self._apply_column_mapping(df)
        
        return df
    
    def generate(
        self, 
        output_path: str = None,
        output_dir: str = None,
        limit: int = None
    ) -> str:
        """
        Genera el reporte completo y lo exporta a Excel.
        
        Args:
            output_path: Ruta completa del archivo (opcional)
            output_dir: Directorio de salida (opcional, genera nombre automático)
            limit: Límite de filas (opcional, para pruebas)
            
        Returns:
            Ruta del archivo Excel generado
        """
        print(f"\n{'='*50}")
        print(f"Generando reporte: {self.get_report_name()}")
        print(f"{'='*50}")
        
        # Determinar ruta de salida
        if output_path:
            final_path = output_path
        else:
            if output_dir is None:
                output_dir = "./exports"
            filename = generate_output_filename(self.get_report_name())
            final_path = str(Path(output_dir) / filename)
        
        # Ejecutar query
        print("Ejecutando query...")
        df = self.execute(limit=limit)
        print(f"  Registros obtenidos: {len(df):,}")
        
        # Exportar
        print("Exportando a Excel...")
        exporter = ExcelExporter(final_path, self.get_sheet_name())
        result_path = exporter.export(df)
        
        print(f"\n✓ Reporte generado exitosamente!")
        return result_path
    
    def preview(self, n: int = 10) -> pd.DataFrame:
        """
        Obtiene una vista previa del reporte (primeras N filas).
        Útil para verificar que el query funciona.
        
        Args:
            n: Número de filas a mostrar
            
        Returns:
            DataFrame con las primeras N filas
        """
        return self.execute(limit=n)
