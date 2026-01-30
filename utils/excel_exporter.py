"""
Módulo de exportación a Excel optimizado
=========================================
Utilidades para exportar DataFrames a Excel de forma eficiente.
"""
import pandas as pd
from pathlib import Path
from typing import Iterator, Optional, List, Dict, Any
from datetime import datetime

import sys
sys.path.insert(0, '..')
from config.settings import EXPORT_CONFIG


class ExcelExporter:
    """
    Exportador de DataFrames a Excel con optimizaciones.
    
    Características:
    - Usa xlsxwriter para mejor rendimiento
    - Soporte para exportación por chunks
    - Formateo automático de columnas
    - Auto-ajuste de anchos de columna
    """
    
    def __init__(
        self, 
        output_path: str,
        sheet_name: str = "Data",
        datetime_format: str = None
    ):
        """
        Inicializa el exportador.
        
        Args:
            output_path: Ruta del archivo Excel a crear
            sheet_name: Nombre de la hoja
            datetime_format: Formato para fechas
        """
        self.output_path = Path(output_path)
        self.sheet_name = sheet_name
        self.datetime_format = datetime_format or EXPORT_CONFIG.datetime_format
        
        # Crear directorio si no existe
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
    
    def export(self, df: pd.DataFrame, auto_width: bool = True) -> str:
        """
        Exporta un DataFrame a Excel.
        
        Args:
            df: DataFrame a exportar
            auto_width: Si ajustar automáticamente el ancho de columnas
            
        Returns:
            Ruta del archivo creado
        """
        # Usar xlsxwriter para mejor rendimiento
        with pd.ExcelWriter(
            self.output_path, 
            engine='xlsxwriter',
            datetime_format=self.datetime_format
        ) as writer:
            df.to_excel(writer, sheet_name=self.sheet_name, index=False)
            
            if auto_width:
                self._adjust_column_widths(writer, df)
        
        print(f"[OK] Exportado: {self.output_path} ({len(df):,} filas)")
        return str(self.output_path)
    
    def export_chunks(
        self, 
        chunks: Iterator[pd.DataFrame],
        auto_width: bool = True
    ) -> str:
        """
        Exporta múltiples chunks de DataFrames a un solo Excel.
        Optimizado para datasets muy grandes.
        
        Args:
            chunks: Iterador de DataFrames
            auto_width: Si ajustar automáticamente el ancho de columnas
            
        Returns:
            Ruta del archivo creado
        """
        first_chunk = True
        total_rows = 0
        sample_df = None
        
        with pd.ExcelWriter(
            self.output_path, 
            engine='xlsxwriter',
            datetime_format=self.datetime_format
        ) as writer:
            workbook = writer.book
            worksheet = workbook.add_worksheet(self.sheet_name)
            
            for chunk_num, df in enumerate(chunks):
                if first_chunk:
                    # Escribir headers
                    for col_num, column in enumerate(df.columns):
                        worksheet.write(0, col_num, column)
                    sample_df = df.head(100)  # Para calcular anchos
                    first_chunk = False
                    start_row = 1
                else:
                    start_row = total_rows + 1
                
                # Escribir datos
                for row_num, row in enumerate(df.itertuples(index=False)):
                    for col_num, value in enumerate(row):
                        worksheet.write(start_row + row_num, col_num, value)
                
                total_rows += len(df)
                print(f"  Procesado chunk {chunk_num + 1}: {len(df):,} filas")
            
            # Ajustar anchos usando sample
            if auto_width and sample_df is not None:
                self._adjust_column_widths_worksheet(worksheet, sample_df)
        
        print(f"[OK] Exportado: {self.output_path} ({total_rows:,} filas totales)")
        return str(self.output_path)
    
    def _adjust_column_widths(self, writer, df: pd.DataFrame) -> None:
        """Ajusta el ancho de columnas basado en el contenido."""
        worksheet = writer.sheets[self.sheet_name]
        
        for idx, col in enumerate(df.columns):
            # Calcular ancho máximo
            max_len = max(
                df[col].astype(str).map(len).max(),
                len(str(col))
            ) + 2  # Padding
            
            # Limitar ancho máximo
            max_len = min(max_len, 50)
            
            worksheet.set_column(idx, idx, max_len)
    
    def _adjust_column_widths_worksheet(self, worksheet, df: pd.DataFrame) -> None:
        """Ajusta el ancho de columnas en un worksheet existente."""
        for idx, col in enumerate(df.columns):
            max_len = max(
                df[col].astype(str).map(len).max(),
                len(str(col))
            ) + 2
            max_len = min(max_len, 50)
            worksheet.set_column(idx, idx, max_len)


def export_dataframe(
    df: pd.DataFrame, 
    output_path: str, 
    sheet_name: str = "Data"
) -> str:
    """
    Función de utilidad para exportar un DataFrame rápidamente.
    
    Args:
        df: DataFrame a exportar
        output_path: Ruta del archivo Excel
        sheet_name: Nombre de la hoja
        
    Returns:
        Ruta del archivo creado
    """
    exporter = ExcelExporter(output_path, sheet_name)
    return exporter.export(df)


def generate_output_filename(report_name: str, extension: str = "xlsx") -> str:
    """
    Genera un nombre de archivo con timestamp.
    
    Args:
        report_name: Nombre base del reporte
        extension: Extensión del archivo
        
    Returns:
        Nombre de archivo con timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{report_name}_{timestamp}.{extension}"
