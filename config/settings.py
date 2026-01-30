"""
Configuración centralizada para Master Data Reports
====================================================
Modifica estos valores según tu entorno.
"""
from dataclasses import dataclass
from typing import List, Optional


# ============================================
# CONFIGURACIÓN DE BASE DE DATOS
# ============================================
@dataclass
class DatabaseConfig:
    """Configuración de conexión a SQL Server"""
    server: str = "USE-IDB048"
    #database: str = "bdcmc_test"
    #username: str = "aura"
    #password: str = "SGSColombia2025*"
    database: str = "bdcmc"
    username: str = "acastiblanco"
    password: str = "acastiblanco"
    driver: str = "ODBC Driver 18 for SQL Server"
    trust_certificate: bool = True
    
    def get_connection_string(self) -> str:
        """Genera la cadena de conexión ODBC"""
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate={'yes' if self.trust_certificate else 'no'};"
        )


# Instancia global de configuración
DB_CONFIG = DatabaseConfig()


# ============================================
# CONSTANTES DE CONVERSIÓN
# ============================================
CHF_RATE: float = 0.000210699  # Tasa de conversión a CHF


# ============================================
# CÓDIGOS DE DOCUMENTO POR CATEGORÍA
# ============================================
@dataclass
class DocumentTypes:
    """Clasificación de tipos de documento"""
    # Facturas y documentos de débito
    invoice_debit: tuple = ('FE', 'FD', 'DE')
    
    # Notas de crédito
    credit_notes: tuple = ('NP', 'NA')
    
    # Documentos con saldo (todos los que afectan balance)
    balance_positive: tuple = ('C2', 'DE', 'FD', 'FE', 'FP', 'LG', 'NO', 'RG')
    balance_negative: tuple = ('M2', 'N2', 'NA', 'NP')
    
    # Órdenes de compra
    purchase_orders: tuple = ('OC', 'OS')
    
    # Contratos/Acuerdos
    agreements: tuple = ('CX',)
    
    # Documentos a excluir (intercompany)
    excluded: tuple = ('EC', 'ER', 'SB', 'SD')
    
    # Todos los documentos transaccionales
    all_transactional: tuple = (
        'C2', 'DE', 'FD', 'FE', 'FP', 'LG', 'NO', 'RG', 
        'M2', 'N2', 'NA', 'NP'
    )


DOC_TYPES = DocumentTypes()


# ============================================
# CÓDIGOS DE ORIGEN VÁLIDOS
# ============================================
VALID_ORIGIN_CODES: tuple = (
    '011', 'F490401', 'F490411', 'F491201', 'F494101', 'F494151'
)


# ============================================
# CÓDIGOS DE PROVEEDOR A EXCLUIR
# ============================================
EXCLUDED_VENDOR_CODES: tuple = (
    '0000000012', '0000000011', 'F491201', 'F490411', 
    'F490411', 'F490401', 'F490421', '2000022410', '2000103488'
)


# ============================================
# CONFIGURACIÓN DE EXPORTACIÓN
# ============================================
@dataclass
class ExportConfig:
    """Configuración para exportación a Excel"""
    chunk_size: int = 5000  # Filas por chunk
    max_rows_per_sheet: int = 1000000  # Límite de Excel
    default_output_dir: str = "./exports"
    datetime_format: str = "%Y-%m-%d"


EXPORT_CONFIG = ExportConfig()
