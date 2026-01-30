"""
Master Data Reports - CLI Entry Point
======================================
Genera reportes de Master Data (Suppliers, Customers, etc.) y exporta a Excel.

Uso:
    python main.py --report supplier_header --output suppliers.xlsx
    python main.py --report supplier_site --output sites.xlsx
    python main.py --report all --output-dir ./exports/
    python main.py --list
    python main.py --test-connection
"""
import argparse
import sys
from pathlib import Path
from typing import Dict, Type

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent))

from reports.base import BaseReport
from reports.suppliers import SupplierHeaderReport, SupplierSiteReport
from utils.database import test_connection
from utils.excel_exporter import generate_output_filename


# ============================================
# REGISTRO DE REPORTES DISPONIBLES
# ============================================
AVAILABLE_REPORTS: Dict[str, Type[BaseReport]] = {
    "supplier_header": SupplierHeaderReport,
    "supplier_site": SupplierSiteReport,
    # Futuros reportes - agregar aquí:
    # "customer_header": CustomerHeaderReport,
    # "customer_site": CustomerSiteReport,
}


def list_reports() -> None:
    """Muestra lista de reportes disponibles."""
    print("\nReportes disponibles:")
    print("-" * 40)
    for name, report_class in AVAILABLE_REPORTS.items():
        report = report_class()
        print(f"  • {name:20} - {report.get_sheet_name()}")
    print()


def generate_report(
    report_name: str,
    output_path: str = None,
    output_dir: str = None,
    limit: int = None
) -> str:
    """
    Genera un reporte específico.
    
    Args:
        report_name: Nombre del reporte (ej: 'supplier_header')
        output_path: Ruta completa del archivo de salida
        output_dir: Directorio de salida (genera nombre automático)
        limit: Límite de filas (para pruebas)
        
    Returns:
        Ruta del archivo generado
    """
    if report_name not in AVAILABLE_REPORTS:
        print(f"[ERROR] Reporte '{report_name}' no encontrado.")
        list_reports()
        sys.exit(1)
    
    report_class = AVAILABLE_REPORTS[report_name]
    report = report_class()
    
    return report.generate(
        output_path=output_path,
        output_dir=output_dir,
        limit=limit
    )


def generate_all_reports(output_dir: str = None, limit: int = None) -> None:
    """Genera todos los reportes disponibles."""
    output_dir = output_dir or "./exports"
    print(f"\nGenerando todos los reportes en: {output_dir}")
    print("=" * 50)
    
    generated = []
    for report_name in AVAILABLE_REPORTS.keys():
        try:
            path = generate_report(
                report_name=report_name,
                output_dir=output_dir,
                limit=limit
            )
            generated.append(path)
        except Exception as e:
            print(f"[ERROR] Error generando {report_name}: {e}")
    
    print("\n" + "=" * 50)
    print(f"[OK] Generados {len(generated)} reportes:")
    for path in generated:
        print(f"   • {path}")


def main():
    """Punto de entrada principal."""
    parser = argparse.ArgumentParser(
        description="Master Data Reports - Generador de Excel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python main.py --list
  python main.py --test-connection
  python main.py --report supplier_header
  python main.py --report supplier_site --output sites.xlsx
  python main.py --report all --output-dir ./exports
  python main.py --report supplier_header --limit 10
        """
    )
    
    parser.add_argument(
        "--report", "-r",
        type=str,
        help="Nombre del reporte a generar (o 'all' para todos)"
    )
    
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Ruta del archivo Excel de salida"
    )
    
    parser.add_argument(
        "--output-dir", "-d",
        type=str,
        default="./exports",
        help="Directorio de salida (default: ./exports)"
    )
    
    parser.add_argument(
        "--limit", "-l",
        type=int,
        help="Límite de filas (para pruebas)"
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="Lista los reportes disponibles"
    )
    
    parser.add_argument(
        "--test-connection", "-t",
        action="store_true",
        help="Prueba la conexión a la base de datos"
    )
    
    args = parser.parse_args()
    
    # Acciones
    if args.list:
        list_reports()
        return
    
    if args.test_connection:
        print("\nProbando conexion a SQL Server...")
        if test_connection():
            print("[OK] Conexion exitosa!")
        else:
            print("[ERROR] Conexion fallida. Verifica config/settings.py")
            sys.exit(1)
        return
    
    if not args.report:
        parser.print_help()
        return
    
    # Generar reportes
    if args.report.lower() == "all":
        generate_all_reports(
            output_dir=args.output_dir,
            limit=args.limit
        )
    else:
        generate_report(
            report_name=args.report,
            output_path=args.output,
            output_dir=args.output_dir,
            limit=args.limit
        )


if __name__ == "__main__":
    main()
