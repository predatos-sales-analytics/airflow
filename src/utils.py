"""
Utilidades comunes para el pipeline.

Este módulo contiene funciones auxiliares para formateo,
manejo de archivos y logging del pipeline.
"""

from typing import Dict, Any
import json
from datetime import datetime


def format_number(num: int) -> str:
    """
    Formatea un número con separadores de miles.

    Args:
        num: Número a formatear

    Returns:
        String con número formateado
    """
    return f"{num:,}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """
    Formatea un valor como porcentaje.

    Args:
        value: Valor a formatear
        decimals: Número de decimales

    Returns:
        String con porcentaje formateado
    """
    return f"{value:.{decimals}f}%"


def print_section_header(title: str, char: str = "=") -> None:
    """
    Imprime un encabezado de sección formateado.

    Args:
        title: Título de la sección
        char: Carácter para la línea
    """
    width = 80
    print(f"\n{char * width}")
    print(title)
    print(f"{char * width}\n")


def print_subsection_header(title: str) -> None:
    """
    Imprime un encabezado de subsección formateado.

    Args:
        title: Título de la subsección
    """
    print(f"\n{title}")
    print("-" * 60)


def save_json(data: Dict[str, Any], file_path: str) -> None:
    """
    Guarda datos en formato JSON.

    Args:
        data: Datos a guardar
        file_path: Ruta del archivo
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_json(file_path: str) -> Dict[str, Any]:
    """
    Carga datos desde un archivo JSON.

    Args:
        file_path: Ruta del archivo

    Returns:
        Diccionario con los datos
    """
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_timestamp() -> str:
    """
    Obtiene timestamp actual formateado.

    Returns:
        String con timestamp
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def print_success(message: str) -> None:
    """Imprime mensaje de éxito."""
    print(f"[OK] {message}")


def print_error(message: str) -> None:
    """Imprime mensaje de error."""
    print(f"[ERROR] {message}")


def print_warning(message: str) -> None:
    """Imprime mensaje de advertencia."""
    print(f"[ADVERTENCIA] {message}")


def print_info(message: str) -> None:
    """Imprime mensaje informativo."""
    print(f"[INFO] {message}")

