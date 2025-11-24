"""
Sistema de notificaciones y logging para Prefect Flows.

Proporciona funciones para notificaciones de consola con colores,
logging estructurado en JSON, y preparación para integraciones futuras.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    # Definir fallbacks si colorama no está disponible
    class Fore:
        GREEN = RED = YELLOW = BLUE = CYAN = MAGENTA = WHITE = ""
    class Style:
        BRIGHT = RESET_ALL = ""


class NotificationService:
    """Servicio de notificaciones para flows de Prefect."""

    def __init__(self, log_dir: Optional[str] = None):
        """
        Inicializa el servicio de notificaciones.

        Args:
            log_dir: Directorio para guardar logs JSON (default: logs/prefect_runs)
        """
        if log_dir is None:
            log_dir = os.getenv("OUTPUT_PATH", "/opt/prefect/work-dir/output")
            log_dir = os.path.join(os.path.dirname(log_dir), "logs", "prefect_runs")

        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def log_flow_start(self, flow_name: str, params: Optional[Dict[str, Any]] = None):
        """
        Registra el inicio de un flow.

        Args:
            flow_name: Nombre del flow
            params: Parámetros del flow
        """
        message = f"🚀 Iniciando flow: {flow_name}"
        if params:
            message += f" con parámetros: {params}"

        self._print_info(message)
        self._log_to_file(
            {
                "timestamp": datetime.now().isoformat(),
                "event": "flow_start",
                "flow_name": flow_name,
                "params": params or {},
            }
        )

    def log_flow_success(
        self, flow_name: str, duration_seconds: Optional[float] = None
    ):
        """
        Registra el éxito de un flow.

        Args:
            flow_name: Nombre del flow
            duration_seconds: Duración en segundos
        """
        message = f"✅ Flow completado exitosamente: {flow_name}"
        if duration_seconds:
            message += f" (duración: {duration_seconds:.2f}s)"

        self._print_success(message)
        self._log_to_file(
            {
                "timestamp": datetime.now().isoformat(),
                "event": "flow_success",
                "flow_name": flow_name,
                "duration_seconds": duration_seconds,
            }
        )

    def log_flow_failure(self, flow_name: str, error: Exception):
        """
        Registra el fallo de un flow.

        Args:
            flow_name: Nombre del flow
            error: Excepción ocurrida
        """
        message = f"❌ Flow falló: {flow_name}"
        self._print_error(message)
        self._print_error(f"   Error: {str(error)}")

        self._log_to_file(
            {
                "timestamp": datetime.now().isoformat(),
                "event": "flow_failure",
                "flow_name": flow_name,
                "error": str(error),
                "error_type": type(error).__name__,
            }
        )

    def log_task_start(self, task_name: str):
        """
        Registra el inicio de una task.

        Args:
            task_name: Nombre de la task
        """
        self._print_info(f"   → Ejecutando task: {task_name}")

    def log_task_success(self, task_name: str, result: Optional[str] = None):
        """
        Registra el éxito de una task.

        Args:
            task_name: Nombre de la task
            result: Resultado opcional de la task
        """
        message = f"   ✓ Task completada: {task_name}"
        if result:
            message += f" - {result}"
        self._print_success(message)

    def log_task_failure(self, task_name: str, error: Exception):
        """
        Registra el fallo de una task.

        Args:
            task_name: Nombre de la task
            error: Excepción ocurrida
        """
        self._print_error(f"   ✗ Task falló: {task_name}")
        self._print_error(f"     Error: {str(error)}")

    def log_info(self, message: str):
        """Log de información general."""
        self._print_info(message)

    def log_warning(self, message: str):
        """Log de advertencia."""
        self._print_warning(message)

    def log_error(self, message: str):
        """Log de error."""
        self._print_error(message)

    def _print_success(self, message: str):
        """Imprime mensaje de éxito en verde."""
        if COLORAMA_AVAILABLE:
            print(f"{Fore.GREEN}{message}{Style.RESET_ALL}")
        else:
            print(message)

    def _print_error(self, message: str):
        """Imprime mensaje de error en rojo."""
        if COLORAMA_AVAILABLE:
            print(f"{Fore.RED}{message}{Style.RESET_ALL}")
        else:
            print(message)

    def _print_warning(self, message: str):
        """Imprime mensaje de advertencia en amarillo."""
        if COLORAMA_AVAILABLE:
            print(f"{Fore.YELLOW}{message}{Style.RESET_ALL}")
        else:
            print(message)

    def _print_info(self, message: str):
        """Imprime mensaje de información en azul."""
        if COLORAMA_AVAILABLE:
            print(f"{Fore.CYAN}{message}{Style.RESET_ALL}")
        else:
            print(message)

    def _log_to_file(self, log_entry: Dict[str, Any]):
        """
        Guarda entrada de log en archivo JSON.

        Args:
            log_entry: Diccionario con datos del log
        """
        try:
            # Crear archivo de log por fecha
            date_str = datetime.now().strftime("%Y-%m-%d")
            log_file = self.log_dir / f"prefect_runs_{date_str}.jsonl"

            # Escribir en formato JSONL (JSON Lines)
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"Warning: No se pudo escribir log a archivo: {e}")


# Instancia global del servicio de notificaciones
_notification_service = None


def get_notification_service() -> NotificationService:
    """
    Obtiene la instancia global del servicio de notificaciones.

    Returns:
        Instancia de NotificationService
    """
    global _notification_service
    if _notification_service is None:
        _notification_service = NotificationService()
    return _notification_service


def notify_success(message: str):
    """Notificación rápida de éxito."""
    get_notification_service().log_info(f"✅ {message}")


def notify_error(message: str):
    """Notificación rápida de error."""
    get_notification_service().log_error(f"❌ {message}")


def notify_warning(message: str):
    """Notificación rápida de advertencia."""
    get_notification_service().log_warning(f"⚠️  {message}")


def notify_info(message: str):
    """Notificación rápida de información."""
    get_notification_service().log_info(f"ℹ️  {message}")

