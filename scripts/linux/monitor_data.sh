#!/bin/bash
# Script para monitorear nuevos datos en PostgreSQL
# Este script ejecuta el monitor una vez y reporta si hay datos nuevos

set -euo pipefail

# Colores para logs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO] $1${NC}"
}

log_error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

# Cambiar al directorio raíz del proyecto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Verificar que el servicio esté corriendo
if ! docker compose ps prefect-worker | grep -q "Up"; then
    log_error "El servicio prefect-worker no está corriendo"
    log_info "Ejecuta: docker compose up -d"
    exit 1
fi

echo "========================================"
echo "      MONITOR DE NUEVOS DATOS"
echo "========================================"
echo ""

# Ejecutar el flow de monitoreo
docker compose exec -T prefect-worker python -m flows.data_monitor_flow

echo ""
echo "========================================"
echo "Para ejecutar el master flow manualmente:"
echo "  ./scripts/linux/run_prefect_flow.sh master"
echo ""
echo "Para ver detalles en la UI de Prefect:"
echo "  http://localhost:4200"
echo "========================================"

