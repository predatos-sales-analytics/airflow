#!/bin/bash
# Script helper para ejecutar pipelines dentro del contenedor Docker (Linux)

# Cambiar al directorio raiz del proyecto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Colores para logs
BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO] $1${NC}"
}

log_success() {
    echo -e "${GREEN}[OK] $1${NC}"
}

log_error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

if [ -z "$1" ]; then
    echo ""
    echo "================================================================================"
    echo "EJECUTAR PIPELINES DE PROCESAMIENTO"
    echo "================================================================================"
    echo ""
    echo "Uso: ./scripts/linux/run_pipeline_docker.sh <pipeline_name> [opciones]"
    echo ""
    echo "Pipelines disponibles:"
    echo "  - executive_summary  - Resumen ejecutivo (diario)"
    echo "  - analytics          - Analisis analitico (semanal)"
    echo "  - clustering         - Clustering de clientes (mensual)"
    echo "  - recommendations    - Recomendaciones de productos (mensual)"
    echo "  - all                - Ejecutar todos los pipelines"
    echo ""
    echo "Ejemplos:"
    echo "  ./scripts/linux/run_pipeline_docker.sh executive_summary"
    echo "  ./scripts/linux/run_pipeline_docker.sh clustering --n-clusters 5"
    echo "  ./scripts/linux/run_pipeline_docker.sh recommendations --min-support 0.02"
    echo ""
    exit 1
fi

PIPELINE_NAME="$1"

# Verificar que docker compose este disponible
if ! command -v docker &> /dev/null; then
    log_error "docker no se encuentra en el PATH"
    echo "Asegurate de tener Docker Desktop instalado y en ejecucion"
    exit 1
fi

# Verificar que el contenedor spark-client este corriendo
if ! docker compose ps spark-client | grep -q "Up"; then
    log_error "El contenedor spark-client no esta corriendo"
    echo "Ejecuta: docker compose up -d"
    exit 1
fi

echo ""
echo "================================================================================"
log_info "Ejecutando pipeline: ${PIPELINE_NAME}"
echo "================================================================================"
echo ""

# Ejecutar el pipeline con todos los argumentos
if docker compose exec spark-client python3 /opt/spark/work-dir/src/run_pipeline.py "$@"; then
    echo ""
    echo "================================================================================"
    log_success "Pipeline '${PIPELINE_NAME}' completado exitosamente"
    echo "================================================================================"
    exit 0
else
    echo ""
    echo "================================================================================"
    log_error "Pipeline '${PIPELINE_NAME}' fallo"
    echo "================================================================================"
    exit 1
fi
