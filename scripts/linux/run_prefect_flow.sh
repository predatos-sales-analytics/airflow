#!/bin/bash
# Script para ejecutar flows de Prefect

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

log_success() {
    echo -e "${GREEN}[OK] $1${NC}"
}

log_error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

# Cambiar al directorio raíz del proyecto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Verificar argumento
if [ $# -eq 0 ]; then
    log_error "Debes especificar el nombre del flow"
    echo ""
    echo "Uso: $0 <flow_name> [options]"
    echo ""
    echo "Flows disponibles:"
    echo "  data_loading      - Cargar datos CSV a PostgreSQL"
    echo "  master            - Ejecutar todos los pipelines"
    echo "  executive_summary - Pipeline de resumen ejecutivo"
    echo "  analytics         - Pipeline de análisis temporal"
    echo "  clustering        - Pipeline de clustering de clientes"
    echo "  recommendations   - Pipeline de recomendaciones"
    echo "  output_sync       - Sincronizar outputs al frontend"
    echo ""
    echo "Ejemplos:"
    echo "  $0 data_loading"
    echo "  $0 master"
    echo "  $0 clustering --n-clusters 5"
    echo ""
    exit 1
fi

FLOW_NAME=$1
shift  # Remover el primer argumento, el resto son opciones

# Mapear nombre de flow a archivo Python
case $FLOW_NAME in
    data_loading)
        FLOW_MODULE="flows.data_loading_flow"
        ;;
    master)
        FLOW_MODULE="flows.master_flow"
        ;;
    executive_summary)
        FLOW_MODULE="flows.executive_summary_flow"
        ;;
    analytics)
        FLOW_MODULE="flows.analytics_flow"
        ;;
    clustering)
        FLOW_MODULE="flows.clustering_flow"
        ;;
    recommendations)
        FLOW_MODULE="flows.recommendations_flow"
        ;;
    output_sync)
        FLOW_MODULE="flows.output_sync_flow"
        ;;
    *)
        log_error "Flow desconocido: $FLOW_NAME"
        exit 1
        ;;
esac

# Verificar que el servicio esté corriendo
if ! docker compose ps prefect-worker | grep -q "Up"; then
    log_error "El servicio prefect-worker no está corriendo"
    log_info "Ejecuta: docker compose up -d"
    exit 1
fi

log_info "Ejecutando flow: $FLOW_NAME"
echo ""

# Ejecutar el flow en el worker
docker compose exec -T prefect-worker python -m "$FLOW_MODULE" "$@"

echo ""
log_success "Flow ejecutado. Revisa la UI de Prefect para más detalles: http://localhost:4200"

