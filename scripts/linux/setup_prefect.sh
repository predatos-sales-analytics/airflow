#!/bin/bash
# Script para configurar Prefect Server y registrar flows

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

log_warning() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

log_error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

log_step() {
    echo ""
    echo -e "${BLUE}================================================================================"
    echo -e "${BLUE}$1"
    echo -e "${BLUE}================================================================================"
}

# Cambiar al directorio raíz del proyecto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

log_step "CONFIGURACIÓN DE PREFECT"

# Verificar que docker compose esté disponible
if ! command -v docker &> /dev/null; then
    log_error "docker no se encuentra en el PATH"
    log_info "Asegúrate de tener Docker Desktop instalado y en ejecución"
    exit 1
fi

# Verificar que los servicios estén corriendo
log_info "Verificando servicios de Docker..."
if ! docker compose ps prefect-server | grep -q "Up"; then
    log_error "El servicio prefect-server no está corriendo"
    log_info "Ejecuta: docker compose up -d"
    exit 1
fi

log_success "Servicios de Docker activos"

# Esperar a que Prefect Server esté listo
log_info "Esperando a que Prefect Server esté listo..."
for i in {1..30}; do
    if docker compose exec -T prefect-server curl -sf http://localhost:4200/api/health > /dev/null 2>&1; then
        log_success "Prefect Server está listo"
        break
    fi
    
    if [ $i -eq 30 ]; then
        log_error "Timeout esperando a Prefect Server"
        exit 1
    fi
    
    sleep 2
done

# Crear work pool si no existe
log_info "Creando work pool 'default-pool'..."
docker compose exec -T prefect-worker bash -c "prefect work-pool create default-pool --type process 2>/dev/null || true"
log_success "Work pool configurado"

# Crear deployment del monitor de datos
echo ""
log_info "Configurando deployment automático del monitor de datos..."
if docker compose exec -T prefect-worker python src/deployments/monitor_deployment.py; then
    log_success "Deployment del monitor configurado (ejecutará cada 2 minutos)"
else
    log_warning "No se pudo crear el deployment automático"
    log_info "Puedes ejecutar el monitor manualmente: ./scripts/linux/monitor_data.sh"
fi

# Información de acceso
log_step "CONFIGURACIÓN COMPLETADA"
echo ""
echo -e "${GREEN}✅ Prefect está configurado y listo para usar${NC}"
echo ""
echo "Acceso a la interfaz web:"
echo "  URL: http://localhost:4200"
echo ""
echo "Monitor de datos:"
echo "  - Configurado para ejecutarse automáticamente cada 2 minutos"
echo "  - Gestiona desde la UI: http://localhost:4200/deployments"
echo "  - Ejecución manual: ./scripts/linux/monitor_data.sh"
echo ""
echo "Para ejecutar flows, usa los scripts en scripts/linux/:"
echo "  ./scripts/linux/run_prefect_flow.sh data_loading"
echo "  ./scripts/linux/run_prefect_flow.sh master"
echo "  ./scripts/linux/run_prefect_flow.sh executive_summary"
echo "  ./scripts/linux/run_prefect_flow.sh analytics"
echo "  ./scripts/linux/run_prefect_flow.sh clustering"
echo "  ./scripts/linux/run_prefect_flow.sh recommendations"
echo "  ./scripts/linux/run_prefect_flow.sh output_sync"
echo ""

