#! /bin/bash
# Script helper para cargar datos en la base de datos PostgreSQL

# Cambiar al directorio raiz del proyecto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Configuracion
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-sales}"
PGUSER="${PGUSER:-sales}"
PGPASSWORD="${PGPASSWORD:-sales}"
TRANSACTION_FILE="${1:-}"

export PGPASSWORD

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

# Si se ejecuta desde dentro del contenedor, usar psql directo
# Si se ejecuta desde fuera, usar docker compose
if [ -f /.dockerenv ]; then
    PGHOST="${PGHOST:-postgres}"
    DATA_DIR="${DATA_DIR:-data}"
    psql_cmd() {
        psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE}" "$@"
    }
else
    log_info "Ejecutando desde fuera del contenedor, usando docker compose..."
    
    # Verificar que docker compose este disponible
    if ! command -v docker &> /dev/null; then
        log_error "docker no se encuentra en el PATH"
        log_info "Asegurate de tener Docker Desktop instalado y en ejecucion"
        exit 1
    fi
    
    # Verificar que el contenedor de postgres este corriendo
    if ! docker compose ps postgres | grep -q "Up"; then
        log_error "El contenedor de postgres no esta corriendo"
        log_info "Ejecuta: docker compose up -d postgres"
        exit 1
    fi
    
    # El directorio data esta montado en /data dentro del contenedor
    DATA_DIR="/data"
    psql_cmd() {
        docker compose exec -T postgres psql -U "${PGUSER}" -d "${PGDATABASE}" "$@"
    }
fi

log_step "PREPARACION DE BASE DE DATOS"

log_info "Creando tablas si no existen..."
psql_cmd <<'EOSQL' > /dev/null 2>&1
CREATE TABLE IF NOT EXISTS categories (
    category_id INT PRIMARY KEY,
    category_name TEXT
);

CREATE TABLE IF NOT EXISTS product_categories (
    product_id INT,
    category_id INT
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_date DATE,
    store_id INT,
    customer_id INT,
    products TEXT
);
EOSQL
log_success "Tablas creadas/verificadas"

log_info "Limpiando datos existentes..."
psql_cmd -c "TRUNCATE TABLE categories;" > /dev/null 2>&1
psql_cmd -c "TRUNCATE TABLE product_categories;" > /dev/null 2>&1
psql_cmd -c "TRUNCATE TABLE transactions;" > /dev/null 2>&1
log_success "Tablas limpiadas"

log_step "CARGA DE DATOS DE REFERENCIA"

log_info "Cargando categorias desde ${DATA_DIR}/products/Categories.csv..."
if psql_cmd -c "\copy categories(category_id, category_name) FROM '${DATA_DIR}/products/Categories.csv' WITH (FORMAT csv, DELIMITER '|');" > /dev/null 2>&1; then
    COUNT=$(psql_cmd -t -c "SELECT COUNT(*) FROM categories;" | tr -d ' ')
    log_success "Categorias cargadas: ${COUNT} registros"
else
    log_error "No se pudo cargar el archivo de categorias"
    exit 1
fi

log_info "Cargando productos-categorias desde ${DATA_DIR}/products/ProductCategory.csv..."
if psql_cmd -c "\copy product_categories(product_id, category_id) FROM '${DATA_DIR}/products/ProductCategory.csv' WITH (FORMAT csv, DELIMITER '|', HEADER true);" > /dev/null 2>&1; then
    COUNT=$(psql_cmd -t -c "SELECT COUNT(*) FROM product_categories;" | tr -d ' ')
    log_success "Productos-categorias cargadas: ${COUNT} registros"
else
    log_error "No se pudo cargar el archivo de productos-categorias"
    exit 1
fi

log_step "CARGA DE TRANSACCIONES"

if [ -n "${TRANSACTION_FILE}" ]; then
    # Cargar un archivo especifico
    FILE_NAME=$(basename "${TRANSACTION_FILE}")
    log_info "Cargando archivo especifico: ${FILE_NAME}..."
    
    if [[ "${FILE_NAME}" != *_Tran.csv ]]; then
        log_error "El archivo debe ser un CSV de transacciones (formato: *_Tran.csv)"
        exit 1
    fi
    
    # Si el archivo tiene ruta relativa, construir la ruta completa dentro del contenedor
    if [[ "${TRANSACTION_FILE}" == /* ]]; then
        FILE_PATH="${TRANSACTION_FILE}"
    else
        FILE_PATH="${DATA_DIR}/transactions/${FILE_NAME}"
    fi
    
    log_info "Ruta del archivo en el contenedor: ${FILE_PATH}"
    
    if psql_cmd -c "\copy transactions(transaction_date, store_id, customer_id, products) FROM '${FILE_PATH}' WITH (FORMAT csv, DELIMITER '|');" > /dev/null 2>&1; then
        COUNT=$(psql_cmd -t -c "SELECT COUNT(*) FROM transactions;" | tr -d ' ')
        log_success "Archivo ${FILE_NAME} cargado. Total de transacciones: ${COUNT}"
    else
        log_error "No se pudo cargar el archivo ${FILE_NAME}"
        exit 1
    fi
else
    # Cargar todas las transacciones
    log_info "Cargando todas las transacciones desde ${DATA_DIR}/transactions/..."
    
    LOADED_FILES=0
    FAILED_FILES=0
    
    # Lista de archivos conocidos
    TRANSACTION_FILES="102_Tran.csv 103_Tran.csv 107_Tran.csv 110_Tran.csv"
    
    for FILE_NAME in ${TRANSACTION_FILES}; do
        FILE_PATH="${DATA_DIR}/transactions/${FILE_NAME}"
        log_info "  -> Cargando: ${FILE_NAME}..."
        
        if psql_cmd -c "\copy transactions(transaction_date, store_id, customer_id, products) FROM '${FILE_PATH}' WITH (FORMAT csv, DELIMITER '|');" > /dev/null 2>&1; then
            log_success "    ${FILE_NAME} cargado exitosamente"
            ((LOADED_FILES++))
        else
            log_warning "    Error al cargar ${FILE_NAME} (archivo puede no existir)"
            ((FAILED_FILES++))
        fi
    done
    
    if [ ${LOADED_FILES} -eq 0 ]; then
        log_error "No se pudieron cargar archivos de transacciones"
        log_info "Verifica que los archivos existan en el directorio data/transactions/"
        exit 1
    fi
    
    if [ ${FAILED_FILES} -gt 0 ]; then
        log_warning "Algunos archivos no se pudieron cargar: ${FAILED_FILES} archivo(s)"
    fi
    
    TOTAL_ROWS=$(psql_cmd -t -c "SELECT COUNT(*) FROM transactions;" | tr -d ' ')
    log_success "Transacciones cargadas: ${LOADED_FILES} archivo(s), ${TOTAL_ROWS} registro(s) totales"
fi

log_step "RESUMEN FINAL"

TOTAL_CATEGORIES=$(psql_cmd -t -c "SELECT COUNT(*) FROM categories;" | tr -d ' ')
TOTAL_PRODUCTS=$(psql_cmd -t -c "SELECT COUNT(DISTINCT product_id) FROM product_categories;" | tr -d ' ')
TOTAL_TRANSACTIONS=$(psql_cmd -t -c "SELECT COUNT(*) FROM transactions;" | tr -d ' ')

echo ""
echo -e "${GREEN}================================================================================"
echo -e "${GREEN}CARGA DE DATOS COMPLETADA"
echo -e "${GREEN}================================================================================"
echo -e "Categorias:        ${TOTAL_CATEGORIES}"
echo -e "Productos unicos:  ${TOTAL_PRODUCTS}"
echo -e "Transacciones:     ${TOTAL_TRANSACTIONS}"
echo -e "${GREEN}================================================================================"
echo ""
