#!/usr/bin/env bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" <<-EOSQL
    DO
    \$\$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_roles WHERE rolname = 'sales'
        ) THEN
            CREATE ROLE sales LOGIN PASSWORD 'sales';
        END IF;
    END
    \$\$;

    DO
    \$\$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_database WHERE datname = 'sales'
        ) THEN
            CREATE DATABASE sales OWNER sales;
        END IF;
    END
    \$\$;
EOSQL

