#!/usr/bin/env bash
# Carga los datos sintéticos de la capa RAW en BigQuery.
#
# Uso:
#   bash scripts/setup.sh <PROJECT_ID>              # crea raw_data.{users,products,orders}
#   bash scripts/setup.sh <PROJECT_ID> --simulate   # simula un nuevo día de cambios
set -euo pipefail

PROJECT_ID="${1:?Uso: bash scripts/setup.sh <PROJECT_ID> [--simulate]}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${2:-}" == "--simulate" ]]; then
  SQL_FILE="${SCRIPT_DIR}/02_simulate_changes.sql"
else
  SQL_FILE="${SCRIPT_DIR}/01_create_raw_data.sql"
fi

echo "Ejecutando $(basename "${SQL_FILE}") en el proyecto ${PROJECT_ID}..."
bq query --project_id="${PROJECT_ID}" --location=US --use_legacy_sql=false < "${SQL_FILE}"
echo "Listo."
