#!/bin/bash
set -e

echo "==> Installing dbt packages..."
dbt deps --profiles-dir .

echo "==> Running dbt models (prod)..."
dbt run --profiles-dir . --target prod

echo "==> Done."
