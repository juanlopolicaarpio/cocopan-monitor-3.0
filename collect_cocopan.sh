#!/usr/bin/env bash
set -euo pipefail

# ---- CONFIG (edit if yours differ) ----
CONTAINER="${CONTAINER:-cocopan_postgres}"
DB_USER="${DB_USER:-cocopan}"
DB_NAME="${DB_NAME:-cocopan_monitor}"
SCHEMA="${SCHEMA:-public}"
SAMPLE_LIMIT="${SAMPLE_LIMIT:-200}"
# ---------------------------------------

ts="$(date +%Y%m%d_%H%M%S)"
OUTDIR="cocopan_audit_${ts}"
mkdir -p "${OUTDIR}/samples"

echo "==> Collecting schema from container '${CONTAINER}' DB '${DB_NAME}' as user '${DB_USER}'"
docker exec "${CONTAINER}" pg_dump -U "${DB_USER}" -d "${DB_NAME}" -s > "${OUTDIR}/schema.sql"

echo "==> Collecting approximate row counts per table"
docker exec -i "${CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -Atc \
"SELECT table_schema||'.'||table_name||','||reltuples::bigint
 FROM pg_class c
 JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN information_schema.tables t
   ON t.table_name=c.relname AND t.table_schema=n.nspname
  WHERE c.relkind='r' AND t.table_schema='${SCHEMA}'
 ORDER BY reltuples::bigint DESC;" > "${OUTDIR}/table_counts.csv"


echo "==> Listing ${SCHEMA} tables"
TABLES=$(docker exec -i "${CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -Atc \
"SELECT table_name FROM information_schema.tables
 WHERE table_schema='${SCHEMA}' AND table_type='BASE TABLE'
 ORDER BY table_name;")

# Export a small sample from every table
for T in ${TABLES}; do
  echo "==> Sampling ${SCHEMA}.${T} (first ${SAMPLE_LIMIT} rows)"
  docker exec -i "${CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -c \
  "\COPY (SELECT * FROM ${SCHEMA}.\"${T}\" LIMIT ${SAMPLE_LIMIT}) TO STDOUT WITH CSV HEADER" \
  > "${OUTDIR}/samples/${SCHEMA}.${T}.csv" || echo "    (skip ${T} — copy failed)"
done

# Convenience: top 100 most recent status_checks if present
if echo "${TABLES}" | grep -q '^status_checks$'; then
  docker exec -i "${CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -c \
  "\COPY (SELECT * FROM ${SCHEMA}.status_checks ORDER BY checked_at DESC LIMIT 100) TO STDOUT WITH CSV HEADER" \
  > "${OUTDIR}/samples/${SCHEMA}.status_checks_recent.csv" || true
fi

# Zip everything
ZIP="${OUTDIR}.zip"
zip -r "${ZIP}" "${OUTDIR}" >/dev/null
echo
echo "✅ Done. Created:"
echo "   - ${OUTDIR}/schema.sql"
echo "   - ${OUTDIR}/table_counts.csv"
echo "   - ${OUTDIR}/samples/*.csv"
echo "   - ${ZIP}"
echo
echo "Upload '${ZIP}' here."
