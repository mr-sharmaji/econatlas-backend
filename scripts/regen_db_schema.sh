#!/usr/bin/env bash
# Regenerate docs/db_schema.md from the live DB.
# Run AFTER a migration has been applied to production.
set -euo pipefail

API="${API_URL:-https://api.velqon.xyz}"
OUT="$(cd "$(dirname "$0")/.." && pwd)/docs/db_schema.md"
TMP=$(mktemp -d)

sql() {
  curl -sS -X POST "$API/ops/sql" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"$1\"}"
}

sql "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name, ordinal_position" > "$TMP/cols.json"
sql "SELECT tc.table_name, kcu.column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name WHERE tc.table_schema='public' AND tc.constraint_type='PRIMARY KEY'" > "$TMP/pks.json"
sql "SELECT tc.table_name, kcu.column_name, ccu.table_name AS ref_table, ccu.column_name AS ref_column FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name=tc.constraint_name WHERE tc.table_schema='public' AND tc.constraint_type='FOREIGN KEY'" > "$TMP/fks.json"

python3 - "$TMP" "$OUT" "$API" <<'PY'
import json, sys
from pathlib import Path
from collections import defaultdict

tmp, out_path, api = sys.argv[1], sys.argv[2], sys.argv[3]
cols = json.load(open(f"{tmp}/cols.json"))['rows']
pks = defaultdict(list)
for r in json.load(open(f"{tmp}/pks.json"))['rows']:
    pks[r['table_name']].append(r['column_name'])
fks = defaultdict(list)
for r in json.load(open(f"{tmp}/fks.json"))['rows']:
    fks[r['table_name']].append(f"{r['column_name']} -> {r['ref_table']}.{r['ref_column']}")

by_table = defaultdict(list)
for r in cols:
    nn = '' if r['is_nullable']=='YES' else ' NOT NULL'
    by_table[r['table_name']].append(f"  - {r['column_name']}: {r['data_type']}{nn}")

tables = sorted(by_table)
total_cols = len(cols)
lines = [
    "# EconAtlas DB Schema (public)",
    "",
    f"{len(tables)} tables, {total_cols} columns. Snapshot from `information_schema`.",
    "",
    f"Query live via `POST {api}/ops/sql` with body `{{\"query\":\"...\"}}`.",
    "",
    "Regenerate after migrations: `scripts/regen_db_schema.sh`",
    "",
]
for t in tables:
    lines.append(f"## {t}")
    if pks.get(t): lines.append(f"**PK:** {', '.join(pks[t])}")
    if fks.get(t):
        lines.append("**FK:**")
        for f in fks[t]: lines.append(f"  - {f}")
    if pks.get(t) or fks.get(t): lines.append("")
    lines.extend(by_table[t])
    lines.append("")
Path(out_path).write_text('\n'.join(lines))
print(f"wrote {out_path} ({len(tables)} tables, {total_cols} cols)")
PY

rm -rf "$TMP"
