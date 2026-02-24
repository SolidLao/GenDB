#!/usr/bin/env bash
#
# Download and extract SEC EDGAR Financial Statement Data Sets.
#
# Usage:
#   bash benchmarks/sec-edgar/setup_data.sh [YEARS]
#
# YEARS defaults to 3 (2022-2024, 12 quarters).
# Data is written to benchmarks/sec-edgar/data/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YEARS="${1:-3}"
DATA_DIR="${SCRIPT_DIR}/data"

# Compute year range: most recent YEARS years ending at 2024
END_YEAR=2024
START_YEAR=$((END_YEAR - YEARS + 1))

echo "=== SEC EDGAR Data Setup ==="
echo "Years: ${YEARS} (${START_YEAR}-${END_YEAR})"
echo "Data directory: ${DATA_DIR}"

mkdir -p "${DATA_DIR}"

BASE_URL="https://www.sec.gov/files/dera/data/financial-statement-data-sets"

for year in $(seq "${START_YEAR}" "${END_YEAR}"); do
    for quarter in 1 2 3 4; do
        tag="${year}q${quarter}"
        zip_file="${DATA_DIR}/${tag}.zip"
        extract_dir="${DATA_DIR}/${tag}"

        if [ -d "${extract_dir}" ] && [ -f "${extract_dir}/sub.txt" ]; then
            echo "  ${tag}: already extracted, skipping download."
            continue
        fi

        url="${BASE_URL}/${tag}.zip"
        echo "  Downloading ${tag}.zip..."
        if ! curl -fSL --retry 3 --retry-delay 5 \
             -H "User-Agent: Mozilla/5.0 (X11; Linux x86_64)" \
             -H "Accept-Encoding: gzip" \
             -o "${zip_file}" "${url}"; then
            echo "  WARNING: Failed to download ${tag}.zip (may not exist yet), skipping."
            rm -f "${zip_file}"
            continue
        fi

        echo "  Extracting ${tag}.zip..."
        mkdir -p "${extract_dir}"
        unzip -o -q "${zip_file}" -d "${extract_dir}"
        rm -f "${zip_file}"
    done
done

echo ""
echo "=== SEC EDGAR data ready ==="
echo ""
echo "Row counts per quarter:"
for dir in "${DATA_DIR}"/*/; do
    tag=$(basename "${dir}")
    echo "  ${tag}:"
    for f in "${dir}"*.txt; do
        if [ -f "$f" ]; then
            name=$(basename "$f")
            count=$(($(wc -l < "$f") - 1))  # subtract header line
            echo "    ${name}: ${count} rows"
        fi
    done
done
