#!/usr/bin/env bash
set -euo pipefail

# Run mypy over all Python files in the repository (excluding common virtualenv/git/cache folders).
# Usage: ./scripts/check_types.sh

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "Project root: $PROJECT_ROOT"

# Find all .py files excluding common directories
mapfile -t PY_FILES < <(find . -type f -name "*.py" \
    ! -path "./.venv/*" \
    ! -path "./venv/*" \
    ! -path "./.git/*" \
    ! -path "./__pycache__/*" \
    ! -path "./.mypy_cache/*" \
    -print)

if [ ${#PY_FILES[@]} -eq 0 ]; then
    echo "No Python files found to check."
    exit 0
fi

# Ensure mypy is available
if ! python -c "import mypy" >/dev/null 2>&1; then
    if ! command -v mypy >/dev/null 2>&1; then
        echo "mypy is not installed in the active Python environment."
        echo "Install with: python -m pip install -r requirements-dev.txt" \
             "or: python -m pip install mypy"
        exit 2
    fi
fi

echo "Running mypy on ${#PY_FILES[@]} files..."

# Run mypy using project's config if present
if [ -f mypy.ini ]; then
    python -m mypy --config-file mypy.ini "${PY_FILES[@]}"
else
    python -m mypy "${PY_FILES[@]}"
fi

echo "mypy finished."
