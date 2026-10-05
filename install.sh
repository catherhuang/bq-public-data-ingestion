#!/usr/bin/env bash
#
# Installer script for bq-public-data-ingestion Antigravity skill
#
set -e

SKILL_NAME="bq-public-data-ingestion"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    echo "Usage: $0 [--global | --workspace]"
    echo ""
    echo "Options:"
    echo "  --global      Install to ~/.gemini/config/skills/${SKILL_NAME} (Default)"
    echo "  --workspace   Install to current git project (.agents/skills/${SKILL_NAME})"
    exit 1
}

TARGET_MODE="global"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --global) TARGET_MODE="global"; shift ;;
        --workspace) TARGET_MODE="workspace"; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown parameter: $1"; usage ;;
    esac
done

if [ "$TARGET_MODE" = "global" ]; then
    DEST_DIR="${HOME}/.gemini/config/skills/${SKILL_NAME}"
else
    # Find git root
    GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
    DEST_DIR="${GIT_ROOT}/.agents/skills/${SKILL_NAME}"
fi

echo "==> Installing ${SKILL_NAME} skill..."
echo "    Source:      ${SCRIPT_DIR}"
echo "    Destination: ${DEST_DIR}"

mkdir -p "${DEST_DIR}/references"
mkdir -p "${DEST_DIR}/templates"

# Copy core skill components
cp "${SCRIPT_DIR}/SKILL.md" "${DEST_DIR}/"
cp -r "${SCRIPT_DIR}/references/"* "${DEST_DIR}/references/"
cp -r "${SCRIPT_DIR}/templates/"* "${DEST_DIR}/templates/"

echo "==> Installation complete! 🎉"
echo "    The skill '${SKILL_NAME}' is now ready to use in Antigravity."
