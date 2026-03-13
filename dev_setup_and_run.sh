#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"
BACKEND_ENV_FILE="$BACKEND_DIR/.env"
FRONTEND_ENV_FILE="$FRONTEND_DIR/.env.local"
PYTHON_BIN="${PYTHON_BIN:-python3}"

INSTALL_ONLY=false
START_ONLY=false

usage() {
  cat <<USAGE
Usage: ./dev_setup_and_run.sh [--install-only | --start-only]

Options:
  --install-only   Install backend/frontend dependencies and create env files, then exit.
  --start-only     Start backend/frontend assuming dependencies are already installed.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-only)
      INSTALL_ONLY=true
      shift
      ;;
    --start-only)
      START_ONLY=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$INSTALL_ONLY" == true && "$START_ONLY" == true ]]; then
  echo "Choose only one of --install-only or --start-only" >&2
  exit 1
fi

install_backend() {
  echo "[backend] Creating virtual environment (if needed)..."
  cd "$BACKEND_DIR"
  "$PYTHON_BIN" -m venv .venv
  # shellcheck disable=SC1091
  source .venv/bin/activate
  echo "[backend] Installing Python dependencies..."
  pip install -r requirements.txt

  if [[ ! -f "$BACKEND_ENV_FILE" ]]; then
    echo "[backend] Creating backend/.env..."
    cat > "$BACKEND_ENV_FILE" <<'ENVEOF'
FRONTEND_URL=http://localhost:3000

# Leave INTERNAL_API_KEY empty in dev — auth check is skipped when unset.
INTERNAL_API_KEY=

# Optional:
# ANTHROPIC_API_KEY=sk-ant-...
# ORS_API_KEY=...
# CENSUS_API_KEY=...
# PSS_DATA_DIR=...
ENVEOF
  fi
}

install_frontend() {
  echo "[frontend] Installing npm dependencies..."
  cd "$FRONTEND_DIR"
  npm install

  if [[ ! -f "$FRONTEND_ENV_FILE" ]]; then
    echo "[frontend] Creating frontend/.env.local..."
    # Generate a random dev secret so NextAuth doesn't error on startup.
    DEV_SECRET=$(LC_ALL=C tr -dc 'a-f0-9' < /dev/urandom | head -c 32 2>/dev/null || echo "dev-secret-change-in-production")
    cat > "$FRONTEND_ENV_FILE" <<ENVEOF
NEXT_PUBLIC_API_URL=http://localhost:8000

# Leave empty in dev — API key check is skipped on the backend when unset.
NEXT_PUBLIC_API_KEY=

# NextAuth — auto-generated for local dev. Replace in production.
NEXTAUTH_SECRET=${DEV_SECRET}
NEXTAUTH_URL=http://localhost:3000

# Add yourself and any local testers here.
# Format: username:password,username2:password2
AUTH_USERS=admin:admin
ENVEOF
    echo "[frontend] Created frontend/.env.local with a dev login: admin / admin"
    echo "           Edit AUTH_USERS in frontend/.env.local to add your team for local testing."
  fi
}

start_services() {
  cd "$ROOT_DIR"

  echo "[run] Starting backend on http://localhost:8000 ..."
  (
    cd "$BACKEND_DIR"
    # shellcheck disable=SC1091
    source .venv/bin/activate
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
  ) &
  BACKEND_PID=$!

  echo "[run] Starting frontend on http://localhost:3000 ..."
  (
    cd "$FRONTEND_DIR"
    npm run dev -- --hostname 0.0.0.0 --port 3000
  ) &
  FRONTEND_PID=$!

  cleanup() {
    echo ""
    echo "[run] Shutting down services..."
    kill "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
    wait "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
  }

  trap cleanup INT TERM EXIT
  wait "$BACKEND_PID" "$FRONTEND_PID"
}

if [[ "$START_ONLY" == false ]]; then
  install_backend
  install_frontend
fi

if [[ "$INSTALL_ONLY" == true ]]; then
  echo "[done] Installation complete. Run ./dev_setup_and_run.sh --start-only to launch services."
  exit 0
fi

if [[ ! -d "$BACKEND_DIR/.venv" ]]; then
  echo "Missing backend/.venv. Run without --start-only first." >&2
  exit 1
fi

start_services
