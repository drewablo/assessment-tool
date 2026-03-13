#!/usr/bin/env bash
# start.sh — Unified entry point for the Ministry Assessment Tool
#
# Usage:
#   ./start.sh [command]
#
# Commands:
#   dev      (default) Install deps if needed + start backend & frontend locally
#   docker   Start DB/Redis/API/workers in Docker; frontend runs locally
#   prod     Production deploy with Docker Compose (all services + nginx)
#   update   git pull + rebuild production containers in-place
#   stop     Stop Docker services  [--prod for production stack]
#   logs     Tail Docker logs      [--prod] [service-name]
#   status   Show running containers
#   diagnose Collect health + logs for a service (default: frontend)
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"
COMPOSE_DEV="docker-compose.yml"
COMPOSE_PROD="docker-compose.prod.yml"
ENV_PROD=".env.prod"

CMD="${1:-dev}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "  $*"; }
step()  { echo ""; echo "==> $*"; }
die()   { echo "ERROR: $*" >&2; exit 1; }

check_docker() {
  command -v docker >/dev/null 2>&1 || die "Docker is not installed. See https://docs.docker.com/get-docker/"
  docker info >/dev/null 2>&1 || die "Docker daemon is not running."
}

prod_compose() {
  if [[ -f "$ENV_PROD" ]]; then
    docker compose -f "$COMPOSE_PROD" --env-file "$ENV_PROD" "$@"
  else
    docker compose -f "$COMPOSE_PROD" "$@"
  fi
}


# Work around intermittent Docker BuildKit cache corruption errors like:
# "parent snapshot ... does not exist: not found"
prod_up_with_recovery() {
  local output_file
  output_file=$(mktemp)

  set +e
  prod_compose up -d --build --wait 2>&1 | tee "$output_file"
  local status=${PIPESTATUS[0]}
  set -e

  if [[ $status -eq 0 ]]; then
    rm -f "$output_file"
    return 0
  fi

  if grep -Eq "parent snapshot .* does not exist|failed to prepare extraction snapshot" "$output_file"; then
    step "Detected Docker BuildKit cache corruption. Pruning builder cache and retrying once..."
    docker builder prune -f || true

    set +e
    prod_compose up -d --build --wait
    status=$?
    set -e

    rm -f "$output_file"
    return $status
  fi

  rm -f "$output_file"
  return $status
}

# ---------------------------------------------------------------------------
# dev — local Python + npm (no Docker required)
# ---------------------------------------------------------------------------
cmd_dev() {
  step "Starting in local dev mode"
  info "Backend  → http://localhost:8000"
  info "Frontend → http://localhost:3000"
  echo ""
  exec "$ROOT_DIR/dev_setup_and_run.sh" "$@"
}

# ---------------------------------------------------------------------------
# docker — DB/Redis/API/workers in Docker; frontend runs locally (hot-reload)
# ---------------------------------------------------------------------------
cmd_docker() {
  check_docker
  step "Starting Docker services (db, redis, api, worker, beat)..."
  docker compose -f "$COMPOSE_DEV" up -d --build

  # Ensure frontend deps and env are ready
  if [[ ! -d "$FRONTEND_DIR/node_modules" ]]; then
    step "Installing frontend dependencies..."
    (cd "$FRONTEND_DIR" && npm install)
  fi

  if [[ ! -f "$FRONTEND_DIR/.env.local" ]]; then
    step "Creating frontend/.env.local..."
    DEV_SECRET=$(LC_ALL=C tr -dc 'a-f0-9' < /dev/urandom | head -c 32 2>/dev/null || echo "dev-secret-change-in-production")
    cat > "$FRONTEND_DIR/.env.local" <<ENVEOF
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_API_KEY=
NEXTAUTH_SECRET=${DEV_SECRET}
NEXTAUTH_URL=http://localhost:3000
AUTH_USERS=admin:admin
ENVEOF
    info "Created frontend/.env.local  (login: admin / admin)"
  fi

  step "Starting frontend (local, hot-reload)..."
  info "Backend  → http://localhost:8000  (Docker)"
  info "Frontend → http://localhost:3000  (local)"
  echo ""

  cleanup() {
    echo ""
    step "Stopping Docker services..."
    docker compose -f "$COMPOSE_DEV" down --remove-orphans
  }
  trap cleanup INT TERM EXIT

  (cd "$FRONTEND_DIR" && npm run dev -- --hostname 0.0.0.0 --port 3000)
}

# ---------------------------------------------------------------------------
# prod — full production stack via docker-compose.prod.yml
# ---------------------------------------------------------------------------
cmd_prod() {
  check_docker
  [[ -f "$ENV_PROD" ]] || die "$ENV_PROD not found.\nCopy .env.prod.example to .env.prod and fill in all values."
  grep -q "CHANGE_ME" "$ENV_PROD" && die "$ENV_PROD still contains CHANGE_ME placeholders. Fill them in first."

  step "Building and starting production stack..."
  prod_up_with_recovery

  step "Service status"
  prod_compose ps

  FRONTEND_URL=$(grep '^FRONTEND_URL=' "$ENV_PROD" | cut -d= -f2-)
  echo ""
  step "Deploy complete."
  info "App running at: ${FRONTEND_URL:-http://YOUR_SERVER_IP}"
  echo ""
  info "Useful commands:"
  info "  ./start.sh logs --prod     Tail all logs"
  info "  ./start.sh stop --prod     Stop all services"
  info "  ./start.sh update          Pull latest + rebuild"
}

# ---------------------------------------------------------------------------
# update — git pull + rebuild (production)
# ---------------------------------------------------------------------------
cmd_update() {
  check_docker
  [[ -f "$ENV_PROD" ]] || die "$ENV_PROD not found. Run ./start.sh prod first."

  step "Pulling latest code..."
  git pull

  step "Rebuilding and restarting production containers..."
  prod_up_with_recovery

  step "Update complete."
  prod_compose ps
}

# ---------------------------------------------------------------------------
# stop — stop Docker services
# ---------------------------------------------------------------------------
cmd_stop() {
  check_docker
  PROD=false
  for arg in "$@"; do [[ "$arg" == "--prod" ]] && PROD=true; done

  if [[ "$PROD" == true ]]; then
    step "Stopping production stack..."
    prod_compose down --remove-orphans
  else
    step "Stopping dev Docker stack..."
    docker compose -f "$COMPOSE_DEV" down --remove-orphans
  fi
}

# ---------------------------------------------------------------------------
# logs — tail Docker logs
# ---------------------------------------------------------------------------
cmd_logs() {
  check_docker
  PROD=false
  SERVICE=""
  for arg in "$@"; do
    [[ "$arg" == "--prod" ]] && PROD=true || SERVICE="$arg"
  done

  if [[ "$PROD" == true ]]; then
    prod_compose logs -f ${SERVICE}
  else
    docker compose -f "$COMPOSE_DEV" logs -f ${SERVICE}
  fi
}

# ---------------------------------------------------------------------------
# status — show running containers
# ---------------------------------------------------------------------------
cmd_status() {
  check_docker
  echo ""
  echo "Dev stack:"
  docker compose -f "$COMPOSE_DEV" ps 2>/dev/null || info "(not running)"
  echo ""
  echo "Production stack:"
  prod_compose ps 2>/dev/null || info "(not running)"
}

# ---------------------------------------------------------------------------
# diagnose — collect container diagnostics
# ---------------------------------------------------------------------------
cmd_diagnose() {
  check_docker
  PROD=false
  SERVICE="frontend"

  for arg in "$@"; do
    if [[ "$arg" == "--prod" ]]; then
      PROD=true
    elif [[ -n "$arg" ]]; then
      SERVICE="$arg"
    fi
  done

  if [[ "$PROD" == true ]]; then
    COMPOSE_CMD=(docker compose -f "$COMPOSE_PROD")
    [[ -f "$ENV_PROD" ]] && COMPOSE_CMD+=(--env-file "$ENV_PROD")
  else
    COMPOSE_CMD=(docker compose -f "$COMPOSE_DEV")
  fi

  step "Compose service status"
  "${COMPOSE_CMD[@]}" ps || true

  SERVICES=$("${COMPOSE_CMD[@]}" config --services 2>/dev/null || true)
  if [[ -z "$SERVICES" ]]; then
    die "Could not resolve compose services. Check compose file/env configuration."
  fi

  if ! printf '%s\n' "$SERVICES" | grep -qx "$SERVICE"; then
    echo "Defined services:" >&2
    printf '  %s\n' $SERVICES >&2
    die "Service '$SERVICE' is not defined in the selected compose stack."
  fi

  CID=$("${COMPOSE_CMD[@]}" ps -q "$SERVICE" 2>/dev/null || true)
  if [[ -z "$CID" ]]; then
    step "Service '$SERVICE' is defined but has no running/stopped container yet"
    info "Try: ${COMPOSE_CMD[*]} up -d --build $SERVICE"
    if [[ "$PROD" == true ]]; then
      info "Then rerun: ./start.sh diagnose --prod $SERVICE"
    else
      info "Then rerun: ./start.sh diagnose $SERVICE"
    fi
    return 0
  fi

  step "Container state ($SERVICE)"
  docker inspect "$CID" --format 'status={{.State.Status}} restartCount={{.RestartCount}} exitCode={{.State.ExitCode}} error={{.State.Error}}' || true

  step "Container health details ($SERVICE)"
  docker inspect "$CID" --format '{{json .State.Health}}' || true

  step "Healthcheck log entries ($SERVICE)"
  docker inspect "$CID" --format '{{range .State.Health.Log}}{{println .Start "exit=" .ExitCode}}{{println .Output}}{{println "---"}}{{end}}' || true

  step "Recent logs ($SERVICE, last 300 lines)"
  "${COMPOSE_CMD[@]}" logs --tail=300 "$SERVICE" || true
}



# ---------------------------------------------------------------------------
# doctor — API-level diagnostics for DB pipeline readiness
# ---------------------------------------------------------------------------
cmd_doctor() {
  check_docker
  PROD=false
  # Accept both --prod and the literal docs token [--prod]
  for arg in "$@"; do
    [[ "$arg" == "--prod" || "$arg" == "[--prod]" ]] && PROD=true
  done

  command -v curl >/dev/null 2>&1 || die "curl is required for doctor command"

  # Auto-detect production when dev API is not reachable but .env.prod exists.
  if [[ "$PROD" == false ]] && [[ -f "$ENV_PROD" ]]; then
    if ! curl -fsS "http://localhost:8000/api/health" >/dev/null 2>&1; then
      PROD=true
      info "Auto-detected production mode (localhost:8000 unavailable; using $ENV_PROD)."
    fi
  fi

  local base_url
  local api_key=""
  if [[ "$PROD" == true ]]; then
    [[ -f "$ENV_PROD" ]] || die "$ENV_PROD not found."
    base_url=$(grep '^FRONTEND_URL=' "$ENV_PROD" | cut -d= -f2-)
    [[ -n "$base_url" ]] || die "FRONTEND_URL is empty in $ENV_PROD"
    api_key=$(grep '^INTERNAL_API_KEY=' "$ENV_PROD" | cut -d= -f2-)
  else
    base_url="http://localhost:8000"
  fi

  step "API health"
  curl -fsS "$base_url/api/health" || {
    if [[ "$PROD" == false ]]; then
      die "Health endpoint failed: $base_url/api/health. If this is a production-only host, run: ./start.sh doctor --prod"
    fi
    die "Health endpoint failed: $base_url/api/health"
  }
  echo ""

  step "Pipeline status and DB readiness"
  local tmp_json
  tmp_json=$(mktemp)

  if [[ -n "$api_key" ]]; then
    curl -fsS -H "X-API-Key: $api_key" "$base_url/api/pipeline/status" > "$tmp_json" || die "Pipeline status endpoint failed."
  else
    curl -fsS "$base_url/api/pipeline/status" > "$tmp_json" || die "Pipeline status endpoint failed (if INTERNAL_API_KEY is set, use --prod with .env.prod configured)."
  fi

  local py_bin=""
  if command -v python3 >/dev/null 2>&1; then
    py_bin="python3"
  elif command -v python >/dev/null 2>&1; then
    py_bin="python"
  fi

  if [[ -n "$py_bin" ]]; then
    "$py_bin" - "$tmp_json" <<'PYEOF'
import json, sys
p=sys.argv[1]
data=json.load(open(p))
ready = bool(data.get('db_ready_for_analysis'))
readiness_status = data.get('readiness_status') or ('ready' if ready else 'not_ready')
print(f"db_ready_for_analysis={ready}")
print(f"readiness_status={readiness_status}")
print("record_counts:")
for k,v in (data.get('record_counts') or {}).items():
    print(f"  - {k}: {v}")
print("stale_pipelines:", ", ".join(data.get('stale_pipelines') or []) or "none")
issues=data.get('diagnostics') or []
if issues:
    print("diagnostics:")
    for item in issues:
        print(f"  - {item}")
else:
    print("diagnostics: none")

if not ready:
    print("\nnext_steps:")
    print("  - Run data ingestion: ./start.sh ingest --prod")
    print("  - Recheck readiness: ./start.sh doctor --prod")
PYEOF
  else
    info "Neither python3 nor python is installed; printing raw JSON output instead."
    cat "$tmp_json"
    echo ""
  fi

  rm -f "$tmp_json"
}



# ---------------------------------------------------------------------------
# ingest — run DB pipeline ingestion jobs in api container
# ---------------------------------------------------------------------------
cmd_ingest() {
  check_docker
  PROD=false
  PIPELINE="all"
  VINTAGE="2022"
  STATES=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --prod) PROD=true ;;
      --pipeline) PIPELINE="${2:-all}"; shift ;;
      --vintage) VINTAGE="${2:-2022}"; shift ;;
      --states) STATES="${2:-}"; shift ;;
      *) die "Unknown ingest option: $1" ;;
    esac
    shift
  done

  if [[ "$PROD" == true ]]; then
    COMPOSE_CMD=(docker compose -f "$COMPOSE_PROD")
    [[ -f "$ENV_PROD" ]] && COMPOSE_CMD+=(--env-file "$ENV_PROD")
  else
    COMPOSE_CMD=(docker compose -f "$COMPOSE_DEV")
  fi

  case "$PIPELINE" in
    all) CLI_CMD=(python -m pipeline.cli ingest-all --vintage "$VINTAGE") ;;
    census) CLI_CMD=(python -m pipeline.cli ingest-census --vintage "$VINTAGE") ;;
    schools) CLI_CMD=(python -m pipeline.cli ingest-schools) ;;
    elder-care) CLI_CMD=(python -m pipeline.cli ingest-elder-care) ;;
    housing) CLI_CMD=(python -m pipeline.cli ingest-housing) ;;
    status) CLI_CMD=(python -m pipeline.cli status) ;;
    *) die "Unknown pipeline '$PIPELINE'. Use: all|census|schools|elder-care|housing|status" ;;
  esac

  if [[ "$PIPELINE" == "all" || "$PIPELINE" == "census" ]] && [[ -n "$STATES" ]]; then
    CLI_CMD+=(--states "$STATES")
  fi

  step "Running ingestion in api container (pipeline=$PIPELINE)"
  "${COMPOSE_CMD[@]}" exec -T api "${CLI_CMD[@]}"

  step "Post-ingest readiness check"
  if [[ "$PROD" == true ]]; then
    cmd_doctor --prod
  else
    cmd_doctor
  fi
}

# ---------------------------------------------------------------------------
# usage
# ---------------------------------------------------------------------------
cmd_help() {
  cat <<USAGE

  Ministry Assessment Tool — start.sh

  Usage: ./start.sh [command] [options]

  Commands:
    dev             Install deps + start backend & frontend locally (default)
    docker          Run DB/Redis/API/workers in Docker; frontend hot-reloads locally
    prod            Full production deploy (all services + nginx)
    update          git pull + rebuild production containers
    stop            Stop dev Docker services  (--prod for production stack)
    logs [svc]      Tail Docker logs          (--prod for production stack)
    status          Show all running containers
    diagnose [svc]  Show health + logs (default svc: frontend) [--prod]
    doctor          Check API DB readiness + pipeline diagnostics [--prod]
    ingest          Run pipeline ingestion in api container [--prod]
    help            Show this message

  Examples:
    ./start.sh                    # quick local dev start
    ./start.sh docker             # Docker backend + local frontend
    ./start.sh prod               # deploy to production server
    ./start.sh update             # pull latest + rebuild prod
    ./start.sh logs api           # tail api logs (dev stack)
    ./start.sh logs --prod        # tail all prod logs
    ./start.sh stop --prod        # stop production stack
    ./start.sh diagnose --prod frontend  # debug frontend startup
    ./start.sh doctor --prod             # API-level DB readiness checks
    ./start.sh doctor [--prod]           # bracket form also accepted
    ./start.sh ingest --prod             # ingest all datasets into DB

USAGE
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
shift || true   # consume the command arg so remaining $@ are options
case "$CMD" in
  dev)    cmd_dev    "$@" ;;
  docker) cmd_docker "$@" ;;
  prod)   cmd_prod   "$@" ;;
  update) cmd_update "$@" ;;
  stop)   cmd_stop   "$@" ;;
  logs)   cmd_logs   "$@" ;;
  status) cmd_status        ;;
  diagnose) cmd_diagnose "$@" ;;
  doctor) cmd_doctor "$@" ;;
  ingest) cmd_ingest "$@" ;;
  help|-h|--help) cmd_help  ;;
  *)
    echo "Unknown command: $CMD" >&2
    cmd_help
    exit 1
    ;;
esac
