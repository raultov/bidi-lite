#!/usr/bin/env bash
set -euo pipefail

# Directory for dedicated Firefox profile and PID file (handles snap and non-snap)
if [ -d "$HOME/snap/firefox/common" ]; then
  DEFAULT_BASE="$HOME/snap/firefox/common/bidi-lite"
else
  DEFAULT_BASE="$HOME/.local/share/bidi-lite/e2e"
fi
E2E_BASE="${BIDI_LITE_E2E_HOME:-$DEFAULT_BASE}"
PROFILE_DIR="$E2E_BASE/profile"
PID_FILE="$E2E_BASE/firefox.pid"
LOG_FILE="$E2E_BASE/firefox.log"

BIDI_PORT="${BIDI_PORT:-9222}"
RDP_PORT="${RDP_PORT:-6000}"

wait_port() {
  local port="$1"
  local timeout=20
  local start
  start=$(date +%s)
  while true; do
    if (echo > /dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
      return 0
    fi
    local now
    now=$(date +%s)
    if [ $((now - start)) -ge "$timeout" ]; then
      echo "Timeout waiting for port $port" >&2
      return 1
    fi
    sleep 0.2
  done
}

start_firefox() {
  if [ -f "$PID_FILE" ]; then
    local old_pid
    old_pid=$(cat "$PID_FILE")
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "Firefox already running with PID $old_pid"
      return 0
    else
      rm -f "$PID_FILE"
    fi
  fi

  mkdir -p "$PROFILE_DIR"
  cat <<'EOF' > "$PROFILE_DIR/user.js"
user_pref("devtools.debugger.remote-enabled", true);
user_pref("devtools.chrome.enabled", true);
user_pref("devtools.debugger.prompt-connection", false);
user_pref("browser.shell.checkDefaultBrowser", false);
user_pref("app.update.auto", false);
user_pref("app.update.enabled", false);
EOF

  local firefox_bin
  firefox_bin=$(command -v firefox || echo "/usr/bin/firefox")

  echo "Starting Firefox (visible window, not headless) with BiDi on :$BIDI_PORT and RDP on ws:$RDP_PORT..."
  "$firefox_bin" \
    --no-remote \
    --profile "$PROFILE_DIR" \
    --remote-debugging-port="$BIDI_PORT" \
    --start-debugger-server "ws:$RDP_PORT" \
    > "$LOG_FILE" 2>&1 &

  local pid=$!
  echo "$pid" > "$PID_FILE"

  if ! wait_port "$BIDI_PORT"; then
    echo "Failed to connect to BiDi port $BIDI_PORT. Log:" >&2
    cat "$LOG_FILE" >&2
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    exit 1
  fi

  if ! wait_port "$RDP_PORT"; then
    echo "Failed to connect to RDP port $RDP_PORT. Log:" >&2
    cat "$LOG_FILE" >&2
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    exit 1
  fi

  echo "Firefox ready (PID $pid)."
}

stop_firefox() {
  if [ -f "$PID_FILE" ]; then
    local pid
    pid=$(cat "$PID_FILE")
    echo "Stopping Firefox PID $pid..."
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      local timeout=10
      local start
      start=$(date +%s)
      while kill -0 "$pid" 2>/dev/null; do
        local now
        now=$(date +%s)
        if [ $((now - start)) -ge "$timeout" ]; then
          kill -9 "$pid" 2>/dev/null || true
          break
        fi
        sleep 0.2
      done
    fi
    rm -f "$PID_FILE"
    echo "Firefox stopped."
  else
    echo "No Firefox PID file found at $PID_FILE."
  fi
}

status_firefox() {
  if [ -f "$PID_FILE" ]; then
    local pid
    pid=$(cat "$PID_FILE")
    if kill -0 "$pid" 2>/dev/null; then
      echo "Firefox running with PID $pid"
      return 0
    fi
  fi
  echo "Firefox not running"
  return 1
}

case "${1:-}" in
  start)
    start_firefox
    ;;
  stop)
    stop_firefox
    ;;
  status)
    status_firefox
    ;;
  *)
    echo "Usage: $0 {start|stop|status}" >&2
    exit 1
    ;;
esac
