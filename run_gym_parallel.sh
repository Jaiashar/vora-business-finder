#!/bin/bash

# Vora Gym Pipeline - PARALLEL version
# Runs 3 workers simultaneously, each processing its own batch of cities

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results/gyms"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"
LOG_DIR="$SCRIPT_DIR/results/gyms/logs"

mkdir -p "$RESULTS_DIR" "$LOG_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m'

print_msg() { echo -e "${1}${2}${NC}"; }

city_to_filename() {
    echo "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/_/g' | sed 's/__*/_/g' | sed 's/^_//;s/_$//'
}

# Process a single city end-to-end
process_city() {
    local city="$1"
    local worker="$2"
    local filename=$(city_to_filename "$city")
    local raw_csv="$RESULTS_DIR/${filename}.csv"
    local enriched_csv="$RESULTS_DIR/${filename}_enriched.csv"

    echo "[W${worker}] ========== Starting: $city =========="

    # Step 1: Scrape
    if [[ -f "$raw_csv" ]]; then
        # Check if it's raw scraper output (has 'input_id' column) or extracted emails
        if head -1 "$raw_csv" | grep -q "input_id"; then
            echo "[W${worker}] $city: Raw scraper CSV found, re-running email extraction..."
            local emails_csv="${raw_csv%.csv}_emails.csv"
            "$VENV_PYTHON" "$SCRIPT_DIR/extract_emails.py" "$raw_csv" "$emails_csv"
            if [[ -f "$emails_csv" ]]; then
                mv "$emails_csv" "$raw_csv"
            fi
        else
            local line_count=$(wc -l < "$raw_csv" | tr -d ' ')
            if [[ $line_count -gt 1 ]]; then
                echo "[W${worker}] $city: Already have extracted emails CSV, skipping scrape"
            else
                echo "[W${worker}] $city: Empty CSV, re-scraping..."
                rm -f "$raw_csv"
            fi
        fi
    fi

    if [[ ! -f "$raw_csv" ]] || head -1 "$raw_csv" 2>/dev/null | grep -q "input_id"; then
        echo "[W${worker}] $city: Scraping..."
        if ! bash "$SCRIPT_DIR/find_gym_leads.sh" "$city" --force 2>&1; then
            echo "[W${worker}] $city: SCRAPE FAILED, skipping"
            return 1
        fi
    fi

    # Verify CSV
    if [[ ! -f "$raw_csv" ]]; then
        echo "[W${worker}] $city: No CSV found, skipping"
        return 1
    fi

    local lead_count=$(( $(wc -l < "$raw_csv" | tr -d ' ') - 1 ))
    if [[ $lead_count -le 0 ]]; then
        echo "[W${worker}] $city: No leads, skipping"
        return 0
    fi
    echo "[W${worker}] $city: $lead_count leads found"

    # Step 2: Enrich
    if [[ -f "$enriched_csv" ]] && [[ $(wc -l < "$enriched_csv" | tr -d ' ') -gt 1 ]]; then
        local ec=$(( $(wc -l < "$enriched_csv" | tr -d ' ') - 1 ))
        echo "[W${worker}] $city: Already enriched ($ec leads), skipping enrich"
    else
        echo "[W${worker}] $city: Enriching..."
        if ! "$VENV_PYTHON" "$SCRIPT_DIR/gym_outreach.py" "$raw_csv" --enrich 2>&1; then
            echo "[W${worker}] $city: ENRICH FAILED, skipping"
            return 1
        fi
    fi

    # Step 3: Send live
    echo "[W${worker}] $city: Sending live emails..."
    if ! "$VENV_PYTHON" "$SCRIPT_DIR/gym_outreach.py" "$enriched_csv" --live 2>&1; then
        echo "[W${worker}] $city: SEND FAILED"
        return 1
    fi

    echo "[W${worker}] ========== DONE: $city =========="
    return 0
}

# Worker function - processes a list of cities sequentially
run_worker() {
    local worker_id="$1"
    shift
    local cities=("$@")

    echo "[W${worker_id}] Starting worker with ${#cities[@]} cities"
    local done=0
    local failed=0

    for city in "${cities[@]}"; do
        if process_city "$city" "$worker_id"; then
            done=$((done + 1))
        else
            failed=$((failed + 1))
        fi
        sleep 2
    done

    echo "[W${worker_id}] WORKER COMPLETE: $done done, $failed failed"
}

# ── Assign cities to workers ─────────────────────────────────
# Skip Irvine (fully complete). Aliso Viejo needs email extraction.

WORKER1_CITIES=(
    "Aliso Viejo, CA"
    "Anaheim, CA"
    "Brea, CA"
    "Buena Park, CA"
    "Costa Mesa, CA"
    "Cypress, CA"
)

WORKER2_CITIES=(
    "Dana Point, CA"
    "Fountain Valley, CA"
    "Fullerton, CA"
    "Garden Grove, CA"
    "Huntington Beach, CA"
    "La Habra, CA"
)

WORKER3_CITIES=(
    "La Palma, CA"
    "Laguna Beach, CA"
    "Laguna Hills, CA"
    "Laguna Niguel, CA"
    "Laguna Woods, CA"
    "Lake Forest, CA"
)

echo ""
print_msg "$MAGENTA" "╔══════════════════════════════════════════════════════════╗"
print_msg "$MAGENTA" "║    VORA GYM PIPELINE - 3 PARALLEL WORKERS               ║"
print_msg "$MAGENTA" "║    18 cities remaining (Irvine already done)             ║"
print_msg "$MAGENTA" "╚══════════════════════════════════════════════════════════╝"
echo ""
print_msg "$CYAN" "  Worker 1: Aliso Viejo, Anaheim, Brea, Buena Park, Costa Mesa, Cypress"
print_msg "$CYAN" "  Worker 2: Dana Point, Fountain Valley, Fullerton, Garden Grove, Huntington Beach, La Habra"
print_msg "$CYAN" "  Worker 3: La Palma, Laguna Beach, Laguna Hills, Laguna Niguel, Laguna Woods, Lake Forest"
echo ""

START_TIME=$(date +%s)

# Launch 3 workers in parallel
run_worker 1 "${WORKER1_CITIES[@]}" > "$LOG_DIR/worker1.log" 2>&1 &
W1_PID=$!

run_worker 2 "${WORKER2_CITIES[@]}" > "$LOG_DIR/worker2.log" 2>&1 &
W2_PID=$!

run_worker 3 "${WORKER3_CITIES[@]}" > "$LOG_DIR/worker3.log" 2>&1 &
W3_PID=$!

print_msg "$GREEN" "  Workers launched: PIDs $W1_PID, $W2_PID, $W3_PID"
print_msg "$BLUE" "  Logs: $LOG_DIR/worker{1,2,3}.log"
echo ""

# Monitor progress
while true; do
    W1_ALIVE=0; W2_ALIVE=0; W3_ALIVE=0
    kill -0 $W1_PID 2>/dev/null && W1_ALIVE=1
    kill -0 $W2_PID 2>/dev/null && W2_ALIVE=1
    kill -0 $W3_PID 2>/dev/null && W3_ALIVE=1

    ALIVE=$((W1_ALIVE + W2_ALIVE + W3_ALIVE))
    ELAPSED=$(( $(date +%s) - START_TIME ))
    ELAPSED_MIN=$(( ELAPSED / 60 ))

    # Count completed cities from logs
    W1_DONE=$(grep -c "========== DONE:" "$LOG_DIR/worker1.log" 2>/dev/null || true)
    W2_DONE=$(grep -c "========== DONE:" "$LOG_DIR/worker2.log" 2>/dev/null || true)
    W3_DONE=$(grep -c "========== DONE:" "$LOG_DIR/worker3.log" 2>/dev/null || true)
    W1_DONE=${W1_DONE:-0}; W2_DONE=${W2_DONE:-0}; W3_DONE=${W3_DONE:-0}
    TOTAL_DONE=$((W1_DONE + W2_DONE + W3_DONE))

    # Show current city per worker
    W1_CURRENT=$(grep "Starting:" "$LOG_DIR/worker1.log" 2>/dev/null | tail -1 | sed 's/.*Starting: //' | sed 's/ =.*//')
    W2_CURRENT=$(grep "Starting:" "$LOG_DIR/worker2.log" 2>/dev/null | tail -1 | sed 's/.*Starting: //' | sed 's/ =.*//')
    W3_CURRENT=$(grep "Starting:" "$LOG_DIR/worker3.log" 2>/dev/null | tail -1 | sed 's/.*Starting: //' | sed 's/ =.*//')

    echo "[${ELAPSED_MIN}m] Workers: $ALIVE/3 active | Cities: $TOTAL_DONE/18 done | W1: $W1_CURRENT ($W1_DONE/6) | W2: $W2_CURRENT ($W2_DONE/6) | W3: $W3_CURRENT ($W3_DONE/6)"

    if [[ $ALIVE -eq 0 ]]; then
        break
    fi

    sleep 60
done

# Wait for all to finish
wait $W1_PID 2>/dev/null
wait $W2_PID 2>/dev/null
wait $W3_PID 2>/dev/null

END_TIME=$(date +%s)
TOTAL_DURATION=$(( END_TIME - START_TIME ))
TOTAL_MIN=$(( TOTAL_DURATION / 60 ))

echo ""
print_msg "$MAGENTA" "╔══════════════════════════════════════════════════════════╗"
print_msg "$MAGENTA" "║    ALL WORKERS COMPLETE                                  ║"
print_msg "$MAGENTA" "╚══════════════════════════════════════════════════════════╝"
echo ""
print_msg "$GREEN" "  Total time: ${TOTAL_MIN} minutes"
print_msg "$GREEN" "  Cities done: $TOTAL_DONE/18"
echo ""
print_msg "$BLUE" "  Worker 1 log: $LOG_DIR/worker1.log"
print_msg "$BLUE" "  Worker 2 log: $LOG_DIR/worker2.log"
print_msg "$BLUE" "  Worker 3 log: $LOG_DIR/worker3.log"
echo ""

# Show any failures
echo "--- Failures ---"
grep "FAILED" "$LOG_DIR/worker1.log" "$LOG_DIR/worker2.log" "$LOG_DIR/worker3.log" 2>/dev/null || echo "None!"
echo ""
