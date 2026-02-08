#!/bin/bash

# Vora Combined Pipeline - Clinics + Gyms
# Runs both clinic and gym scraping/outreach for each city
# Uses 3 parallel workers

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINIC_RESULTS_DIR="$SCRIPT_DIR/results"
GYM_RESULTS_DIR="$SCRIPT_DIR/results/gyms"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"
LOG_DIR="$SCRIPT_DIR/results/combined_logs"

mkdir -p "$CLINIC_RESULTS_DIR" "$GYM_RESULTS_DIR" "$LOG_DIR"

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

# Process a single city: both clinic + gym pipelines
process_city() {
    local city="$1"
    local worker="$2"
    local filename=$(city_to_filename "$city")
    local clinic_csv="$CLINIC_RESULTS_DIR/${filename}.csv"
    local clinic_enriched="$CLINIC_RESULTS_DIR/${filename}_enriched.csv"
    local gym_csv="$GYM_RESULTS_DIR/${filename}.csv"
    local gym_enriched="$GYM_RESULTS_DIR/${filename}_enriched.csv"

    echo "[W${worker}] ========== Starting: $city =========="

    # ── Step 1: Scrape BOTH in parallel ────────────────────────
    local clinic_scrape_needed=true
    local gym_scrape_needed=true

    # Check if clinic CSV already exists and is extracted
    if [[ -f "$clinic_csv" ]]; then
        if head -1 "$clinic_csv" | grep -q "input_id"; then
            echo "[W${worker}] $city: Raw clinic CSV found, re-running email extraction..."
            local clinic_emails="${clinic_csv%.csv}_emails.csv"
            "$VENV_PYTHON" "$SCRIPT_DIR/extract_emails.py" "$clinic_csv" "$clinic_emails"
            if [[ -f "$clinic_emails" ]]; then
                mv "$clinic_emails" "$clinic_csv"
            fi
            clinic_scrape_needed=false
        else
            local lc=$(wc -l < "$clinic_csv" | tr -d ' ')
            if [[ $lc -gt 1 ]]; then
                echo "[W${worker}] $city: Clinic CSV already extracted, skipping scrape"
                clinic_scrape_needed=false
            else
                echo "[W${worker}] $city: Empty clinic CSV, will re-scrape"
                rm -f "$clinic_csv"
            fi
        fi
    fi

    # Check if gym CSV already exists and is extracted
    if [[ -f "$gym_csv" ]]; then
        if head -1 "$gym_csv" | grep -q "input_id"; then
            echo "[W${worker}] $city: Raw gym CSV found, re-running email extraction..."
            local gym_emails="${gym_csv%.csv}_emails.csv"
            "$VENV_PYTHON" "$SCRIPT_DIR/extract_emails.py" "$gym_csv" "$gym_emails"
            if [[ -f "$gym_emails" ]]; then
                mv "$gym_emails" "$gym_csv"
            fi
            gym_scrape_needed=false
        else
            local lc=$(wc -l < "$gym_csv" | tr -d ' ')
            if [[ $lc -gt 1 ]]; then
                echo "[W${worker}] $city: Gym CSV already extracted, skipping scrape"
                gym_scrape_needed=false
            else
                echo "[W${worker}] $city: Empty gym CSV, will re-scrape"
                rm -f "$gym_csv"
            fi
        fi
    fi

    # Run scrapers sequentially (parallel overloads the system with too many browsers)
    local clinic_ok=true
    local gym_ok=true

    if [[ "$clinic_scrape_needed" == true ]]; then
        echo "[W${worker}] $city: Scraping clinics..."
        if ! bash "$SCRIPT_DIR/find_leads.sh" "$city" --force 2>&1; then
            echo "[W${worker}] $city: CLINIC SCRAPE FAILED"
            clinic_ok=false
        fi
    fi

    if [[ "$gym_scrape_needed" == true ]]; then
        echo "[W${worker}] $city: Scraping gyms..."
        if ! bash "$SCRIPT_DIR/find_gym_leads.sh" "$city" --force 2>&1; then
            echo "[W${worker}] $city: GYM SCRAPE FAILED"
            gym_ok=false
        fi
    fi

    # ── Step 2: Verify CSVs and count leads ────────────────────
    local clinic_leads=0
    local gym_leads=0

    if [[ "$clinic_ok" == true ]] && [[ -f "$clinic_csv" ]]; then
        # Make sure it's extracted format
        if head -1 "$clinic_csv" | grep -q "input_id"; then
            echo "[W${worker}] $city: Clinic CSV still raw, extracting emails..."
            local clinic_emails="${clinic_csv%.csv}_emails.csv"
            "$VENV_PYTHON" "$SCRIPT_DIR/extract_emails.py" "$clinic_csv" "$clinic_emails"
            if [[ -f "$clinic_emails" ]]; then
                mv "$clinic_emails" "$clinic_csv"
            fi
        fi
        clinic_leads=$(( $(wc -l < "$clinic_csv" | tr -d ' ') - 1 ))
        if [[ $clinic_leads -le 0 ]]; then clinic_leads=0; fi
    fi

    if [[ "$gym_ok" == true ]] && [[ -f "$gym_csv" ]]; then
        # Make sure it's extracted format
        if head -1 "$gym_csv" | grep -q "input_id"; then
            echo "[W${worker}] $city: Gym CSV still raw, extracting emails..."
            local gym_emails="${gym_csv%.csv}_emails.csv"
            "$VENV_PYTHON" "$SCRIPT_DIR/extract_emails.py" "$gym_csv" "$gym_emails"
            if [[ -f "$gym_emails" ]]; then
                mv "$gym_emails" "$gym_csv"
            fi
        fi
        gym_leads=$(( $(wc -l < "$gym_csv" | tr -d ' ') - 1 ))
        if [[ $gym_leads -le 0 ]]; then gym_leads=0; fi
    fi

    echo "[W${worker}] $city: ${clinic_leads} clinic leads, ${gym_leads} gym leads"

    # ── Step 3: Send clinic emails ─────────────────────────────
    if [[ $clinic_leads -gt 0 ]]; then
        echo "[W${worker}] $city: Sending clinic emails..."
        if ! "$VENV_PYTHON" "$SCRIPT_DIR/outreach.py" "$clinic_csv" --live 2>&1; then
            echo "[W${worker}] $city: CLINIC SEND FAILED"
        fi
    else
        echo "[W${worker}] $city: No clinic leads to send"
    fi

    # ── Step 4: Send gym emails ────────────────────────────────
    if [[ $gym_leads -gt 0 ]]; then
        echo "[W${worker}] $city: Sending gym emails..."
        if ! "$VENV_PYTHON" "$SCRIPT_DIR/gym_outreach.py" "$gym_csv" --live 2>&1; then
            echo "[W${worker}] $city: GYM SEND FAILED"
        fi
    else
        echo "[W${worker}] $city: No gym leads to send"
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

# ── Assign 15 remaining cities to 3 workers ───────────────────

# Remaining 6 cities

WORKER1_CITIES=(
    "Placentia, CA"
    "San Clemente, CA"
    "San Juan Capistrano, CA"
)

WORKER2_CITIES=(
    "Villa Park, CA"
    "Westminster, CA"
    "Yorba Linda, CA"
)

TOTAL_CITIES=6

echo ""
print_msg "$MAGENTA" "╔══════════════════════════════════════════════════════════╗"
print_msg "$MAGENTA" "║    VORA COMBINED PIPELINE - Clinics + Gyms              ║"
print_msg "$MAGENTA" "║    $TOTAL_CITIES cities | 2 parallel workers                        ║"
print_msg "$MAGENTA" "╚══════════════════════════════════════════════════════════╝"
echo ""
print_msg "$CYAN" "  Worker 1: Placentia, San Clemente, San Juan Capistrano"
print_msg "$CYAN" "  Worker 2: Villa Park, Westminster, Yorba Linda"
echo ""

START_TIME=$(date +%s)

# Launch 2 workers in parallel
run_worker 1 "${WORKER1_CITIES[@]}" > "$LOG_DIR/worker1.log" 2>&1 &
W1_PID=$!

run_worker 2 "${WORKER2_CITIES[@]}" > "$LOG_DIR/worker2.log" 2>&1 &
W2_PID=$!

print_msg "$GREEN" "  Workers launched: PIDs $W1_PID, $W2_PID"
print_msg "$BLUE" "  Logs: $LOG_DIR/worker{1,2}.log"
echo ""

# Monitor progress
while true; do
    W1_ALIVE=0; W2_ALIVE=0
    kill -0 $W1_PID 2>/dev/null && W1_ALIVE=1
    kill -0 $W2_PID 2>/dev/null && W2_ALIVE=1

    ALIVE=$((W1_ALIVE + W2_ALIVE))
    ELAPSED=$(( $(date +%s) - START_TIME ))
    ELAPSED_MIN=$(( ELAPSED / 60 ))

    # Count completed cities from logs
    W1_DONE=$(grep -c "========== DONE:" "$LOG_DIR/worker1.log" 2>/dev/null || true)
    W2_DONE=$(grep -c "========== DONE:" "$LOG_DIR/worker2.log" 2>/dev/null || true)
    W1_DONE=${W1_DONE:-0}; W2_DONE=${W2_DONE:-0}
    TOTAL_DONE=$((W1_DONE + W2_DONE))

    # Show current city per worker
    W1_CURRENT=$(grep "Starting:" "$LOG_DIR/worker1.log" 2>/dev/null | tail -1 | sed 's/.*Starting: //' | sed 's/ =.*//' || echo "---")
    W2_CURRENT=$(grep "Starting:" "$LOG_DIR/worker2.log" 2>/dev/null | tail -1 | sed 's/.*Starting: //' | sed 's/ =.*//' || echo "---")

    echo "[${ELAPSED_MIN}m] Workers: $ALIVE/2 | Done: $TOTAL_DONE/$TOTAL_CITIES | W1: $W1_CURRENT ($W1_DONE/3) | W2: $W2_CURRENT ($W2_DONE/3)"

    if [[ $ALIVE -eq 0 ]]; then
        break
    fi

    sleep 60
done

# Wait for all to finish
wait $W1_PID 2>/dev/null
wait $W2_PID 2>/dev/null

END_TIME=$(date +%s)
TOTAL_DURATION=$(( END_TIME - START_TIME ))
TOTAL_MIN=$(( TOTAL_DURATION / 60 ))

echo ""
print_msg "$MAGENTA" "╔══════════════════════════════════════════════════════════╗"
print_msg "$MAGENTA" "║    ALL WORKERS COMPLETE                                  ║"
print_msg "$MAGENTA" "╚══════════════════════════════════════════════════════════╝"
echo ""
print_msg "$GREEN" "  Total time: ${TOTAL_MIN} minutes"
print_msg "$GREEN" "  Cities done: $TOTAL_DONE/$TOTAL_CITIES"
echo ""

# Show send totals
echo "--- Send Totals ---"
for w in 1 2; do
    TOTAL_SENT=$(grep "Sent:" "$LOG_DIR/worker${w}.log" 2>/dev/null | awk '{sum += $2} END {print sum+0}')
    echo "  Worker $w: $TOTAL_SENT total emails sent"
done
echo ""

# Show any failures
echo "--- Failures ---"
grep "FAILED" "$LOG_DIR/worker1.log" "$LOG_DIR/worker2.log" 2>/dev/null || echo "  None!"
echo ""

print_msg "$BLUE" "  Logs:"
print_msg "$BLUE" "    $LOG_DIR/worker1.log"
print_msg "$BLUE" "    $LOG_DIR/worker2.log"
echo ""
