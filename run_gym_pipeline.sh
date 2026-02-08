#!/bin/bash

# Vora Gym Pipeline - Scrape, Enrich, Send for all completed cities
# Processes one city at a time: scrape → enrich → send live → next city

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results/gyms"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
NC='\033[0m'

print_msg() {
    echo -e "${1}${2}${NC}"
}

# All 19 cities already completed for clinic outreach
CITIES=(
    "Irvine, CA"
    "Aliso Viejo, CA"
    "Anaheim, CA"
    "Brea, CA"
    "Buena Park, CA"
    "Costa Mesa, CA"
    "Cypress, CA"
    "Dana Point, CA"
    "Fountain Valley, CA"
    "Fullerton, CA"
    "Garden Grove, CA"
    "Huntington Beach, CA"
    "La Habra, CA"
    "La Palma, CA"
    "Laguna Beach, CA"
    "Laguna Hills, CA"
    "Laguna Niguel, CA"
    "Laguna Woods, CA"
    "Lake Forest, CA"
)

# Convert city to filename
city_to_filename() {
    echo "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/_/g' | sed 's/__*/_/g' | sed 's/^_//;s/_$//'
}

TOTAL=${#CITIES[@]}
COMPLETED=0
FAILED_CITIES=()

echo ""
print_msg "$MAGENTA" "╔══════════════════════════════════════════════════════════╗"
print_msg "$MAGENTA" "║         VORA GYM & FITNESS PIPELINE                     ║"
print_msg "$MAGENTA" "║         $TOTAL cities to process                             ║"
print_msg "$MAGENTA" "╚══════════════════════════════════════════════════════════╝"
echo ""

START_TIME=$(date +%s)

for i in "${!CITIES[@]}"; do
    city="${CITIES[$i]}"
    city_num=$((i + 1))
    filename=$(city_to_filename "$city")
    raw_csv="$RESULTS_DIR/${filename}.csv"
    enriched_csv="$RESULTS_DIR/${filename}_enriched.csv"

    echo ""
    print_msg "$MAGENTA" "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_msg "$MAGENTA" "  CITY $city_num/$TOTAL: $city"
    print_msg "$MAGENTA" "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""

    # ── Step 1: Scrape ──────────────────────────────────────────
    # Check if already scraped (CSV exists with data)
    if [[ -f "$raw_csv" ]] && [[ $(wc -l < "$raw_csv" | tr -d ' ') -gt 1 ]]; then
        line_count=$(wc -l < "$raw_csv" | tr -d ' ')
        lead_count=$((line_count - 1))
        print_msg "$YELLOW" "  [SCRAPE] Already have $lead_count leads in $raw_csv, skipping scrape"
    else
        print_msg "$BLUE" "  [SCRAPE] Starting scrape for $city..."
        CITY_START=$(date +%s)

        if bash "$SCRIPT_DIR/find_gym_leads.sh" "$city"; then
            CITY_END=$(date +%s)
            CITY_DURATION=$(( CITY_END - CITY_START ))
            print_msg "$GREEN" "  [SCRAPE] Done in ${CITY_DURATION}s"
        else
            print_msg "$RED" "  [SCRAPE] FAILED for $city, skipping to next city"
            FAILED_CITIES+=("$city (scrape failed)")
            continue
        fi
    fi

    # Verify CSV exists
    if [[ ! -f "$raw_csv" ]]; then
        print_msg "$RED" "  [ERROR] No CSV found at $raw_csv after scrape, skipping"
        FAILED_CITIES+=("$city (no CSV)")
        continue
    fi

    lead_count=$(( $(wc -l < "$raw_csv" | tr -d ' ') - 1 ))
    if [[ $lead_count -le 0 ]]; then
        print_msg "$YELLOW" "  [SKIP] No leads found for $city, moving on"
        continue
    fi

    print_msg "$GREEN" "  [SCRAPE] $lead_count leads found"

    # ── Step 2: Enrich ──────────────────────────────────────────
    if [[ -f "$enriched_csv" ]] && [[ $(wc -l < "$enriched_csv" | tr -d ' ') -gt 1 ]]; then
        enriched_count=$(( $(wc -l < "$enriched_csv" | tr -d ' ') - 1 ))
        print_msg "$YELLOW" "  [ENRICH] Already have $enriched_count enriched leads, skipping enrich"
    else
        print_msg "$BLUE" "  [ENRICH] Enriching $lead_count leads..."

        if "$VENV_PYTHON" "$SCRIPT_DIR/gym_outreach.py" "$raw_csv" --enrich; then
            enriched_count=$(( $(wc -l < "$enriched_csv" | tr -d ' ') - 1 ))
            print_msg "$GREEN" "  [ENRICH] Done. $enriched_count enriched leads"
        else
            print_msg "$RED" "  [ENRICH] FAILED for $city, skipping to next city"
            FAILED_CITIES+=("$city (enrich failed)")
            continue
        fi
    fi

    # ── Step 3: Send Live ───────────────────────────────────────
    print_msg "$BLUE" "  [SEND] Sending live emails for $city..."

    if "$VENV_PYTHON" "$SCRIPT_DIR/gym_outreach.py" "$enriched_csv" --live; then
        print_msg "$GREEN" "  [SEND] Done for $city!"
    else
        print_msg "$RED" "  [SEND] FAILED for $city"
        FAILED_CITIES+=("$city (send failed)")
        continue
    fi

    COMPLETED=$((COMPLETED + 1))

    # Progress update
    echo ""
    ELAPSED=$(( $(date +%s) - START_TIME ))
    ELAPSED_MIN=$(( ELAPSED / 60 ))
    print_msg "$GREEN" "  Progress: $COMPLETED/$TOTAL cities completed (${ELAPSED_MIN}m elapsed)"

    # Brief pause between cities
    sleep 2
done

# ── Final Summary ───────────────────────────────────────────────
END_TIME=$(date +%s)
TOTAL_DURATION=$(( END_TIME - START_TIME ))
TOTAL_MIN=$(( TOTAL_DURATION / 60 ))
TOTAL_SEC=$(( TOTAL_DURATION % 60 ))

echo ""
echo ""
print_msg "$MAGENTA" "╔══════════════════════════════════════════════════════════╗"
print_msg "$MAGENTA" "║         PIPELINE COMPLETE                               ║"
print_msg "$MAGENTA" "╚══════════════════════════════════════════════════════════╝"
echo ""
print_msg "$GREEN" "  Cities completed: $COMPLETED/$TOTAL"
print_msg "$BLUE" "  Total time: ${TOTAL_MIN}m ${TOTAL_SEC}s"

if [[ ${#FAILED_CITIES[@]} -gt 0 ]]; then
    echo ""
    print_msg "$RED" "  Failed cities:"
    for fc in "${FAILED_CITIES[@]}"; do
        print_msg "$RED" "    - $fc"
    done
fi

echo ""
