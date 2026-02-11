#!/bin/bash

# Real Estate Lead Finder
# Scrapes real estate brokerages/agents from Google Maps for broker preview outreach
# Searches by zip code for both general queries and specific brokerages

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_PATH="$PROJECT_DIR/bin/google-maps-scraper"
RESULTS_DIR="$SCRIPT_DIR/results"
COMPLETED_FILE="$SCRIPT_DIR/completed_zips.txt"
TEMP_QUERIES="$SCRIPT_DIR/.temp_queries_$$.txt"

# General real estate query types
QUERY_TYPES=(
    "real estate agent"
    "real estate broker"
    "realtor"
    "real estate office"
    "real estate agency"
    "property agent"
    "residential real estate"
    "luxury real estate agent"
    "home selling agent"
    "listing agent"
    "buyer agent"
)

# Specific brokerages to target
BROKERAGES=(
    "Keller Williams Realty"
    "Coldwell Banker Realty"
    "Berkshire Hathaway HomeServices"
    "RE/MAX"
    "Compass real estate"
    "eXp Realty"
    "Seven Gables Real Estate"
    "Harvest Realty Development"
    "Sun Cal Real Estate"
    "Juwai IQI"
    "Caimeiju"
    "Homesmart"
    "Modha Realty"
    "AREAA real estate"
    "Century 21"
    "Sotheby's International Realty"
    "Douglas Elliman"
)

# Zip codes
OC_ZIPS=(92618 92602 92612 92614 92782 92660 92603 92625 92683 92843 92844 92840)
LA_ZIPS=()

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_msg() {
    local color=$1
    local msg=$2
    echo -e "${color}${msg}${NC}"
}

# Extract emails from websites
extract_emails_from_websites() {
    local csv_file=$1
    local output_file="${csv_file%.csv}_emails.csv"

    print_msg "$BLUE" "Extracting emails from business websites..."
    print_msg "$YELLOW" "(This may take a few minutes)"
    echo ""

    python3 "$PROJECT_DIR/extract_emails.py" "$csv_file" "$output_file"

    if [[ -f "$output_file" ]]; then
        mv "$output_file" "$csv_file"
    fi
}

usage() {
    echo "Usage: $0 <zip_code|all_oc|all_la|all>"
    echo ""
    echo "Examples:"
    echo "  $0 92618              # Single zip code"
    echo "  $0 all_oc             # All Orange County zips"
    echo "  $0 all_la             # All LA zips"
    echo "  $0 all                # All zips (OC + LA)"
    echo ""
    echo "Options:"
    echo "  --test    Run with only 3 query types + 2 brokerages for quick testing"
    echo "  --force   Skip re-run prompts"
    echo "  --help    Show this help message"
    exit 1
}

check_binary() {
    if [[ ! -f "$BIN_PATH" ]]; then
        print_msg "$RED" "Error: google-maps-scraper binary not found at $BIN_PATH"
        echo ""
        echo "Please download it from:"
        echo "  https://github.com/gosom/google-maps-scraper/releases"
        echo ""
        echo "Then place it in the bin/ folder and make it executable:"
        echo "  chmod +x bin/google-maps-scraper"
        exit 1
    fi

    if [[ ! -x "$BIN_PATH" ]]; then
        print_msg "$YELLOW" "Making binary executable..."
        chmod +x "$BIN_PATH"
    fi
}

check_completed() {
    local zip=$1
    if [[ ! -f "$COMPLETED_FILE" ]]; then
        return 1
    fi
    if grep -q "^${zip}|" "$COMPLETED_FILE" 2>/dev/null; then
        return 0
    fi
    return 1
}

get_completion_date() {
    local zip=$1
    grep "^${zip}|" "$COMPLETED_FILE" 2>/dev/null | tail -1 | cut -d'|' -f2
}

mark_completed() {
    local zip=$1
    local date=$(date '+%Y-%m-%d %H:%M:%S')
    echo "${zip}|${date}" >> "$COMPLETED_FILE"
}

generate_queries() {
    local zip=$1
    local test_mode=$2

    > "$TEMP_QUERIES"

    local qtypes=("${QUERY_TYPES[@]}")
    local broks=("${BROKERAGES[@]}")

    if [[ "$test_mode" == "true" ]]; then
        qtypes=("${QUERY_TYPES[@]:0:3}")
        broks=("${BROKERAGES[@]:0:2}")
    fi

    # General queries by zip code
    for qtype in "${qtypes[@]}"; do
        echo "${qtype} ${zip}" >> "$TEMP_QUERIES"
    done

    # Specific brokerage queries by zip code
    for brok in "${broks[@]}"; do
        echo "${brok} ${zip}" >> "$TEMP_QUERIES"
    done

    local total_queries=$(wc -l < "$TEMP_QUERIES" | tr -d ' ')
    print_msg "$BLUE" "Generated ${total_queries} search queries for zip ${zip}"
}

run_scraper() {
    local zip=$1
    local output_file=$2
    local test_mode=$3

    print_msg "$GREEN" "Starting scraper for zip: ${zip}"
    print_msg "$BLUE" "Output file: ${output_file}"
    echo ""

    local args=(
        "-input" "$TEMP_QUERIES"
        "-results" "$output_file"
        "-depth" "1"
        "-exit-on-inactivity" "3m"
        "-c" "4"
    )

    if [[ "$test_mode" == "true" ]]; then
        print_msg "$YELLOW" "[TEST MODE] Running with reduced queries..."
        args=(
            "-input" "$TEMP_QUERIES"
            "-results" "$output_file"
            "-depth" "1"
            "-exit-on-inactivity" "2m"
            "-c" "2"
        )
    fi

    print_msg "$BLUE" "Running: $BIN_PATH ${args[*]}"
    echo ""

    "$BIN_PATH" "${args[@]}"

    return $?
}

scrape_zip() {
    local zip=$1
    local test_mode=$2
    local force_mode=$3

    # Check if already completed
    if check_completed "$zip"; then
        local completed_date=$(get_completion_date "$zip")
        print_msg "$YELLOW" "⚠ Zip ${zip} was already scraped on ${completed_date}"

        if [[ "$force_mode" == "true" ]]; then
            print_msg "$YELLOW" "  --force: re-running anyway"
        else
            echo ""
            read -p "Re-run zip ${zip}? (y/n): " -n 1 -r
            echo ""
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                print_msg "$BLUE" "Skipping ${zip}."
                return 0
            fi
        fi
    fi

    local output_file="${RESULTS_DIR}/zip_${zip}.csv"

    if [[ -f "$output_file" ]]; then
        if [[ "$force_mode" == "true" ]]; then
            rm -f "$output_file"
        else
            local timestamp=$(date '+%Y%m%d_%H%M%S')
            output_file="${RESULTS_DIR}/zip_${zip}_${timestamp}.csv"
        fi
    fi

    generate_queries "$zip" "$test_mode"

    echo ""
    print_msg "$BLUE" "Query preview (first 5):"
    head -5 "$TEMP_QUERIES" | while read line; do
        echo "  - $line"
    done
    echo "  ..."
    echo ""

    if run_scraper "$zip" "$output_file" "$test_mode"; then
        mark_completed "$zip"

        echo ""
        print_msg "$GREEN" "✓ Scraping complete for zip ${zip}!"

        if [[ -f "$output_file" ]]; then
            extract_emails_from_websites "$output_file"

            local count=$(wc -l < "$output_file" | tr -d ' ')
            count=$((count - 1))

            if [[ $count -gt 0 ]]; then
                print_msg "$GREEN" "  Results: ${output_file}"
                print_msg "$GREEN" "  Leads with emails: ${count}"
            else
                print_msg "$YELLOW" "  No leads with valid emails found."
            fi
        fi
    else
        print_msg "$RED" "✗ Scraping failed for zip ${zip}."
        return 1
    fi

    rm -f "$TEMP_QUERIES"
}

# Main
main() {
    local target=""
    local test_mode="false"
    local force_mode="false"

    while [[ $# -gt 0 ]]; do
        case $1 in
            --test) test_mode="true"; shift ;;
            --force) force_mode="true"; shift ;;
            --help|-h) usage ;;
            *)
                if [[ -z "$target" ]]; then
                    target="$1"
                else
                    print_msg "$RED" "Error: Unexpected argument: $1"
                    usage
                fi
                shift ;;
        esac
    done

    if [[ -z "$target" ]]; then
        print_msg "$RED" "Error: Target argument required"
        usage
    fi

    echo ""
    print_msg "$GREEN" "╔══════════════════════════════════════════════╗"
    print_msg "$GREEN" "║   Real Estate Lead Finder                    ║"
    print_msg "$GREEN" "║   Broker Preview Outreach                    ║"
    print_msg "$GREEN" "╚══════════════════════════════════════════════╝"
    echo ""

    check_binary
    mkdir -p "$RESULTS_DIR"

    # Determine which zips to scrape
    local zips=()

    case "$target" in
        all_oc)
            zips=("${OC_ZIPS[@]}")
            print_msg "$BLUE" "Scraping all ${#zips[@]} Orange County zip codes"
            ;;
        all_la)
            zips=("${LA_ZIPS[@]}")
            print_msg "$BLUE" "Scraping all ${#zips[@]} LA zip codes"
            ;;
        all)
            zips=("${OC_ZIPS[@]}" "${LA_ZIPS[@]}")
            print_msg "$BLUE" "Scraping all ${#zips[@]} zip codes (OC + LA)"
            ;;
        *)
            zips=("$target")
            print_msg "$BLUE" "Scraping zip code: $target"
            ;;
    esac

    echo ""

    local total_zips=${#zips[@]}
    local completed=0
    local failed=0

    for zip in "${zips[@]}"; do
        echo ""
        print_msg "$GREEN" "─── Zip ${zip} ($(($completed + 1))/${total_zips}) ───────────────"

        if scrape_zip "$zip" "$test_mode" "$force_mode"; then
            completed=$((completed + 1))
        else
            failed=$((failed + 1))
        fi
    done

    echo ""
    print_msg "$GREEN" "════════════════════════════════════════════════"
    print_msg "$GREEN" "  ALL DONE!"
    print_msg "$GREEN" "  Completed: ${completed}/${total_zips}"
    if [[ $failed -gt 0 ]]; then
        print_msg "$RED" "  Failed: ${failed}"
    fi
    print_msg "$GREEN" "  Results in: ${RESULTS_DIR}/"
    print_msg "$GREEN" "════════════════════════════════════════════════"
}

main "$@"
