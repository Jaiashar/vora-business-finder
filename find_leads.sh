#!/bin/bash

# Vora Business Finder
# Scrapes health/mobility clinics from Google Maps for B2B outreach

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_PATH="$SCRIPT_DIR/bin/google-maps-scraper"
RESULTS_DIR="$SCRIPT_DIR/results"
COMPLETED_FILE="$SCRIPT_DIR/completed_cities.txt"
TEMP_QUERIES="$SCRIPT_DIR/.temp_queries_$$.txt"

# Business types to search for
BUSINESS_TYPES=(
    "physical therapy clinic"
    "rehabilitation center"
    "orthopedic clinic"
    "sports medicine clinic"
    "chiropractic clinic"
    "pain management clinic"
    "wellness center"
    "occupational therapy clinic"
    "integrative medicine clinic"
    "outpatient rehabilitation"
    "physical medicine clinic"
    "musculoskeletal clinic"
    "spine clinic"
    "joint clinic"
    "mobility specialist"
    "movement disorder clinic"
    "neurological rehabilitation"
    "post surgical rehabilitation"
    "chronic pain clinic"
    "functional medicine clinic"
    "holistic health clinic"
    "preventive medicine clinic"
    "lifestyle medicine clinic"
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored message
print_msg() {
    local color=$1
    local msg=$2
    echo -e "${color}${msg}${NC}"
}

# Extract emails from websites using our custom extractor
extract_emails_from_websites() {
    local csv_file=$1
    local output_file="${csv_file%.csv}_emails.csv"
    
    print_msg "$BLUE" "Extracting emails from business websites..."
    print_msg "$YELLOW" "(This may take a few minutes)"
    echo ""
    
    python3 "$SCRIPT_DIR/extract_emails.py" "$csv_file" "$output_file"
    
    # Replace original with emails-only version
    if [[ -f "$output_file" ]]; then
        mv "$output_file" "$csv_file"
    fi
}

# Print usage
usage() {
    echo "Usage: $0 <city>"
    echo ""
    echo "Examples:"
    echo "  $0 \"Austin, TX\""
    echo "  $0 \"Denver, CO\""
    echo "  $0 \"New York, NY\""
    echo ""
    echo "Options:"
    echo "  --test    Run with only 2 business types for quick testing"
    echo "  --force   Skip re-run prompts (for automation/parallel use)"
    echo "  --help    Show this help message"
    exit 1
}

# Check if binary exists
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

# Convert city to filename-safe format
city_to_filename() {
    local city=$1
    echo "$city" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/_/g' | sed 's/__*/_/g' | sed 's/^_//;s/_$//'
}

# Check if city was already scraped
check_completed() {
    local city=$1
    
    if [[ ! -f "$COMPLETED_FILE" ]]; then
        return 1  # Not completed (file doesn't exist)
    fi
    
    # Check if city exists in completed file (case-insensitive)
    if grep -qi "^${city}|" "$COMPLETED_FILE" 2>/dev/null; then
        return 0  # Already completed
    fi
    
    return 1  # Not completed
}

# Get completion date for a city
get_completion_date() {
    local city=$1
    grep -i "^${city}|" "$COMPLETED_FILE" 2>/dev/null | tail -1 | cut -d'|' -f2
}

# Mark city as completed
mark_completed() {
    local city=$1
    local date=$(date '+%Y-%m-%d %H:%M:%S')
    echo "${city}|${date}" >> "$COMPLETED_FILE"
}

# Generate query file for a city
generate_queries() {
    local city=$1
    local test_mode=$2
    
    # Clear temp file
    > "$TEMP_QUERIES"
    
    local types_to_use=("${BUSINESS_TYPES[@]}")
    
    # In test mode, only use first 2 types
    if [[ "$test_mode" == "true" ]]; then
        types_to_use=("${BUSINESS_TYPES[@]:0:2}")
    fi
    
    # Convert city format: "Austin, TX" -> "Austin TX" (simpler format works better)
    local city_simple=$(echo "$city" | sed 's/,//g')
    
    for btype in "${types_to_use[@]}"; do
        echo "${btype} ${city_simple}" >> "$TEMP_QUERIES"
    done
    
    print_msg "$BLUE" "Generated ${#types_to_use[@]} search queries for ${city}"
}

# Run the scraper
run_scraper() {
    local city=$1
    local output_file=$2
    local test_mode=$3
    
    print_msg "$GREEN" "Starting scraper for: ${city}"
    print_msg "$BLUE" "Output file: ${output_file}"
    echo ""
    
    # Scraper arguments - NO email flag (we extract emails separately for reliability)
    local args=(
        "-input" "$TEMP_QUERIES"
        "-results" "$output_file"
        "-depth" "1"
        "-exit-on-inactivity" "3m"
        "-c" "4"
    )
    
    if [[ "$test_mode" == "true" ]]; then
        print_msg "$YELLOW" "[TEST MODE] Running with 2 business types..."
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
    
    # Run scraper
    "$BIN_PATH" "${args[@]}"
    
    return $?
}

# Main function
main() {
    local city=""
    local test_mode="false"
    local force_mode="false"
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --test)
                test_mode="true"
                shift
                ;;
            --force)
                force_mode="true"
                shift
                ;;
            --help|-h)
                usage
                ;;
            *)
                if [[ -z "$city" ]]; then
                    city="$1"
                else
                    print_msg "$RED" "Error: Unexpected argument: $1"
                    usage
                fi
                shift
                ;;
        esac
    done
    
    # Validate city argument
    if [[ -z "$city" ]]; then
        print_msg "$RED" "Error: City argument is required"
        usage
    fi
    
    echo ""
    print_msg "$GREEN" "╔══════════════════════════════════════════╗"
    print_msg "$GREEN" "║       Vora Business Finder               ║"
    print_msg "$GREEN" "╚══════════════════════════════════════════╝"
    echo ""
    
    # Check for binary
    check_binary
    
    # Check if already completed
    if check_completed "$city"; then
        local completed_date=$(get_completion_date "$city")
        print_msg "$YELLOW" "⚠ ${city} was already scraped on ${completed_date}"
        
        if [[ "$force_mode" == "true" ]]; then
            print_msg "$YELLOW" "  --force: re-running anyway"
        else
            echo ""
            read -p "Do you want to re-run? (y/n): " -n 1 -r
            echo ""
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                print_msg "$BLUE" "Skipping. Use a different city or delete the entry from completed_cities.txt"
                exit 0
            fi
        fi
        echo ""
    fi
    
    # Generate output filename
    local filename=$(city_to_filename "$city")
    local output_file="${RESULTS_DIR}/${filename}.csv"
    
    # If re-running and file exists, overwrite in force mode, otherwise timestamp
    if [[ -f "$output_file" ]]; then
        if [[ "$force_mode" == "true" ]]; then
            rm -f "$output_file"
        else
            local timestamp=$(date '+%Y%m%d_%H%M%S')
            output_file="${RESULTS_DIR}/${filename}_${timestamp}.csv"
        fi
    fi
    
    # Ensure results directory exists
    mkdir -p "$RESULTS_DIR"
    
    # Generate queries
    generate_queries "$city" "$test_mode"
    
    # Show query preview
    echo ""
    print_msg "$BLUE" "Query preview (first 3):"
    head -3 "$TEMP_QUERIES" | while read line; do
        echo "  - $line"
    done
    echo "  ..."
    echo ""
    
    # Run scraper
    if run_scraper "$city" "$output_file" "$test_mode"; then
        # Mark as completed
        mark_completed "$city"
        
        echo ""
        print_msg "$GREEN" "✓ Scraping complete!"
        
        # Post-process: extract emails from websites
        if [[ -f "$output_file" ]]; then
            extract_emails_from_websites "$output_file"
            
            # Show final count (after filtering)
            local count=$(wc -l < "$output_file" | tr -d ' ')
            count=$((count - 1))  # Subtract header row
            
            if [[ $count -gt 0 ]]; then
                print_msg "$GREEN" "  Results saved to: ${output_file}"
                print_msg "$GREEN" "  Leads with valid emails: ${count}"
            else
                print_msg "$YELLOW" "  No leads with valid emails found."
                print_msg "$YELLOW" "  Try a different city or check if the scraper ran correctly."
            fi
        fi
    else
        print_msg "$RED" "✗ Scraping failed. Check the error messages above."
        exit 1
    fi
    
    # Cleanup temp file
    rm -f "$TEMP_QUERIES"
}

# Run main
main "$@"
