# Vora Business Finder

A simple tool to find health and mobility clinics for B2B outreach. Uses Google Maps to extract business information including emails.

## Quick Start

```bash
# 1. Download the scraper binary (one-time setup)
# Go to: https://github.com/gosom/google-maps-scraper/releases
# Download the macOS binary and place in bin/ folder

# 2. Make files executable
chmod +x find_leads.sh
chmod +x bin/google-maps-scraper

# 3. Run for a city
./find_leads.sh "Austin, TX"
```

## Usage

### Basic Usage

```bash
./find_leads.sh "City, State"
```

### Examples

```bash
./find_leads.sh "Austin, TX"
./find_leads.sh "Denver, CO"
./find_leads.sh "New York, NY"
./find_leads.sh "Los Angeles, CA"
```

### Test Mode

Run with only 2 business types to quickly verify everything works:

```bash
./find_leads.sh --test "Austin, TX"
```

## What It Searches For

The tool searches for 23 different types of health/mobility businesses:

- Physical therapy clinic
- Rehabilitation center
- Orthopedic clinic
- Sports medicine clinic
- Chiropractic clinic
- Pain management clinic
- Wellness center
- Occupational therapy clinic
- Integrative medicine clinic
- Outpatient rehabilitation
- Physical medicine clinic
- Musculoskeletal clinic
- Spine clinic
- Joint clinic
- Mobility specialist
- Movement disorder clinic
- Neurological rehabilitation
- Post surgical rehabilitation
- Chronic pain clinic
- Functional medicine clinic
- Holistic health clinic
- Preventive medicine clinic
- Lifestyle medicine clinic

## Output

Results are saved to `results/<city>.csv` with columns:

| Column | Description |
|--------|-------------|
| title | Business name |
| address | Full address |
| phone | Phone number |
| website | Business website |
| emails | Extracted email addresses |
| category | Business type |
| review_rating | Star rating |
| review_count | Number of reviews |
| link | Google Maps URL |

## Tracking Completed Cities

The tool tracks which cities have been scraped in `completed_cities.txt`. 

If you try to scrape a city again, it will ask if you want to re-run:

```
⚠ Austin, TX was already scraped on 2026-02-05 14:30:00
Do you want to re-run? (y/n):
```

## File Structure

```
VoraBusinessFinder/
├── bin/
│   └── google-maps-scraper    # Downloaded binary
├── results/
│   ├── austin_tx.csv
│   ├── denver_co.csv
│   └── ...
├── completed_cities.txt       # Tracking file
├── find_leads.sh              # Main script
└── README.md
```

## Performance

| Metric | Value |
|--------|-------|
| Queries per city | 23 |
| Results per query | ~20 |
| Leads per city | ~460 max |
| Time per city (no email) | ~5-10 minutes |
| Time per city (with email) | ~15-25 minutes |

## Troubleshooting

### "Binary not found" error

Download the scraper from:
https://github.com/gosom/google-maps-scraper/releases

Place it in the `bin/` folder and run:
```bash
chmod +x bin/google-maps-scraper
```

### "Permission denied" error

Make the scripts executable:
```bash
chmod +x find_leads.sh
chmod +x bin/google-maps-scraper
```

### Scraper exits immediately

Make sure you have a stable internet connection. The scraper needs to load Google Maps pages.

### No emails in results

Email extraction visits each business website, which takes time. Make sure the scraper runs long enough to complete.

## Tips

1. **Start small**: Use `--test` flag first to verify setup
2. **One city at a time**: Let each city complete before starting another
3. **Check results**: Open the CSV to verify data quality before scaling up
4. **Proxies**: For large-scale scraping, consider using proxies (see scraper docs)
