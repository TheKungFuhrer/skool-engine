# Skool Engine

Classifies ~1,400 members of the "Six-Figure Venue Engine" Skool community into four categories:
- **active_venue_owner** - currently owns/operates a wedding or event venue
- **aspiring_venue_owner** - wants to open a venue but doesn't have one yet
- **service_provider** - event professionals (planners, decorators, DJs, photographers, caterers, florists, etc.)
- **other** - not related to the events/venue industry

## Data Pipeline

1. **Merge** (`node scripts/merge.js`) - Joins Skoot CRM CSV export with Inbox Insights XLSX (DM history) by member name → `data/merged/members.json`
2. **Classify** (`python enrichment/classify_batch.py`) - Sends merged profiles to Claude Haiku via Anthropic Batch API → `data/classified/classified_members.json`
3. **Export** (`node scripts/export.js`) - Converts classified JSON to CSV → `data/classified/classified_members.csv`

## Data Sources (data/imports/)

- `export_plus_active_20251205_235149.csv` - Skoot CRM export with member profiles (name, email, bio, location, survey answers, social links, ACE scores)
- `Inbox Insights.xlsx` - DM conversation history from Skool inbox

## Environment

- `ANTHROPIC_API_KEY_BATCH` in `.env` - API key for Anthropic Batch API
- Node.js with `xlsx`, `csv-parse`, `csv-stringify`
- Python with `anthropic`, `python-dotenv`

## Commands

```bash
npm run merge      # Step 1: merge data sources
npm run classify   # Step 2: batch classify with Haiku
npm run export     # Step 3: export results to CSV
npm run pipeline   # Run all steps in sequence
```
