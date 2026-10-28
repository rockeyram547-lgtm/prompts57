# Faymas prompt scraper

Scrapes every prompt from https://faymas.in and saves CSV files per faymas.in category, 100 prompts per file:

    output/photography/photography_1-100.csv, photography_101-200.csv, ...

## On GitHub

Actions tab -> "Scrape Faymas prompts" -> Run workflow, then pick a mode:
- **explore**: diagnostics only (categories found, does scrolling load more, does parsing work). Send the log if anything looks wrong.
- **test**: about 20 prompts.
- **full**: everything. Each run works up to ~5 hours, saves progress, and starts the next run by itself until done.

Every Monday it also looks for newly published prompts.

## Making sure nothing is missing

- `data/discovery_report.json`: prompts found per category, and any prompt seen on the home page but in no category.
- `data/remaining_urls.txt`: prompts that could not be scraped yet (exists only while something is missing).
- Compare the per-category counts with the site.

## Locally

    pip install -r requirements.txt
    playwright install chromium
    python scrape_faymas.py --explore
    python scrape_faymas.py --render

Flags: `--workers`, `--delay`, `--rediscover`, `--category <slug>`, `--build-only`, `--flatten`, `--inspect <url> --render`.

Check the site's terms before republishing prompts publicly; they belong to their creators.

## If the site blocks the scraper

The script watches for rate limits (HTTP 429), refusals (403/503) and bot-check pages. When it sees them it
slows down, pauses (honouring `Retry-After`), remembers the slower pace for the next run, and if the site keeps
refusing it stops cleanly and saves progress. It does **not** try to defeat a block (no CAPTCHA solving, fake
fingerprints or IP rotation). Your options:

1. Wait a few hours and run again. It resumes where it stopped.
2. Run locally from your own connection: `python scrape_faymas.py --render --workers 1 --delay 3`
3. `python scrape_faymas.py --warmup` opens a real window. Pass any check or login by hand, then reuse that
   session: `python scrape_faymas.py --render --storage-state data/browser_state.json` (never commit that file).
4. Save pages yourself (browser "Save page as", or the SingleFile extension) and load them:
   `python scrape_faymas.py --import-html saved_pages/`. Use `data/remaining_urls.txt` to see which ones.
5. Ask faymas.in for a data export or written permission. This is the most reliable way to get all 22,000+.

Other files: `data/gone_urls.txt` (pages that return 404 twice, so they are not counted as missing) and
`--try-http-first` (a cheap plain request before the browser, if prompts are in the page source).
Running `explore` also saves any JSON replies the site's own pages load to `data/explore/api_*.json`.

