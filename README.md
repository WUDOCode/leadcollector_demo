# LeadCollector

A web scraping and AI classification pipeline that automatically discovers, fetches, and scores news articles as B2B sales leads for **Rail Cargo Group**, a rail freight company. Qualified leads are exported directly into Microsoft Dynamics CRM format.

A benchmark system evaluates multiple local LLMs and prompt strategies against a curated gold dataset of ~500 manually verified leads.

---

## What it does

```
Sources (YAML) → Discover URLs → Fetch HTML → Extract Text → Classify with LLM → Export to CRM
```

1. **Discover** — reads news sources from `registry.yaml` and finds article URLs via RSS, sitemaps, or HTML scraping
2. **Fetch** — downloads raw HTML for each discovered URL
3. **Extract** — parses HTML and extracts clean article text using JSON-LD, paragraph scraping, or Mozilla Readability
4. **Pre-filter** — instantly discards articles with no freight/industry relevance using keyword matching (no LLM needed)
5. **Classify** — sends remaining articles to a local LLM (Ollama) which scores each one 1–10 for rail freight lead potential and extracts company, city, country, description, who, what, when
6. **Review** — sales team reviews leads in the GUI, marks them as `confirmed`, `follow_up`, `contacted`, or `rejected`
7. **Export** — confirmed leads are written into the Dynamics CRM Excel import template

---

## Project structure

```
leadcollector/
├── app/
│   ├── registry.yaml              # list of news sources to scrape
│   ├── models.sql                 # database schema
│   └── src/lc/
│       ├── config.py              # environment variables and paths
│       ├── db.py                  # database helpers
│       ├── utils.py               # URL normalization, hashing
│       ├── prompts.py             # PROMPT_A (conservative) and PROMPT_B (generous)
│       ├── discover.py            # step 1: find article URLs
│       ├── fetch.py               # step 2: download HTML
│       ├── extract.py             # step 3: parse and clean text
│       ├── filters.py             # keyword pre-filter (shared)
│       ├── classify.py            # step 4: LLM lead classification
│       ├── benchmark.py           # benchmark analysis against ground truth
│       ├── import_crm_leads.py    # import leads from CRM Excel export
│       ├── import_monitoring.py   # import leads from monitoring spreadsheet
│       ├── import_more_leads.py   # import leads from More_Leads.docx
│       └── export_leads.py        # export confirmed leads to Excel
├── app_gui_streamlit.py           # web GUI (Pipeline / Dashboard / Leads / Statistics)
├── clean_items.sql                # SQL to remove garbage items before classification
├── docker-compose.yml             # PostgreSQL database
├── requirements.txt               # Python dependencies
└── Lead Import Template Original.xlsx  # Dynamics CRM import template
```

---

## Requirements

- Python 3.12+
- Docker Desktop
- [Ollama](https://ollama.com) (local LLM inference)

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/YOURUSERNAME/leadcollector.git
cd leadcollector
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the database

```bash
docker-compose up -d db
```

### 4. Initialize the database schema

```powershell
Get-Content app/models.sql | docker exec -i leadcollector-db-1 psql -U lc -d lc
```

### 5. Install Ollama and pull models

Download from [ollama.com](https://ollama.com), then pull any of the benchmark models:

```bash
ollama pull llama3.2:3b    # ~2GB
ollama pull qwen2.5:3b     # ~2GB
ollama pull gemma3:4b      # ~3.3GB
ollama pull mistral:7b     # ~4.1GB
```

### 6. Start the GUI

```bash
python -m streamlit run app_gui_streamlit.py
```

Open [http://localhost:8501](http://localhost:8501)

---

## Gold dataset import

The benchmark uses a curated gold dataset of ~500 manually verified leads. Import them in order:

```powershell
$env:PYTHONPATH = "D:\Bachelor\leadcollector\app\src"

# Import CRM-verified leads (includes ground truth labels)
python app/src/lc/import_crm_leads.py --file "All_Leads.xlsx"
python app/src/lc/import_crm_leads.py --file "BD_Leads_View_20_03_26_12-48-40__after_Dez_4_.xlsx"

# Import monitoring spreadsheet (fetches article URLs automatically)
python app/src/lc/import_monitoring.py --file "Monitoring_der_Standorte_Final_Version__news_articles_only_.xlsx"

# Import manually researched leads from docx
python app/src/lc/import_more_leads.py --file "More_Leads.docx"

# Remove garbage items (PDFs, captcha pages, login pages, too-short text)
Get-Content clean_items.sql | docker exec -i leadcollector-db-1 psql -U lc -d lc

# Verify counts
docker exec leadcollector-db-1 psql -U lc -d lc -c "select source_id, count(*) from items group by source_id;"
docker exec leadcollector-db-1 psql -U lc -d lc -c "select label, count(*) from lead_labels group by label;"
```

### Export gold dataset backup

```powershell
docker exec leadcollector-db-1 psql -U lc -d lc -c "copy items to stdout csv header" > gold_items.csv
docker exec leadcollector-db-1 psql -U lc -d lc -c "copy lead_labels to stdout csv header" > gold_labels.csv
docker exec leadcollector-db-1 psql -U lc -d lc -c "copy sources to stdout csv header" > gold_sources.csv
```

### Restore gold dataset

```powershell
"truncate table item_scores, lead_labels restart identity cascade; delete from items; delete from sources;" | docker exec -i leadcollector-db-1 psql -U lc -d lc
docker exec leadcollector-db-1 psql -U lc -d lc -c "copy sources from stdin csv header" < gold_sources.csv
docker exec leadcollector-db-1 psql -U lc -d lc -c "copy items from stdin csv header" < gold_items.csv
docker exec leadcollector-db-1 psql -U lc -d lc -c "copy lead_labels from stdin csv header" < gold_labels.csv
```

---

## Benchmark

The benchmark evaluates 4 models x 2 prompts = 8 combinations against the ground truth labels in `lead_labels`.

### Prompts

Two prompt strategies are defined in `prompts.py`:

| Prompt | Name | Philosophy |
|--------|------|------------|
| `A` | Conservative / Balanced | Warns against over-scoring. Many articles belong in 5-8, not 9. High scores require strong evidence of freight demand. |
| `B` | Generous / Freight-first | Asks if the article creates freight flows. More liberal with high scores for industrial sites. |

### Running all 8 combinations

```powershell
$env:PYTHONPATH = "D:\Bachelor\leadcollector\app\src"

python app/src/lc/classify.py --model llama3.2:3b --prompt A
docker exec leadcollector-db-1 psql -U lc -d lc -c "update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"

python app/src/lc/classify.py --model llama3.2:3b --prompt B
docker exec leadcollector-db-1 psql -U lc -d lc -c "update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"

python app/src/lc/classify.py --model qwen2.5:3b --prompt A
docker exec leadcollector-db-1 psql -U lc -d lc -c "update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"

python app/src/lc/classify.py --model qwen2.5:3b --prompt B
docker exec leadcollector-db-1 psql -U lc -d lc -c "update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"

python app/src/lc/classify.py --model gemma3:4b --prompt A
docker exec leadcollector-db-1 psql -U lc -d lc -c "update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"

python app/src/lc/classify.py --model gemma3:4b --prompt B
docker exec leadcollector-db-1 psql -U lc -d lc -c "update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"

python app/src/lc/classify.py --model mistral:7b --prompt A
docker exec leadcollector-db-1 psql -U lc -d lc -c "update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"

python app/src/lc/classify.py --model mistral:7b --prompt B
```

Each run stores results in `item_scores` with `model` and `prompt_version` columns. The unique constraint is `(item_id, model, prompt_version)` so all 8 runs coexist without overwriting each other.

### Analyzing results

```powershell
python app/src/lc/benchmark.py                  # default threshold=7
python app/src/lc/benchmark.py --threshold 8    # stricter threshold
```

Output is saved to `data/benchmark/benchmark_results.csv`. Results are also visible in the GUI under **Statistics -> Model Benchmark**.

### Exporting all scores

```powershell
docker exec leadcollector-db-1 psql -U lc -d lc -c "copy item_scores to stdout csv header" > all_scores.csv
```

### Benchmark results (threshold >= 7)

| Model | Prompt | Precision | Recall | F1 | Avg Score | High% |
|-------|--------|-----------|--------|----|-----------|-------|
| gemma3:4b | B | 0.302 | 0.957 | **0.459** | 7.11 | 87.2% |
| gemma3:4b | A | 0.291 | 0.787 | 0.425 | 6.68 | 76.7% |
| llama3.2:3b | A | 0.289 | 0.936 | 0.442 | 6.78 | 90.1% |
| llama3.2:3b | B | 0.286 | 0.936 | 0.438 | 8.28 | 90.6% |
| mistral:7b | B | 0.294 | 0.933 | 0.447 | 8.19 | 91.5% |
| mistral:7b | A | 0.292 | 0.745 | 0.419 | 7.19 | 74.5% |
| qwen2.5:3b | B | 0.246 | 0.362 | 0.293 | 4.42 | 32.5% |
| qwen2.5:3b | A | 0.200 | 0.043 | 0.070 | 2.97 | 4.2% |

Ground truth: 172 labeled items (47 relevant, 125 rejected) from CRM data.

**Key findings:**
- Prompt B (generous) consistently outperforms Prompt A across all models
- gemma3:4b + Prompt B achieves the best F1 (0.459) and recall (0.957)
- qwen2.5:3b dramatically underscores and is not suitable for this task
- Precision is similar across all capable models (~0.28-0.30); recall is the main differentiator
- High recall is more important than high precision for lead generation — missing a lead is more costly than reviewing a false positive

---

## Pipeline usage

### Running the scraping pipeline

In the GUI, go to **Pipeline** and click buttons in order, or run manually:

```powershell
python app/src/lc/discover.py app/registry.yaml
python app/src/lc/fetch.py
python app/src/lc/extract.py
python app/src/lc/classify.py --model gemma3:4b --prompt B
```

### Reviewing leads

1. Go to **Leads -> Review & Label**
2. Set the status of each lead (`confirmed`, `follow_up`, `contacted`, `rejected`)
3. Click **Save Labels**

### Exporting to Dynamics CRM

1. In **Leads**, click **Export to Excel**
2. Only `confirmed` leads are exported
3. The file is saved to `data/exports/leads_export_TIMESTAMP.xlsx`
4. Import into Microsoft Dynamics CRM via the Lead Import Template

---

## Lead scoring

The LLM scores each article from 1-10:

| Score | Meaning |
|-------|---------|
| 9-10 | Very strong lead — major industrial site, confirmed freight demand |
| 7-8 | Strong lead — clear physical investment, likely freight demand |
| 5-6 | Moderate — early stage, indirect signal |
| 3-4 | Weak signal — some industry relevance but no clear freight need |
| 1-2 | Not relevant — finance news, management changes, opinion pieces |
| 0 | Filtered by keyword pre-filter — no LLM used |

To change the export threshold:

```powershell
python app/src/lc/export_leads.py --threshold 8
```

---

## Configuration

All settings can be overridden with environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://lc:lc@127.0.0.1:5432/lc` | PostgreSQL connection string — use `127.0.0.1` not `localhost` to avoid IPv6 timeout |
| `LC_DISCOVER_WORKERS` | `5` | Parallel source workers for discovery |
| `LC_FETCH_WORKERS` | `5` | Parallel source workers for fetching |
| `LC_FETCH_PER_SOURCE` | `30` | Max URLs to fetch per source per run |
| `LC_EXTRACT_WORKERS` | `4` | Parallel workers for extraction |
| `LC_EXTRACT_BATCH` | `25` | Batch size for DB writes |

---

## Database

### Reset item scores between benchmark runs

```powershell
docker exec leadcollector-db-1 psql -U lc -d lc -c "update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"
```

### Reset all scores

```powershell
docker exec leadcollector-db-1 psql -U lc -d lc -c "truncate table item_scores restart identity cascade; update items set lead_score=null, lead_reason=null, lead_classified_at=null, classifier_model=null;"
```

### Key tables

| Table | Description |
|-------|-------------|
| `items` | All scraped/imported articles with clean text |
| `item_scores` | One row per article per model per prompt — benchmark results |
| `lead_labels` | Ground truth labels from CRM (confirmed/rejected/follow_up/contacted) |
| `sources` | Configured news sources |
| `url_state` | Discovered URLs and fetch status |
| `leads` | Finalized leads for CRM export |
| `pipeline_runs` | Run statistics for dashboard |

---

## Troubleshooting

**Ollama model not found** — use the exact model name from `ollama list`:
```powershell
ollama list
python app/src/lc/classify.py --model gemma3:4b --prompt B
```

**DB connection slow (130s timeout)** — ensure `DATABASE_URL` uses `127.0.0.1` not `localhost`. Windows resolves `localhost` to IPv6 first, causing a 130-second timeout before falling back to IPv4.

**Classification is slow** — check GPU utilization:
```powershell
nvidia-smi
```
The `num_ctx=2048` setting in classify.py reduces VRAM usage for smaller GPUs (tested on RTX 3080 10GB).

**Encoding issues (ae/oe/ue garbled)** — the import scripts use `r.content` with BeautifulSoup for correct encoding detection. Fix existing garbled items with:
```powershell
docker exec leadcollector-db-1 psql -U lc -d lc -c "UPDATE items SET clean_text = convert_from(convert_to(clean_text, 'latin1'), 'utf8'), title = convert_from(convert_to(title, 'latin1'), 'utf8') WHERE clean_text LIKE '%├%' OR clean_text LIKE '%┼%';"
```

**Export hangs** — make sure `Lead Import Template Original.xlsx` is in the `leadcollector/` root folder.
