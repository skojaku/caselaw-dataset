# caselaw-dataset

Snakemake pipeline that downloads [CourtListener](https://www.courtlistener.com/) bulk data from S3 and preprocesses it into structured tables for citation network analysis.

No PostgreSQL required — the pipeline downloads CSV snapshots and converts them to JSON, then builds the final outputs.

## Outputs

The pipeline produces two sets of outputs:

- **`preprocessed_unfiltered/`** — all cases
- **`preprocessed/`** — filtered to the largest weakly connected component (LCC) of the citation network, with contiguous re-indexed IDs

| File | Description |
|------|-------------|
| `paper_table.csv` | Case metadata (paper_id, opinion_id, date, year, venue, venueType, case_name, ...) |
| `citation_net.npz` | Citation network (scipy sparse CSR matrix); `net[i,j]=1` means case i cites case j |
| `court_table.csv` | Court hierarchy (venue, parent, venueType: Supreme/Appeals/District) |
| `category_table.csv` | Category labels derived from court hierarchy |
| `paper_category_table.csv` | Case–category assignments (main_class_id, sub_class_id) |

In the **filtered** output, all IDs are re-mapped to contiguous ranges starting from 0.

## Setup

### 1. Install dependencies

```bash
pip install numpy scipy pandas polars networkx ujson tqdm snakemake
pip install awscli   # for downloading from S3
```

### 2. Configure paths

Edit `config.yaml`:

```yaml
raw_dir: "/path/to/raw/caselaw"          # where to download CourtListener CSVs
json_dir: "/path/to/raw/caselaw/json"    # where to write converted JSON files
output_dir: "/path/to/output/preprocessed"

bulk_data_date: "2026-03-31"  # snapshot date (quarterly releases)
```

Available snapshot dates are listed at the [CourtListener bulk data page](https://wiki.free.law/c/courtlistener/help/api/bulk-data/bulk-legal-data).

## Usage

```bash
# Dry run
snakemake -n

# Run full pipeline
snakemake --cores all

# Download CSVs only
snakemake download

# Convert CSVs to JSON only (after download)
snakemake convert_to_json
```

## Pipeline

```
S3 (com-courtlistener-storage)
  → download_csv           Download citation-map, opinions, opinion-clusters,
                           dockets, courts, court-appeals-to  (.csv.bz2 → .csv)
  → convert_to_json        Convert CSVs to three JSON files:
                             Legal_Citation_Dict.json
                             Citation_Info_Dict.json
                             court_hierarchy.json
  → build_citation_net     Parse JSON → citation_net.npz, paper_table.csv, court_table.csv
  → build_category_table   Map courts to category hierarchy → category_table.csv, paper_category_table.csv
  → filter_to_lcc          Filter to LCC, re-index all IDs, write final outputs
```

### JSON file formats

**`Legal_Citation_Dict.json`**
```json
{"cited_opinion_id": [citing_opinion_id, ...], ...}
```

**`Citation_Info_Dict.json`**
```json
{"opinion_id": {"date": "YYYY-MM-DD", "court": "Court Name", "case_name": "...", "case_name_full": "..."}, ...}
```

**`court_hierarchy.json`**
```json
[
  ["Supreme Court of the United States"],
  ["Court of Appeals for the First Circuit", "District Court, D. Maine", ...],
  ...
]
```

**`build_citation_net`** parses the JSON files, assigns contiguous `paper_id`s, constructs a scipy sparse CSR citation matrix, and extracts the court hierarchy tree.

**`build_category_table`** maps each case to its court circuit (sub-category) and court level (Supreme/Appeals/District as main category).

**`filter_to_lcc`** finds the largest weakly connected component, re-maps `paper_id`s to contiguous integers, and rebuilds all output tables.
