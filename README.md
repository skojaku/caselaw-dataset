# caselaw-dataset

Snakemake pipeline that preprocesses raw caselaw data into structured tables for citation network analysis.

## Outputs

The pipeline produces two sets of outputs:

- **`preprocessed_unfiltered/`** — all cases
- **`preprocessed/`** — filtered to the largest weakly connected component (LCC) of the citation network, with contiguous re-indexed IDs

| File | Description |
|------|-------------|
| `paper_table.csv` | Case metadata (paper_id, opinion_id, title, year, date, venue, venueType, ...) |
| `citation_net.npz` | Citation network (scipy sparse CSR matrix); `net[i,j]=1` means case i cites case j |
| `court_table.csv` | Court hierarchy (venue, parent, venueType: Supreme/Appeals/District) |
| `category_table.csv` | Category labels derived from court hierarchy |
| `paper_category_table.csv` | Case–category assignments (main_class_id, sub_class_id) |

In the **filtered** output, all IDs are re-mapped to contiguous ranges starting from 0.

## Raw Data

Three JSON files are required (set `raw_dir` in `config.yaml`):

| File | Description |
|------|-------------|
| `Legal_Citation_Dict.json` | Citation links: `{cited_id: [citing_id, ...]}` |
| `Citation_Info_Dict.json` | Case metadata keyed by opinion ID |
| `court_hierarchy.json` | Court hierarchy tree |

## Setup

### 1. Install dependencies

```bash
pip install numpy scipy pandas networkx ujson tqdm snakemake
```

### 2. Configure paths

Edit `config.yaml`:

```yaml
raw_dir: "/path/to/raw/caselaw"
output_dir: "/path/to/output/preprocessed"
```

## Usage

```bash
# Dry run
snakemake -n

# Run full pipeline
snakemake --cores all
```

## Pipeline

```
Raw JSON files
  → build_citation_net     Parse citations and case metadata → citation_net.npz, paper_table.csv, court_table.csv
  → build_category_table   Map courts to category hierarchy → category_table.csv, paper_category_table.csv
  → filter_to_lcc          Filter to LCC, re-index all IDs, write final outputs
```

**build_citation_net** parses `Legal_Citation_Dict.json` and `Citation_Info_Dict.json`, assigns contiguous `paper_id`s, constructs a scipy sparse CSR citation matrix, and extracts the court hierarchy tree.

**build_category_table** maps each case to its court circuit (sub-category) and court level (Supreme/Appeals/District as main category).

**filter_to_lcc** finds the largest weakly connected component, re-maps `paper_id`s to contiguous integers, and rebuilds all output tables.
