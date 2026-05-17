"""
Caselaw Dataset Pipeline

Downloads CourtListener bulk CSV data from S3 and preprocesses it into
structured tables (cases, judges, citations, court categories).

Two output directories:
  - preprocessed_unfiltered/: all cases
  - preprocessed/: filtered to largest weakly connected component only

Usage:
    snakemake --cores all     # Run full pipeline
    snakemake -n              # Dry run
    snakemake download        # Download CSVs only
    snakemake convert_to_json # Convert CSVs to JSON only
"""

from os.path import join as j

configfile: "config.yaml"

# =============================================================================
# Directories
# =============================================================================

RAW_DIR = config["raw_dir"]         # CourtListener bulk CSVs go here
JSON_DIR = config.get("json_dir", j(RAW_DIR, "json"))  # converted JSON files
OUTPUT_DIR = config["output_dir"]
UNFILTERED_DIR = OUTPUT_DIR + "_unfiltered"

# =============================================================================
# Bulk data snapshot date (set in config.yaml)
# =============================================================================

DATE = config.get("bulk_data_date", "2026-03-31")
S3_BASE = "s3://com-courtlistener-storage/bulk-data"

# =============================================================================
# Raw CSV inputs (downloaded from S3)
# =============================================================================

CITATION_MAP_CSV = j(RAW_DIR, f"citation-map-{DATE}.csv")
OPINIONS_CSV = j(RAW_DIR, f"opinions-{DATE}.csv")
CLUSTERS_CSV = j(RAW_DIR, f"opinion-clusters-{DATE}.csv")
DOCKETS_CSV = j(RAW_DIR, f"dockets-{DATE}.csv")
COURTS_CSV = j(RAW_DIR, f"courts-{DATE}.csv")
COURT_APPEALS_TO_CSV = j(RAW_DIR, f"court-appeals-to-{DATE}.csv")

# =============================================================================
# Converted JSON files (input to the build pipeline)
# =============================================================================

CITATION_DICT_JSON = j(JSON_DIR, "Legal_Citation_Dict.json")
INFO_DICT_JSON = j(JSON_DIR, "Citation_Info_Dict.json")
COURT_HIERARCHY_JSON = j(JSON_DIR, "court_hierarchy.json")

# =============================================================================
# Unfiltered outputs
# =============================================================================

UF_CITATION_NET = j(UNFILTERED_DIR, "citation_net.npz")
UF_PAPER_TABLE = j(UNFILTERED_DIR, "paper_table.csv")
UF_COURT_TABLE = j(UNFILTERED_DIR, "court_table.csv")
UF_CATEGORY_TABLE = j(UNFILTERED_DIR, "category_table.csv")
UF_PAPER_CATEGORY_TABLE = j(UNFILTERED_DIR, "paper_category_table.csv")

# =============================================================================
# Final filtered outputs
# =============================================================================

CITATION_NET = j(OUTPUT_DIR, "citation_net.npz")
PAPER_TABLE = j(OUTPUT_DIR, "paper_table.csv")
COURT_TABLE = j(OUTPUT_DIR, "court_table.csv")
CATEGORY_TABLE = j(OUTPUT_DIR, "category_table.csv")
PAPER_CATEGORY_TABLE = j(OUTPUT_DIR, "paper_category_table.csv")

# =============================================================================
# Rules
# =============================================================================

rule all:
    input:
        CITATION_NET,
        PAPER_TABLE,
        COURT_TABLE,
        CATEGORY_TABLE,
        PAPER_CATEGORY_TABLE,


# -----------------------------------------------------------------------------
# Download
# -----------------------------------------------------------------------------

rule download:
    """Download all required CourtListener bulk CSVs from S3."""
    input:
        CITATION_MAP_CSV,
        OPINIONS_CSV,
        CLUSTERS_CSV,
        DOCKETS_CSV,
        COURTS_CSV,
        COURT_APPEALS_TO_CSV,


def s3_download(wildcards):
    """Return the S3 path for a given local CSV filename."""
    filename = wildcards.filename
    return f"{S3_BASE}/{filename}.bz2"


rule download_csv:
    output:
        j(RAW_DIR, "{filename}.csv"),
    params:
        s3_path = lambda wc: f"{S3_BASE}/{wc.filename}.bz2",
    shell:
        "aws s3 cp '{params.s3_path}' - --no-sign-request | bzcat > '{output}'"


# -----------------------------------------------------------------------------
# Convert CSVs → JSON
# -----------------------------------------------------------------------------

rule convert_to_json:
    """Convert CourtListener CSV bulk data to the three JSON files."""
    input:
        citation_map_file = CITATION_MAP_CSV,
        opinions_file = OPINIONS_CSV,
        clusters_file = CLUSTERS_CSV,
        dockets_file = DOCKETS_CSV,
        courts_file = COURTS_CSV,
        court_appeals_to_file = COURT_APPEALS_TO_CSV,
    output:
        citation_dict = CITATION_DICT_JSON,
        info_dict = INFO_DICT_JSON,
        court_hierarchy = COURT_HIERARCHY_JSON,
    script:
        "scripts/convert_to_json.py"


# -----------------------------------------------------------------------------
# Build (from JSON)
# -----------------------------------------------------------------------------

rule build_citation_net:
    input:
        node_data_file = INFO_DICT_JSON,
        link_data_file = CITATION_DICT_JSON,
        court_data_file = COURT_HIERARCHY_JSON,
    output:
        net_file = UF_CITATION_NET,
        node_table_file = UF_PAPER_TABLE,
        court_table_file = UF_COURT_TABLE,
    script:
        "scripts/build_citation_net.py"


rule build_category_table:
    input:
        input_file = UF_PAPER_TABLE,
        court_hierarchy_file = COURT_HIERARCHY_JSON,
    output:
        output_paper_category_table_file = UF_PAPER_CATEGORY_TABLE,
        output_category_table_file = UF_CATEGORY_TABLE,
    script:
        "scripts/build_category_table.py"


rule filter_to_lcc:
    input:
        net_file = UF_CITATION_NET,
        paper_table_file = UF_PAPER_TABLE,
        paper_category_table_file = UF_PAPER_CATEGORY_TABLE,
        category_table_file = UF_CATEGORY_TABLE,
        court_table_file = UF_COURT_TABLE,
    output:
        net_file = CITATION_NET,
        paper_table_file = PAPER_TABLE,
        paper_category_table_file = PAPER_CATEGORY_TABLE,
        category_table_file = CATEGORY_TABLE,
        court_table_file = COURT_TABLE,
    script:
        "scripts/filter_to_lcc.py"
