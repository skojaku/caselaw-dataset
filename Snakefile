"""
Caselaw Dataset Pipeline

Preprocesses raw caselaw data (CourtListener / Harvard Caselaw Access Project)
into structured tables (cases, judges, citations, court categories).

Two output directories:
  - preprocessed_unfiltered/: all cases
  - preprocessed/: filtered to largest weakly connected component only

Raw input files (set raw_dir in config.yaml):
  - Legal_Citation_Dict.json   — citation links  {cited_id: [citing_id, ...]}
  - Citation_Info_Dict.json    — case metadata
  - court_hierarchy.json       — court hierarchy tree

Usage:
    snakemake --cores all     # Run full pipeline
    snakemake -n              # Dry run
"""

from os.path import join as j

configfile: "config.yaml"

# =============================================================================
# Directories
# =============================================================================

RAW_DIR = config["raw_dir"]
OUTPUT_DIR = config["output_dir"]
UNFILTERED_DIR = OUTPUT_DIR + "_unfiltered"

# =============================================================================
# Raw inputs
# =============================================================================

CITATION_FILE = j(RAW_DIR, "Legal_Citation_Dict.json")
CITATION_INFO_FILE = j(RAW_DIR, "Citation_Info_Dict.json")
COURT_HIERARCHY_FILE = j(RAW_DIR, "court_hierarchy.json")

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


rule build_citation_net:
    input:
        node_data_file = CITATION_INFO_FILE,
        link_data_file = CITATION_FILE,
        court_data_file = COURT_HIERARCHY_FILE,
    output:
        net_file = UF_CITATION_NET,
        node_table_file = UF_PAPER_TABLE,
        court_table_file = UF_COURT_TABLE,
    script:
        "scripts/build_citation_net.py"


rule build_category_table:
    input:
        input_file = UF_PAPER_TABLE,
        court_hierarchy_file = COURT_HIERARCHY_FILE,
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
