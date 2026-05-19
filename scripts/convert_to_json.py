# -*- coding: utf-8 -*-
# @Author: Sadamori Kojaku
# @Date:   2026-05-17
# @Last Modified by:   Sadamori Kojaku
# @Last Modified time: 2026-05-18
"""
Convert CourtListener bulk CSV data to the three JSON files used by the
caselaw preprocessing pipeline:

  - Legal_Citation_Dict.json   {cited_opinion_id: [citing_opinion_id, ...]}
  - Citation_Info_Dict.json    {opinion_id: {date, court, case_name, ...}}
  - court_hierarchy.json       [[supreme_name], [circuit_name, dist1, ...], ...]

Uses polars with lazy evaluation + streaming so large files (opinions.csv has
full HTML text) are never fully loaded into RAM.
"""
# %%
import csv
import json
import sys
from collections import defaultdict

csv.field_size_limit(sys.maxsize)

import polars as pl

if "snakemake" in sys.modules:
    citation_map_file = snakemake.input["citation_map_file"]
    opinions_file = snakemake.input["opinions_file"]
    clusters_file = snakemake.input["clusters_file"]
    dockets_file = snakemake.input["dockets_file"]
    courts_file = snakemake.input["courts_file"]
    out_citation_dict = snakemake.output["citation_dict"]
    out_info_dict = snakemake.output["info_dict"]
    out_court_hierarchy = snakemake.output["court_hierarchy"]
else:
    RAW = "raw"
    citation_map_file = f"{RAW}/citation_map.csv"
    opinions_file = f"{RAW}/opinions.csv"
    clusters_file = f"{RAW}/opinion_clusters.csv"
    dockets_file = f"{RAW}/dockets.csv"
    courts_file = f"{RAW}/courts.csv"
    out_citation_dict = "Legal_Citation_Dict.json"
    out_info_dict = "Citation_Info_Dict.json"
    out_court_hierarchy = "court_hierarchy.json"

# =============================================================================
# 1. Legal_Citation_Dict.json
#    {cited_opinion_id_str: [citing_opinion_id_int, ...]}
# =============================================================================
print("Building citation dict...")
citation = pl.read_csv(
    citation_map_file,
    columns=["cited_opinion_id", "citing_opinion_id"],
    infer_schema_length=0,
)
citation_agg = citation.group_by("cited_opinion_id").agg(
    pl.col("citing_opinion_id").cast(pl.Int64, strict=False).drop_nulls()
)
citation_dict = {
    row["cited_opinion_id"]: row["citing_opinion_id"]
    for row in citation_agg.to_dicts()
}
with open(out_citation_dict, "w") as f:
    json.dump(citation_dict, f)
print(f"  {len(citation_dict):,} cited opinions written")

# =============================================================================
# 2. Citation_Info_Dict.json
#    {opinion_id_str: {date, court, case_name, case_name_full}}
#
#    Join chain (all lazy + streamed):
#      opinions  →(cluster_id)→  opinion_clusters  →(docket_id)→  dockets  →(court_id)→  courts
# =============================================================================
print("Loading courts...")
courts_rows = []
with open(courts_file, newline="", encoding="utf-8") as _f:
    for row in csv.DictReader(_f):
        courts_rows.append(row)
court_id_to_name = {row["id"]: row["full_name"] for row in courts_rows}

print("Loading clusters...")
cluster_info = {}  # cluster_id -> {date, case_name, case_name_full, docket_id}
with open(clusters_file, newline="", encoding="utf-8") as _f:
    for row in csv.DictReader(_f):
        cluster_info[row["id"]] = {
            "date": row.get("date_filed", ""),
            "case_name": row.get("case_name", ""),
            "case_name_full": row.get("case_name_full", ""),
            "docket_id": row.get("docket_id", ""),
        }
print(f"  {len(cluster_info):,} clusters loaded")

print("Loading dockets...")
docket_court = {}  # docket_id -> court_id
with open(dockets_file, newline="", encoding="utf-8") as _f:
    for row in csv.DictReader(_f):
        docket_court[row["id"]] = row.get("court_id", "")
print(f"  {len(docket_court):,} dockets loaded")

print("Building info dict (streaming opinions)...")
info_dict = {}
with open(opinions_file, newline="", encoding="utf-8") as _f:
    for row in csv.DictReader(_f):
        op_id = row.get("id", "")
        cl_id = row.get("cluster_id", "")
        if not op_id.isdigit():
            continue
        c = cluster_info.get(cl_id)
        if c is None:
            continue
        court = court_id_to_name.get(docket_court.get(c["docket_id"], ""), "")
        info_dict[op_id] = {
            "date": c["date"],
            "court": court,
            "case_name": c["case_name"],
            "case_name_full": c["case_name_full"],
        }
with open(out_info_dict, "w") as f:
    json.dump(info_dict, f)
print(f"  {len(info_dict):,} opinions written")

# =============================================================================
# 3. court_hierarchy.json
#    [[supreme_name], [circuit_name, dist1_name, dist2_name, ...], ...]
# =============================================================================
print("Building court hierarchy...")

courts_in_use = [row for row in courts_rows if row.get("in_use") == "t"]

# Canonical district-court-id → circuit-court-id mapping.
# Source: https://www.uscourts.gov/about-federal-courts/court-role-and-structure
DISTRICT_TO_CIRCUIT = {
    # First Circuit
    "med": "ca1", "nhd": "ca1", "mad": "ca1", "rid": "ca1", "prd": "ca1",
    # Second Circuit
    "ctd": "ca2", "nyed": "ca2", "nynd": "ca2", "nysd": "ca2", "nywd": "ca2", "vtd": "ca2",
    # Third Circuit
    "njd": "ca3", "paed": "ca3", "pamd": "ca3", "pawd": "ca3", "ded": "ca3", "vid": "ca3",
    "pennsylvaniad": "ca3",
    # Fourth Circuit
    "mdd": "ca4", "vaed": "ca4", "vawd": "ca4", "wvnd": "ca4", "wvsd": "ca4",
    "nced": "ca4", "ncmd": "ca4", "ncwd": "ca4", "scd": "ca4",
    "southcarolinaed": "ca4", "southcarolinawd": "ca4",
    # Fifth Circuit
    "txed": "ca5", "txnd": "ca5", "txsd": "ca5", "txwd": "ca5",
    "laed": "ca5", "lamd": "ca5", "lawd": "ca5", "orld": "ca5",
    "msnd": "ca5", "mssd": "ca5",
    # Sixth Circuit
    "kyed": "ca6", "kywd": "ca6", "mied": "ca6", "miwd": "ca6",
    "ohnd": "ca6", "ohsd": "ca6", "ohiod": "ca6",
    "tned": "ca6", "tnmd": "ca6", "tnwd": "ca6", "tennessed": "ca6",
    # Seventh Circuit
    "ilnd": "ca7", "ilsd": "ca7", "ilcd": "ca7", "illinoisd": "ca7", "illinoised": "ca7",
    "innd": "ca7", "insd": "ca7", "indianad": "ca7",
    "wied": "ca7", "wiwd": "ca7",
    # Eighth Circuit
    "ared": "ca8", "arwd": "ca8",
    "iand": "ca8", "iasd": "ca8",
    "mnd": "ca8",
    "moed": "ca8", "mowd": "ca8",
    "ned": "ca8", "ndd": "ca8", "sdd": "ca8",
    # Ninth Circuit
    "akd": "ca9", "azd": "ca9",
    "cand": "ca9", "caed": "ca9", "cacd": "ca9", "casd": "ca9", "californiad": "ca9",
    "hid": "ca9", "idd": "ca9", "mtd": "ca9", "nvd": "ca9", "ord": "ca9",
    "waed": "ca9", "wawd": "ca9",
    "gud": "ca9", "nmid": "ca9", "canalzoned": "ca9",
    # Tenth Circuit
    "cod": "ca10", "ksd": "ca10", "nmd": "ca10",
    "oked": "ca10", "oknd": "ca10", "okwd": "ca10",
    "utd": "ca10", "wyd": "ca10",
    # Eleventh Circuit
    "alnd": "ca11", "almd": "ca11", "alsd": "ca11",
    "flmd": "ca11", "flnd": "ca11", "flsd": "ca11",
    "gand": "ca11", "gamd": "ca11", "gasd": "ca11",
    # D.C. Circuit
    "dcd": "cadc",
    # Federal Circuit (specialized)
    "uscfc": "cafc",
}

scotus_id = "scotus"
circuit_ids = set(
    row["id"] for row in courts_in_use
    if row.get("jurisdiction") == "F" and row["id"] not in {scotus_id, "usjc"}
)

circuit_to_districts = defaultdict(list)
for d_id, c_id in DISTRICT_TO_CIRCUIT.items():
    name = court_id_to_name.get(d_id)
    if name and c_id in circuit_ids:
        circuit_to_districts[c_id].append(name)

scotus_name = court_id_to_name.get(scotus_id, "Supreme Court of the United States")
hierarchy = [[scotus_name]]
for c_id in sorted(circuit_ids):
    circuit_name = court_id_to_name.get(c_id, c_id)
    districts = sorted(circuit_to_districts.get(c_id, []))
    hierarchy.append([circuit_name] + districts)

with open(out_court_hierarchy, "w") as f:
    json.dump(hierarchy, f, indent=2)
print(f"  {len(hierarchy)} entries (1 supreme + {len(hierarchy)-1} circuits)")
