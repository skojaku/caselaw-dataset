# -*- coding: utf-8 -*-
# @Author: Sadamori Kojaku
# @Date:   2026-05-17
# @Last Modified by:   Sadamori Kojaku
# @Last Modified time: 2026-05-17
"""
Convert CourtListener bulk CSV data to the three JSON files used by the
caselaw preprocessing pipeline:

  - Legal_Citation_Dict.json   {cited_opinion_id: [citing_opinion_id, ...]}
  - Citation_Info_Dict.json    {opinion_id: {date, court, case_name, ...}}
  - court_hierarchy.json       [[supreme_name], [circuit_name, dist1, ...], ...]
"""
# %%
import csv
import json
import sys
from collections import defaultdict

import pandas as pd

if "snakemake" in sys.modules:
    citation_map_file = snakemake.input["citation_map_file"]
    opinions_file = snakemake.input["opinions_file"]
    clusters_file = snakemake.input["clusters_file"]
    dockets_file = snakemake.input["dockets_file"]
    courts_file = snakemake.input["courts_file"]
    court_appeals_to_file = snakemake.input["court_appeals_to_file"]
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
    court_appeals_to_file = f"{RAW}/court_appeals_to.csv"
    out_citation_dict = "Legal_Citation_Dict.json"
    out_info_dict = "Citation_Info_Dict.json"
    out_court_hierarchy = "court_hierarchy.json"

# =============================================================================
# 1. Legal_Citation_Dict.json
#    {cited_opinion_id_str: [citing_opinion_id_int, ...]}
# =============================================================================
print("Building citation dict...")
citation_dict = defaultdict(list)
with open(citation_map_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cited = row["cited_opinion_id"]
        citing = int(row["citing_opinion_id"])
        citation_dict[cited].append(citing)

with open(out_citation_dict, "w") as f:
    json.dump(dict(citation_dict), f)
print(f"  {len(citation_dict):,} cited opinions written")

# =============================================================================
# 2. Citation_Info_Dict.json
#    {opinion_id_str: {date, court, case_name, case_name_full}}
#
#    Join chain:
#      opinions.id  →(cluster_id)→  opinion_clusters.id
#      opinion_clusters  →(docket_id)→  dockets.id
#      dockets.court_id  →(id)→  courts.full_name
# =============================================================================
print("Loading courts...")
courts = pd.read_csv(courts_file, dtype=str).fillna("")
court_id_to_name = dict(zip(courts["id"], courts["full_name"]))

print("Loading dockets (court_id only)...")
dockets = pd.read_csv(dockets_file, usecols=["id", "court_id"], dtype=str).fillna("")
docket_id_to_court_id = dict(zip(dockets["id"], dockets["court_id"]))

print("Loading opinion clusters...")
clusters = pd.read_csv(
    clusters_file,
    usecols=["id", "date_filed", "case_name", "case_name_full", "docket_id"],
    dtype=str,
).fillna("")
cluster_id_to_info = {}
for _, row in clusters.iterrows():
    court_id = docket_id_to_court_id.get(row["docket_id"], "")
    court_name = court_id_to_name.get(court_id, court_id)
    cluster_id_to_info[row["id"]] = {
        "date": row["date_filed"],
        "court": court_name,
        "case_name": row["case_name"],
        "case_name_full": row["case_name_full"],
    }

print("Loading opinions and building info dict...")
info_dict = {}
with open(opinions_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cluster_info = cluster_id_to_info.get(row["cluster_id"])
        if cluster_info is not None:
            info_dict[row["id"]] = cluster_info

with open(out_info_dict, "w") as f:
    json.dump(info_dict, f)
print(f"  {len(info_dict):,} opinions written")

# =============================================================================
# 3. court_hierarchy.json
#    [[supreme_name], [circuit_name, dist1_name, dist2_name, ...], ...]
#
#    - SCOTUS (jurisdiction='F', id='scotus') is the top level
#    - Circuit courts (jurisdiction='F', id starts with 'ca') are second level
#    - District courts (jurisdiction='FD') are grouped under their circuit
#      using the court_appeals_to table (from_court_id -> to_court_id)
# =============================================================================
print("Building court hierarchy...")

courts_df = courts.copy()
courts_in_use = courts_df[courts_df["in_use"] == "t"]

# Build district -> circuit mapping from court_appeals_to
appeals_to = pd.read_csv(court_appeals_to_file, dtype=str).fillna("")
district_to_circuit = dict(
    zip(appeals_to["from_court_id"], appeals_to["to_court_id"])
)

# Identify court tiers by jurisdiction code
# F = federal (SCOTUS + circuit courts of appeals)
# FD = federal district courts
scotus_id = "scotus"
circuit_ids = [
    r["id"]
    for _, r in courts_in_use.iterrows()
    if r["jurisdiction"] == "F" and r["id"] != scotus_id and r["id"] != "usjc"
]
district_ids = [
    r["id"] for _, r in courts_in_use.iterrows() if r["jurisdiction"] == "FD"
]

# Group districts under their circuit
circuit_to_districts = defaultdict(list)
for d_id in district_ids:
    circuit_id = district_to_circuit.get(d_id)
    if circuit_id and circuit_id in set(circuit_ids):
        circuit_to_districts[circuit_id].append(court_id_to_name.get(d_id, d_id))

# Build the hierarchy list
scotus_name = court_id_to_name.get(scotus_id, "Supreme Court of the United States")
hierarchy = [[scotus_name]]
for c_id in sorted(circuit_ids):
    circuit_name = court_id_to_name.get(c_id, c_id)
    districts = sorted(circuit_to_districts.get(c_id, []))
    hierarchy.append([circuit_name] + districts)

with open(out_court_hierarchy, "w") as f:
    json.dump(hierarchy, f, indent=2)
print(f"  {len(hierarchy)} entries (1 supreme + {len(hierarchy)-1} circuits)")
