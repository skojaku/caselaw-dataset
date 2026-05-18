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
# opinions has large HTML/text fields with embedded newlines; use pandas with
# usecols to avoid csv.DictReader misaligning rows on those fields
opinions_df = pd.read_csv(
    opinions_file, usecols=["id", "cluster_id"], dtype=str
).fillna("")
for _, row in opinions_df.iterrows():
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
#    - SCOTUS is the top level
#    - Circuit courts of appeals are second level
#    - District courts are grouped under their circuit using the canonical
#      federal court system mapping (court_appeals_to only covers special courts)
# =============================================================================
print("Building court hierarchy...")

courts_in_use = courts[courts["in_use"] == "t"]

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
circuit_ids = [
    r["id"]
    for _, r in courts_in_use.iterrows()
    if r["jurisdiction"] == "F" and r["id"] not in (scotus_id, "usjc")
]

# Group districts under their circuit
circuit_to_districts = defaultdict(list)
for d_id, c_id in DISTRICT_TO_CIRCUIT.items():
    name = court_id_to_name.get(d_id)
    if name and c_id in set(circuit_ids):
        circuit_to_districts[c_id].append(name)

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
