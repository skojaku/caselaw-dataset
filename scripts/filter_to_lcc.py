# -*- coding: utf-8 -*-
# @Author: Sadamori Kojaku
# @Date:   2026-03-04
# @Last Modified by:   Sadamori Kojaku
# @Last Modified time: 2026-03-04
# %%
import sys

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.sparse.csgraph import connected_components

if "snakemake" in sys.modules:
    net_file = snakemake.input["net_file"]
    paper_table_file = snakemake.input["paper_table_file"]
    paper_category_table_file = snakemake.input["paper_category_table_file"]
    category_table_file = snakemake.input["category_table_file"]
    court_table_file = snakemake.input["court_table_file"]
    output_net_file = snakemake.output["net_file"]
    output_paper_table_file = snakemake.output["paper_table_file"]
    output_paper_category_table_file = snakemake.output["paper_category_table_file"]
    output_category_table_file = snakemake.output["category_table_file"]
    output_court_table_file = snakemake.output["court_table_file"]
else:
    net_file = "data/caselaw/preprocessed/citation_net_unfiltered.npz"
    paper_table_file = "data/caselaw/preprocessed/paper_table_unfiltered.csv"
    paper_category_table_file = "data/caselaw/preprocessed/paper_category_table_unfiltered.csv"
    category_table_file = "data/caselaw/preprocessed/category_table_unfiltered.csv"
    court_table_file = "data/caselaw/preprocessed/court_table_unfiltered.csv"
    output_net_file = "data/caselaw/preprocessed/citation_net.npz"
    output_paper_table_file = "data/caselaw/preprocessed/paper_table.csv"
    output_paper_category_table_file = "data/caselaw/preprocessed/paper_category_table.csv"
    output_category_table_file = "data/caselaw/preprocessed/category_table.csv"
    output_court_table_file = "data/caselaw/preprocessed/court_table.csv"

# %%
# Load
net = sparse.load_npz(net_file)
paper_table = pd.read_csv(paper_table_file)
paper_category_table = pd.read_csv(paper_category_table_file)
category_table = pd.read_csv(category_table_file)
court_table = pd.read_csv(court_table_file)

# %%
# Find the largest weakly connected component
n_components, labels = connected_components(net, directed=True, connection="weak")
component_sizes = np.bincount(labels)
largest_component = np.argmax(component_sizes)
keep_mask = labels == largest_component
keep_indices = np.where(keep_mask)[0]

print(f"Total nodes: {net.shape[0]}")
print(f"Number of components: {n_components}")
print(f"Largest component size: {component_sizes[largest_component]}")
print(f"Fraction kept: {component_sizes[largest_component] / net.shape[0]:.4f}")

# %%
# Filter citation network: keep only rows/columns in the largest component
net_filtered = net[keep_indices, :][:, keep_indices]

# %%
# Build old_id -> new_id mapping
old_to_new = {old_id: new_id for new_id, old_id in enumerate(keep_indices)}

# Filter paper_table
paper_table_filtered = paper_table[paper_table["paper_id"].isin(keep_indices)].copy()
paper_table_filtered["paper_id"] = paper_table_filtered["paper_id"].map(old_to_new)
paper_table_filtered = paper_table_filtered.sort_values("paper_id").reset_index(drop=True)

# Filter paper_category_table
paper_category_table_filtered = paper_category_table[
    paper_category_table["paper_id"].isin(keep_indices)
].copy()
paper_category_table_filtered["paper_id"] = paper_category_table_filtered[
    "paper_id"
].map(old_to_new)
paper_category_table_filtered = paper_category_table_filtered.sort_values(
    "paper_id"
).reset_index(drop=True)

# category_table and court_table don't reference paper_id, so keep as-is

# %%
# Save
sparse.save_npz(output_net_file, net_filtered)
paper_table_filtered.to_csv(output_paper_table_file, index=False)
paper_category_table_filtered.to_csv(output_paper_category_table_file, index=False)
category_table.to_csv(output_category_table_file, index=False)
court_table.to_csv(output_court_table_file, index=False)
