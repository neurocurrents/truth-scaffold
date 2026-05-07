# Truth Scaffold (TS_v001)
## Dopamine Semantic Geometry Audit

Author: Rex L. Cannon, PhD, BCN  
Organization: Currents / Truth Scaffold  
Version: TS_v001  
Date: 2026

---

# Overview

This repository/archive contains the first Truth Scaffold (TS_v001) computational audit of the dopamine literature using large-scale semantic embedding analysis, UMAP geometry, centroid analysis, semantic overlap metrics, receptor-domain mapping, and chemical-semantic comparison methods.

The purpose of this audit is to examine the large-scale semantic structure of dopamine-related scientific discourse across functional, receptor, and chemical domains.

This archive is intended for:
- scientific review,
- replication,
- computational auditing,
- educational use,
- and methodological transparency.

---
All files needed to reproduce this audit can be obtained via google drive: https://drive.google.com/drive/u/0/folders/1j222CVY-jn8S7O6pokOVq_dj8utTgggC
# Corpus

Primary corpus:
- PubMed dopamine literature dataset
- Final cleaned records: 157,573

Text fields used:
- title
- abstract

---

# Embedding Model

Embeddings generated using:

- sentence-transformers
- all-MiniLM-L6-v2
- 384-dimensional embeddings

---

# UMAP Parameters

UMAP settings:

- metric = cosine
- n_neighbors = 30
- min_dist = 0.1
- random_state = 42

---

# Included Files

## Core Data

- dopamine_cleaned_TS_v001.csv.gz  
  Cleaned dopamine corpus

- dopamine_TS_master_v001.csv  
  Master analysis file including semantic coordinates and audit annotations

---

## Embeddings / Geometry

- dopamine_embeddings_TS_v001.npy  
  384-dimensional semantic embeddings

- dopamine_umap_coords_v001.npy  
  UMAP coordinate array

- dopamine_TS_umap_coordinates_v001.csv  
  Exported UMAP coordinates

---

## Analysis Outputs

Includes:
- semantic overlap analyses
- centroid geometry analyses
- receptor-domain analyses
- functional drift analyses
- SGI calculations
- partial semantic space analyses
- heatmaps and figure exports

---

# Core Concepts

## Semantic Gravity Index (SGI)

SGI estimates the degree to which shared language compresses or stabilizes functional semantic domains.

Higher SGI values suggest stronger semantic compression and reduced functional differentiation.

---

## Identity Loss

Identity loss estimates how substantially domain geometry changes after removal of shared semantic core language.

---

## Variance Compression

Variance compression estimates how much shared language reduces observable semantic variance across domains.

---

# Reproducibility

The notebook and associated files are intended to allow independent replication of:
- embedding generation,
- semantic geometry,
- overlap analyses,
- receptor-domain distributions,
- and TS metrics.

Researchers are encouraged to:
- reproduce analyses,
- test alternate embedding models,
- test alternate UMAP parameters,
- and challenge interpretations.

---

# Interpretation Notice

The analyses in this archive examine semantic and geometric properties of scientific discourse.

These analyses do not independently establish biological truth or falsity.

Truth Scaffold evaluates:
- semantic structure,
- domain overlap,
- receptor-language distributions,
- and historical-functional drift patterns within scientific corpora.

Interpretations remain probabilistic and subject to further validation.

---

# Copyright / Usage

Copyright © 2026 Currents / Rex L. Cannon

The Truth Scaffold framework, scoring systems, computational architecture, and associated analytic methodology remain intellectual property of Currents.

This archive may be used for:
- scholarly review,
- educational purposes,
- and non-commercial replication.

Commercial reuse, redistribution, derivative TS systems, or incorporation into proprietary software platforms requires written permission.

---

# Contact

Rex L. Cannon, PhD, BCN

Currents  
https://neurocurrents.org
