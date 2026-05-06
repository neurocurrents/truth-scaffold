# Dopamine Truth Scaffold Audit

This folder contains Truth Scaffold (TS) audit outputs for the dopamine literature corpus.

## Dataset
- Source: PubMed
- Records (TS-ready): 157,573
- Fields: PMID, title, abstract, year
- File: dopamine_cleaned_TS_v001.csv.gz
- # Dopamine Dataset (TS_v001)

## Download

https://drive.google.com/uc?id=1YR-kQoQzcxa98lPPkbT0ZMsRZnFY070i&export=download

## Instructions

1. Download the dataset
2. Place it in:

data/dopamine/

## Expected file

dopamine_cleaned_TS_v001.csv.gz

## Summary

- Records: 157,573
- Source: PubMed abstracts
- No full-text included
- Prepared for Truth Scaffold analysis

## Python Example

```python
import gdown
import pandas as pd

url = "https://drive.google.com/uc?id=1YR-kQoQzcxa98lPPkbT0ZMsRZnFY070i"
gdown.download(url, "dopamine_cleaned_TS_v001.csv.gz", quiet=False)

df = pd.read_csv("dopamine_cleaned_TS_v001.csv.gz")
print(df.shape)

## Purpose
To evaluate the structural properties of the dopamine literature using:
- Embedding space analysis (UMAP)
- Attractor detection
- Semantic overlap and collapse
- Entropy and structural sufficiency metrics

## Planned Analyses
- UMAP projection and clustering
- Functional centroid mapping (motor, reward, etc.)
- Attractor dominance testing
- Cross-space comparison (chemical vs semantic)

## Outputs
This folder will contain:
- TS reports (.md)
- Figures (.png)
- Summary metrics (.csv)

## Notes
This audit is part of the Truth Scaffold framework for evaluating epistemic structure across scientific domains.
