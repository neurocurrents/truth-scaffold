# ==================================================
# TS DOPAMINE EMBEDDING + UMAP PIPELINE
# SAVE TO TS_v001
# ==================================================

import os
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import umap.umap_ as umap
import matplotlib.pyplot as plt

# --------------------------------------------------
# OUTPUT PATH
# --------------------------------------------------

BASE = r"C:\neurofeedback_models\dopamine_20251202\TS_v001"

os.makedirs(BASE, exist_ok=True)

# --------------------------------------------------
# BUILD TEXT
# --------------------------------------------------

df["text"] = (
    df["title"].fillna("") + ". " +
    df["abstract"].fillna("")
)

texts = df["text"].astype(str).tolist()

print("Documents:", len(texts))

# --------------------------------------------------
# LOAD EMBEDDING MODEL
# --------------------------------------------------

model = SentenceTransformer(
    "sentence-transformers/all-MiniLM-L6-v2"
)

# --------------------------------------------------
# GENERATE EMBEDDINGS
# --------------------------------------------------

embeddings = model.encode(
    texts,
    batch_size=64,
    show_progress_bar=True,
    convert_to_numpy=True
)

print("Embedding shape:", embeddings.shape)

# --------------------------------------------------
# SAVE EMBEDDINGS
# --------------------------------------------------

np.save(
    f"{BASE}\\dopamine_embeddings_TS_v001.npy",
    embeddings
)

print("Saved embeddings.")

# --------------------------------------------------
# RUN UMAP
# --------------------------------------------------

reducer = umap.UMAP(
    n_neighbors=30,
    min_dist=0.1,
    metric="cosine",
    random_state=42
)

coords = reducer.fit_transform(embeddings)

print("UMAP shape:", coords.shape)

# --------------------------------------------------
# SAVE COORDINATES
# --------------------------------------------------

df["UMAP-1"] = coords[:, 0]
df["UMAP-2"] = coords[:, 1]

df.to_csv(
    f"{BASE}\\dopamine_TS_umap_coordinates_v001.csv",
    index=False
)

np.save(
    f"{BASE}\\dopamine_umap_coords_v001.npy",
    coords
)

print("Saved UMAP coordinates.")

# --------------------------------------------------
# QUICK UMAP PLOT
# --------------------------------------------------

plt.figure(figsize=(10,8))

plt.scatter(
    coords[:,0],
    coords[:,1],
    s=2,
    alpha=0.5
)

plt.title(
    "Dopamine Literature Semantic Space",
    fontsize=16
)

plt.xlabel("UMAP-1")
plt.ylabel("UMAP-2")

plt.tight_layout()

plt.savefig(
    f"{BASE}\\dopamine_umap_overview_v001.png",
    dpi=300
)

plt.show()

print("Saved overview plot.")

# --------------------------------------------------
# SAVE TS MASTER FILE
# --------------------------------------------------

df.to_csv(
    f"{BASE}\\dopamine_TS_master_v001.csv",
    index=False
)

print("Saved TS master file.")

# --------------------------------------------------
# FINAL OUTPUTS
# --------------------------------------------------

print("\n=== TS_v001 FILES SAVED ===")

print(f"{BASE}\\dopamine_embeddings_TS_v001.npy")
print(f"{BASE}\\dopamine_umap_coords_v001.npy")
print(f"{BASE}\\dopamine_TS_umap_coordinates_v001.csv")
print(f"{BASE}\\dopamine_TS_master_v001.csv")
print(f"{BASE}\\dopamine_umap_overview_v001.png")