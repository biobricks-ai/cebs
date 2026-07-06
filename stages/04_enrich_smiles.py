#!/usr/bin/env python3
"""
Enrich brick/datasets/chemlists.parquet with SMILES structures.

Background
----------
CEBS is an assay-/study-centric resource keyed by CASRN / DTXSID / chemical name.
Its own ``chemlists`` dataset carries a ``smiles_code`` field, but the source only
populates it for a tiny fraction of chemicals (~3%). Structures are therefore
*genuinely sparse in the source*, not dropped by our build.

This stage backfills the missing ``smiles_code`` values by mapping the identifiers
that ARE present (DTXSID, then CASRN) to InChI via the local DSSTox table
(comptox brick), then converting InChI -> RDKit canonical SMILES. Source-provided
SMILES are preserved as-is; only null cells are filled.

Idempotent: re-running only fills cells that are still null.
"""

import os
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
BRICK = HERE.parent / "brick"
CHEMLISTS = BRICK / "datasets" / "chemlists.parquet"
DSSTOX_PATH = HERE.parent.parent / "comptox" / "brick" / "dsstox_identifiers.parquet"


def _norm(s):
    if s is None:
        return None
    s = str(s).strip()
    return s if s else None


def enrich(df):
    from rdkit import Chem
    from rdkit import RDLogger
    RDLogger.DisableLog("rdApp.*")

    if not DSSTOX_PATH.exists():
        print(f"DSSTox mapping not found at {DSSTOX_PATH}; leaving smiles_code as-is.")
        return df

    d = pd.read_parquet(DSSTOX_PATH, columns=["dtxsid", "casrn", "inchi"])
    d = d[d["inchi"].notna() & (d["inchi"].astype(str).str.strip() != "")].copy()
    dtx_map = d.dropna(subset=["dtxsid"]).drop_duplicates("dtxsid").set_index("dtxsid")["inchi"].to_dict()
    cas_map = d.dropna(subset=["casrn"]).drop_duplicates("casrn").set_index("casrn")["inchi"].to_dict()

    # Resolve an InChI for every row that lacks a SMILES, preferring DTXSID then CASRN.
    def row_inchi(row):
        for key, m in ((_norm(row.get("dtxsid")), dtx_map),
                       (_norm(row.get("dtt_casrn")), cas_map),
                       (_norm(row.get("epa_cas_no")), cas_map)):
            if key and key in m:
                return m[key]
        return None

    need = df["smiles_code"].isna() | (df["smiles_code"].astype(str).str.strip() == "")
    df.loc[need, "_inchi"] = df.loc[need].apply(row_inchi, axis=1)

    # InChI -> canonical SMILES (cached per unique InChI)
    smi_cache = {}
    for inchi in df["_inchi"].dropna().unique():
        mol = Chem.MolFromInchi(str(inchi))
        if mol is not None:
            smi_cache[inchi] = Chem.MolToSmiles(mol)

    fill = need & df["_inchi"].notna()
    df.loc[fill, "smiles_code"] = df.loc[fill, "_inchi"].map(smi_cache)
    df = df.drop(columns=["_inchi"])
    return df


def main():
    df = pd.read_parquet(CHEMLISTS)
    n = len(df)
    before = int((df["smiles_code"].notna() & (df["smiles_code"].astype(str).str.strip() != "")).sum())
    df = enrich(df)
    after = int((df["smiles_code"].notna() & (df["smiles_code"].astype(str).str.strip() != "")).sum())
    df.to_parquet(CHEMLISTS, index=False)
    print(f"chemlists smiles_code: {before}/{n} ({before/n:.1%}) -> {after}/{n} ({after/n:.1%})")


if __name__ == "__main__":
    main()
