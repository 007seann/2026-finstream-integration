"""
Form 4 postprocessing stage.

Joins one quarter's filtered SUBMISSION/REPORTINGOWNER/NONDERIV_TRANS tables
(as produced by collect.collect_quarter) into the pipeline's final schema:
drops SEC footnote columns, converts transaction dates to ISO format, and
derives is_officer/is_director/is_ten_pct_owner/officer_title from the
reporting owner's relationship string.

SEC has used more than one REPORTINGOWNER role encoding over time (an older
isofficer/isdirector/istenpercentowner boolean-column schema is referenced in
some documentation), although every quarter from 2006Q1 through 2024Q1
actually observed while building this pipeline already used the newer
rptowner_relationship/rptowner_title string encoding. Role extraction is
still detected per quarter (not assumed from one sample quarter) so a future
schema change degrades gracefully instead of silently producing empty role
columns.
"""
import pandas as pd

COLUMN_RENAME = {
    "nonderiv_trans_sk": "trans_sk",
    "trans_date": "transaction_date",
    "trans_form_type": "form_type",
    "trans_code": "transaction_code",
    "trans_acquired_disp_cd": "acquired_disposed",
    "trans_shares": "shares",
    "trans_pricepershare": "price_per_share",
    "shrs_ownd_folwng_trans": "shares_after_txn",
    "valu_ownd_folwng_trans": "value_after_txn",
    "direct_indirect_ownership": "ownership_type",
}

FINAL_COLUMNS = [
    "ticker", "issuer_cik", "accession_number", "owner_name", "rptownercik",
    "is_officer", "is_director", "is_ten_pct_owner", "officer_title",
    "transaction_date", "transaction_code", "acquired_disposed", "shares",
    "price_per_share", "shares_after_txn", "value_after_txn", "security_title",
    "ownership_type", "form_type", "trans_sk", "deemed_execution_date",
    "equity_swap_involved", "trans_timeliness", "nature_of_ownership",
]

_EMPTY_ROLES = pd.DataFrame(columns=[
    "accession_number", "rptownercik", "owner_name",
    "is_officer", "is_director", "is_ten_pct_owner", "officer_title",
])


def extract_owner_roles(owners: pd.DataFrame) -> pd.DataFrame:
    if owners is None or owners.empty:
        return _EMPTY_ROLES

    acc_col = next((c for c in owners.columns if "accession" in c), None)
    if acc_col is None:
        return _EMPTY_ROLES

    out = pd.DataFrame({"accession_number": owners[acc_col]})
    out["rptownercik"] = owners["rptownercik"] if "rptownercik" in owners.columns else None
    name_col = next((c for c in owners.columns if c in ("rptownername", "rptowner_name")), None)
    out["owner_name"] = owners[name_col] if name_col else None

    if "rptowner_relationship" in owners.columns:
        rel = owners["rptowner_relationship"].fillna("")
        out["is_officer"] = rel.str.contains("Officer", case=False).astype(int)
        out["is_director"] = rel.str.contains("Director", case=False).astype(int)
        out["is_ten_pct_owner"] = rel.str.contains("TenPercentOwner", case=False).astype(int)
        out["officer_title"] = owners["rptowner_title"] if "rptowner_title" in owners.columns else None
    else:
        dir_col = next((c for c in owners.columns if c in ("isdirector", "isdir")), None)
        off_col = "isofficer" if "isofficer" in owners.columns else None
        pct_col = "istenpercentowner" if "istenpercentowner" in owners.columns else None
        title_col = "officertitle" if "officertitle" in owners.columns else None
        out["is_officer"] = pd.to_numeric(owners[off_col], errors="coerce").fillna(0).astype(int) if off_col else 0
        out["is_director"] = pd.to_numeric(owners[dir_col], errors="coerce").fillna(0).astype(int) if dir_col else 0
        out["is_ten_pct_owner"] = pd.to_numeric(owners[pct_col], errors="coerce").fillna(0).astype(int) if pct_col else 0
        out["officer_title"] = owners[title_col] if title_col else None

    return out.drop_duplicates(subset=["accession_number", "rptownercik"])


def process_quarter(raw: dict, cik_to_ticker: dict) -> pd.DataFrame:
    transactions = raw["transactions"].copy()
    fn_cols = [c for c in transactions.columns if c.endswith("_fn")]
    transactions = transactions.drop(columns=fn_cols)
    transactions = transactions.rename(columns={raw["txn_acc_col"]: "accession_number"})

    submission_slim = raw["submission"][[raw["issuer_col"], raw["acc_col"]]].rename(
        columns={raw["issuer_col"]: "issuer_cik", raw["acc_col"]: "accession_number"}
    )
    merged = transactions.merge(submission_slim, on="accession_number", how="left")

    roles = extract_owner_roles(raw["owners"])
    if not roles.empty:
        merged = merged.merge(roles, on="accession_number", how="left")
    else:
        for col in ("rptownercik", "owner_name", "is_officer", "is_director", "is_ten_pct_owner", "officer_title"):
            merged[col] = None

    merged["issuer_cik"] = pd.to_numeric(merged["issuer_cik"], errors="coerce")
    merged["ticker"] = merged["issuer_cik"].map(cik_to_ticker)

    merged = merged.rename(columns=COLUMN_RENAME)

    # SEC's bulk flat files consistently encode this as DD-MON-YYYY (e.g.
    # "15-NOV-2022") — an explicit format (rather than pandas' newer
    # format="mixed" inference, which pandas 1.3.5 doesn't support) keeps
    # this compatible with the pinned production stack.
    merged["transaction_date"] = pd.to_datetime(
        merged["transaction_date"], format="%d-%b-%Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    for col in FINAL_COLUMNS:
        if col not in merged.columns:
            merged[col] = None
    return merged[FINAL_COLUMNS]
