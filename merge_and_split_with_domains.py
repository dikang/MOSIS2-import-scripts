#!/usr/bin/env python3
"""
merge_and_split_with_domains.py

Merge many CSVs into merged.xlsx and split into merged_deals.xlsx, merged_company.xlsx, merged_contact.xlsx.

Key changes:
 - Contact 'Name' in contact file comes from merged['Name.1'] if present (fallback to merged['Name']).
 - Contact 'Name' is split into First Name and Last Name:
     - single token -> First Name (Last Name blank)
     - multiple tokens -> First token = First Name, rest joined = Last Name

Other preserved behavior: domain inference, NDA autofill, task_gid, pipeline inference, MPW Foundries preservation, etc.

Usage:
  pip install pandas openpyxl
  python merge_and_split_with_domains.py -i ./csv_folder -o merged.xlsx
"""
import os
import glob
import json
import argparse
from difflib import get_close_matches
import pandas as pd
import numpy as np
import re
import pathlib

# ---------------------------
# Config / constants
# ---------------------------
FORCED_HEADER_MAP = {
    "project name": "Deal Name",
    "projects": "Deal Stage",
#    "assignee email": "Deal Owner",
    "assignee email": "Task Assignee",
    "assignee": "Assignee",
    "contact email": "Contact Email",
    "phone number": "Phone Number",
    "organization": "Company Name",
    "name": "Name",
    "notes": "Notes",
    "request details": "Request Details",
    "literature": "Literature",
    "nda type": "Nda Type",
    "section/column": "Section/Column",
    "workflow stage": "Workflow Stage",
    "parent task": "Parent Task",
    "deal owner": "Deal Owner",
    "task id": "Task ID" 
}

DEALS_COLUMNS = ["Deal Owner", "Deal Name", "Deal Stage", "Pipeline", "Nda Type", "Literature", "Request Details", "Notes", "Task Assignee", "Contact Owner", "Company Owner", "createdate"]
COMPANY_COLUMNS = ["Company Name"]
CONTACT_COLUMNS = ["Name", "Contact Email", "Phone Number"]

COMPANY_OWNER_EMAIL = "rkapadia@usc.edu"
CONTACT_OWNER_EMAIL = "rkapadia@usc.edu"
DEAL_OWNER_EMAIL = "rkapadia@usc.edu"

# ---------------------------
# Helpers
# ---------------------------
def normalize_header(h):
    if pd.isna(h):
        return ""
    return str(h).strip().lower()

def load_mapping(mapping_path):
    if not mapping_path:
        return {}
    with open(mapping_path, 'r', encoding='utf-8') as f:
        raw = json.load(f)
    return {normalize_header(k): v.strip() for k, v in raw.items()}

def fuzzy_match_one(header, candidates, cutoff):
    if not cutoff:
        return None
    matches = get_close_matches(header, candidates, n=1, cutoff=cutoff)
    return matches[0] if matches else None

def build_master_map(all_headers_norm, explicit_map, fuzzy_threshold):
    master = {}
    for k, v in FORCED_HEADER_MAP.items():
        master[k] = v
    for k, v in explicit_map.items():
        master[k] = v
    unmapped = [h for h in all_headers_norm if h not in master]
    canonical = {h: h.replace("_", " ").strip() for h in unmapped}
    if fuzzy_threshold:
        for h in unmapped:
            other = [c for c in unmapped if c != h]
            match = fuzzy_match_one(h, other, fuzzy_threshold)
            if match:
                chosen = min(h, match)
                canonical[h] = canonical[chosen]
                canonical[match] = canonical[chosen]
    for h in unmapped:
        if h not in master:
            master[h] = canonical[h].title()
    return master

def is_date_like_series(s, min_fraction=0.8):
    ser = s.dropna().astype(str).str.strip()
    if ser.size == 0:
        return False
    parsed = pd.to_datetime(ser, errors='coerce', infer_datetime_format=True)
    frac = parsed.notna().sum() / float(ser.size)
    return frac >= min_fraction

def make_unique_list(cols):
    out = []
    counts = {}
    for col in cols:
        if col not in counts:
            counts[col] = 1
            out.append(col)
        else:
            counts[col] += 1
            base = col
            new_col = f"{base} ({counts[col]})"
            while new_col in counts:
                counts[col] += 1
                new_col = f"{base} ({counts[col]})"
            counts[new_col] = 1
            out.append(new_col)
    return out

def pipeline_from_source(filename_basename):
    if not filename_basename:
        return "Fab Service"
    first = filename_basename[0].upper()
    if first == "F":
        return "Fab Service"
    if first == "M":
        return "MPW Service"
    return "Fab Service"

def sanitize_company_to_domain(name):
    if not isinstance(name, str):
        return ""
    # remove punctuation
    clean = re.sub(r'[^a-zA-Z0-9 ]+', '', name)
    # remove common corporation suffixes
    clean = re.sub(r'\b(inc|llc|ltd|corp|corporation|co|company|gmbh|sarl|pty)\b', '', clean, flags=re.IGNORECASE)
    clean = clean.strip().lower()
    clean = clean.replace(" ", "")
    if not clean:
        return ""
    return f"{clean}.com"

def extract_domain_from_email(email):
    if not isinstance(email, str):
        return ""
    email = email.strip()
    # If format like: John Doe <john@acme.com>
    if "<" in email and ">" in email:
        try:
            email = email[email.find("<")+1:email.find(">")]
        except Exception:
            pass
    email = email.strip()
    if "@" not in email:
        return ""
    domain = email.split("@")[-1].strip()
    # Remove any trailing non-domain characters
    domain = re.sub(r'[^a-zA-Z0-9\.\-]', '', domain)
    return domain.lower()

# ---------------------------
# Main
# ---------------------------
def main(args):
    input_folder = args.input_folder
    output_master = args.output
    mapping_path = args.mapping
    fuzzy_threshold = args.fuzzy
    date_threshold = args.date_threshold

    patterns = ["*.csv"]
    files = []
    for p in patterns:
        files.extend(glob.glob(os.path.join(input_folder, p)))
    files = sorted(files)
    if not files:
        print("No CSV files found in", input_folder)
        return

    explicit_map = load_mapping(mapping_path)

    all_norm_headers = set()
    frames = []

    print(f"Found {len(files)} CSV files. Reading...")

    for fp in files:
        try:
            df = pd.read_csv(fp, dtype=object, encoding='utf-8', low_memory=False)
        except Exception:
            df = pd.read_csv(fp, dtype=object, low_memory=False)

        # If Name exists and Organization missing or empty, set Organization = Name per-row
        cols = list(df.columns)
        org_col = None
        name_col = None
        for c in cols:
            nc = normalize_header(c)
            if nc == "organization" and org_col is None:
                org_col = c
            if nc == "name" and name_col is None:
                name_col = c

        if name_col is not None and org_col is None:
            df["Organization"] = df[name_col]
            org_col = "Organization"
        if name_col is not None and org_col is not None:
            df[org_col] = df[org_col].replace(r'^\s*$', np.nan, regex=True)
            df[name_col] = df[name_col].replace(r'^\s*$', np.nan, regex=True)
            df[org_col] = df[org_col].fillna(df[name_col])

        # create __orig__ copies
        for c in list(df.columns):
            oc = f"__orig__{c}"
            if oc not in df.columns:
                df[oc] = df[c]

        # add source marker for pipeline inference
        basename = os.path.basename(fp)
        df["__source_file"] = basename

        frames.append((fp, df))
        for c in df.columns:
            if str(c).startswith("__"):
                continue
            all_norm_headers.add(normalize_header(c))

    master_map = build_master_map(all_norm_headers, explicit_map, fuzzy_threshold)
    print("Header mapping (normalized -> final):")
    for k in sorted(master_map.keys()):
        print(f"  {k!r} -> {master_map[k]!r}")

    transformed = []
    for fp, df in frames:
        rename_map = {}
        for c in list(df.columns):
            if str(c).startswith("__"):
                continue
            nc = normalize_header(c)
            if nc in master_map:
                rename_map[c] = master_map[nc]
            else:
                pretty = re.sub(r'\s+', ' ', str(c)).strip()
                rename_map[c] = pretty
        df2 = df.rename(columns=rename_map)
        new_cols = list(df2.columns)
        unique_cols = make_unique_list(new_cols)
        if unique_cols != new_cols:
            df2.columns = unique_cols
        transformed.append(df2)

    merged = pd.concat(transformed, sort=False, ignore_index=True)
    merged = merged.replace(r'^\s*$', np.nan, regex=True)

    # copy first
    merged["Asana Projects"] = merged["Deal Stage"]

    # MPW Service
    merged["M2 Service"] = ""

    merged.loc[
        merged["Asana Projects"].str.contains("Fab ", na=False),
        "M2 Service"
    ] = "Fab Service"
    
    merged.loc[
        merged["Asana Projects"].str.contains("MPW ", na=False),
        "M2 Service"
    ] = "MPW Service"
    
    # Auto-fill Nda Type from Notes when missing
    if "Notes" in merged.columns:
        notes_series = merged["Notes"].astype(str).fillna("").str.strip()
        two_way_pattern = r"\b2[\s\-]*way[\s\-]*nda\b"
        three_way_pattern = r"\b3[\s\-]*way[\s\-]*nda\b"
        if "Nda Type" not in merged.columns:
            merged["Nda Type"] = np.nan
        nda_missing_mask = merged["Nda Type"].isna() | (merged["Nda Type"].astype(str).str.strip() == "")
        mask_2way = nda_missing_mask & notes_series.str.contains(two_way_pattern, case=False, regex=True, na=False)
        if mask_2way.any():
            merged.loc[mask_2way, "Nda Type"] = "2 Way NDA"
        mask_3way = nda_missing_mask & notes_series.str.contains(three_way_pattern, case=False, regex=True, na=False)
        if mask_3way.any():
            merged.loc[mask_3way, "Nda Type"] = "3 Way NDA"

    # Split comma-separated foundries, duplicate rows, and keep one value per row
    merged["Mpw Foundries"] = merged["Mpw Foundries"].fillna("").astype(str)

    merged = merged.assign(
        **{"Mpw Foundries": merged["Mpw Foundries"].str.split(",")}
    ).explode("Mpw Foundries")

    merged["Mpw Foundries"] = merged["Mpw Foundries"].str.strip()

    # Create a counter per original Task ID
    merged["_dup_index"] = merged.groupby("Task ID").cumcount()
    
    # Make Task ID unique
    merged["Task ID"] = merged["Task ID"].astype(str)
    
    merged["Task ID"] = merged.apply(
        lambda row: row["Task ID"] if row["_dup_index"] == 0
        else f"{row['Task ID']}_{row['_dup_index']}",
        axis=1
    )
    
    # Clean up
    merged.drop(columns=["_dup_index"], inplace=True)

    merged = merged.rename(columns={"Name": "Deal Name"})
    merged["Deal Name"] = "[A] " + merged["Deal Name"]    # Replace values based on substring match
    merged.loc[merged["Deal Stage"].str.contains("Fab ", na=False), "Deal Stage"] = \
        "Export Control Check (Fab [1. Customer Evaluation])"

    merged.loc[merged["Deal Stage"].str.contains("MPW ", na=False), "Deal Stage"] = \
        "Export Control Check (MPW [1. Customer Evaluation])"

    # Split comma-separated foundries, duplicate rows, and keep one value per row
    merged["Mpw Foundries"] = merged["Mpw Foundries"].fillna("").astype(str)

    merged = merged.assign(
        **{"Mpw Foundries": merged["Mpw Foundries"].str.split(",")}
    ).explode("Mpw Foundries")

    merged["Mpw Foundries"] = merged["Mpw Foundries"].str.strip()

#    merged = merged.rename(columns={"Name": "Deal Name"})

    # Append the foundry value to Deal Name
    mask = merged["Mpw Foundries"] != ""
    merged.loc[mask, "Deal Name"] = (
    merged.loc[mask, "Deal Name"] + "-" + merged.loc[mask, "Mpw Foundries"] 
    )

    merged = merged.rename(columns={"Mpw Foundries": "Target Foundry"})
    
#    merged = merged.rename(columns={"Name": "Deal Name"})

    # Ensure it's string
    merged["Name.1"] = merged["Name.1"].fillna("").astype(str)

    # convert Target Foundry: Intel Foundry -> Intel
    merged["Target Foundry"] = merged["Target Foundry"].str.replace(
    r"^\s*Intel Foundry\s*$", "Intel", regex=True
    )

    # Split by spaces
    name_split = merged["Name.1"].str.strip().str.split()

    # First name = first word
    merged["First Name"] = name_split.str[0]

    # Last name = everything after first word (if exists)
    merged["Last Name"] = name_split.str[1:].str.join(" ")

    merged = merged.rename(columns={"Name.1": "Full Name"})

    # Replace empty strings with NaN if desired
    merged["First Name"].replace("", pd.NA, inplace=True)
    merged["Last Name"].replace("", pd.NA, inplace=True)

    # Fill Company Name from Deal Name if missing
    if "Company Name" in merged.columns and "Deal Name" in merged.columns:
        merged["Company Name"] = merged["Company Name"].fillna(merged["Deal Name"])

    if "Company Name" not in merged.columns:
        for c in merged.columns:
            if str(c).startswith("__orig__"):
                orig_name = c[len("__orig__"):]
                if normalize_header(orig_name) == "organization":
                    merged["Company Name"] = merged[c]
                    break

    if "Company Name" not in merged.columns and "Name" in merged.columns:
        merged["Company Name"] = merged["Name"]

    if "Organization" not in merged.columns:
        if "Company Name" in merged.columns:
            merged["Organization"] = merged["Company Name"]
        else:
            for c in merged.columns:
                if str(c).startswith("__orig__"):
                    orig_name = c[len("__orig__"):]
                    if normalize_header(orig_name) == "organization":
                        merged["Organization"] = merged[c]
                        break

    # protect important columns including MPW Foundries and Task ID
    protected_cols = set(DEALS_COLUMNS + COMPANY_COLUMNS + CONTACT_COLUMNS + [
        "Deal Name", "Deal Stage", "Deal Owner", "Company Name", "Organization", "Name", "MPW Foundries", "Task ID"
    ])

    # Extract all email addresses from "Contact Email"
    merged["Contact Email"] = merged["Contact Email"].fillna("").astype(str)

    merged["Contact Email"] = merged["Contact Email"].str.findall(
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    )

    # Duplicate rows when multiple email addresses are found
    merged = merged.explode("Contact Email", ignore_index=True)
    
    # Create a counter per original Task ID
    merged["_dup_index_m"] = merged.groupby("Task ID").cumcount()
    
    # Make Task ID unique
    merged["Task ID"] = merged["Task ID"].astype(str)
    
    merged["Task ID"] = merged.apply(
        lambda row: row["Task ID"] if row["_dup_index_m"] == 0
        else f"{row['Task ID']}_m{row['_dup_index_m']}",
        axis=1
    )

    # Make deal name unique
    merged["Deal Name"] = merged["Deal Name"].astype(str)
    
    merged["Deal Name"] = merged.apply(
        lambda row: row["Deal Name"] if row["_dup_index_m"] == 0
        else f"{row['Deal Name']}-m{row['_dup_index_m']}",
        axis=1
    )

    # Clean up
    merged.drop(columns=["_dup_index_m"], inplace=True)

    
    # Clean up
    merged["Contact Email"] = merged["Contact Email"].fillna("").str.strip()
    # merged = merged[merged["Contact Email"] != ""]  # drop rows with no email found
    
    # Append "-" and First Name to Deal Name
    merged["First Name"] = merged["First Name"].fillna("").astype(str).str.strip()
    merged["Deal Name"] = (
        merged["Deal Name"].fillna("").astype(str).str.strip()
        + "-"
        + merged["First Name"]
    )

    # Optional: remove trailing hyphen if First Name is blank
    merged["Deal Name"] = merged["Deal Name"].str.replace(r"-$", "", regex=True)

    merged["Notes"] = merged["Notes"].fillna("").astype(str)

    # Merge: Projects + Notes
    merged["Notes"] = (
        merged["Asana Projects"].str.strip() + " | " + merged["Notes"].str.strip()
    ).str.strip(" |")  # removes leading/trailing separator if one side is empty

    to_drop = []
    for c in list(merged.columns):
        if c in protected_cols:
            continue
        if str(c).startswith("__"):
            continue
        try:
            if is_date_like_series(merged[c], min_fraction=date_threshold):
                to_drop.append(c)
        except Exception:
            continue
    if to_drop:
        print("Dropping date-like columns:", to_drop)
        merged.drop(columns=to_drop, inplace=True)
    else:
        print("No date-like columns detected (or protected).")

    if "Deal Name" in merged.columns and "Project Name" not in merged.columns:
        merged["Project Name"] = merged["Deal Name"]
    if "Company Name" in merged.columns and "Organization" not in merged.columns:
        merged["Organization"] = merged["Company Name"]
    if "Organization" in merged.columns and "Company Name" not in merged.columns:
        merged["Company Name"] = merged["Organization"]

    merged["Deal Owner"] = DEAL_OWNER_EMAIL
    merged["Company Owner"] = COMPANY_OWNER_EMAIL
    merged["Contact Owner"] = CONTACT_OWNER_EMAIL
    merged["createdate"] = merged["__orig__Created At"]

    visible_cols_for_drop = [c for c in merged.columns if not str(c).startswith("__")]
    merged.dropna(subset=visible_cols_for_drop, how="all", inplace=True)

    # write master merged file
    merged_out_path = output_master
    merged.to_excel(merged_out_path, index=False, engine='openpyxl')
    print("Wrote master merged:", merged_out_path)

    exit()

    # --- deals ---
    deals_df = pd.DataFrame()
    if "Assignee Email" in merged.columns:
        owner_series = merged["Assignee Email"]
    elif "Deal Owner" in merged.columns:
        owner_series = merged["Deal Owner"]
    elif "Assignee" in merged.columns:
        owner_series = merged["Assignee"]
    else:
        owner_series = pd.Series([np.nan] * len(merged))
    deals_df["Deal Owner"] = owner_series.astype(object).where(owner_series.notna(), np.nan)

    # Deal Name from Name preferred
    if "Name" in merged.columns:
        deals_df["Deal Name"] = merged["Name"]
    elif "Deal Name" in merged.columns:
        deals_df["Deal Name"] = merged["Deal Name"]
    elif "Project Name" in merged.columns:
        deals_df["Deal Name"] = merged["Project Name"]
    else:
        deals_df["Deal Name"] = pd.Series([np.nan] * len(merged))

    deals_df["Deal Stage"] = merged.get("Deal Stage", merged.get("Projects", pd.Series([np.nan] * len(merged))))

    # Pipeline per-row from source filename
    if "__source_file" in merged.columns:
        deals_df["Pipeline"] = merged["__source_file"].fillna("").astype(str).apply(lambda s: pipeline_from_source(s.strip()))
    else:
        deals_df["Pipeline"] = "Fab Service"

    # Add task_gid from Task ID if present
    if "Task ID" in merged.columns:
        deals_df["task_gid"] = merged["Task ID"]
    else:
        # try case-insensitive fallback
        task_id_col = None
        for c in merged.columns:
            if normalize_header(c) == "task id":
                task_id_col = c
                break
        if task_id_col:
            deals_df["task_gid"] = merged[task_id_col]
        else:
            deals_df["task_gid"] = np.nan

    deals_df["Nda Type"] = merged.get("Nda Type", pd.Series([np.nan] * len(merged)))
    deals_df["Literature"] = merged.get("Literature", pd.Series([np.nan] * len(merged)))
    deals_df["Request Details"] = merged.get("Request Details", pd.Series([np.nan] * len(merged)))
    deals_df["Notes"] = merged.get("Notes", pd.Series([np.nan] * len(merged)))
    deals_df["MPW Foundries"] = merged.get("MPW Foundries", pd.Series([np.nan] * len(merged)))

    # reorder to put task_gid first if present
    cols = list(deals_df.columns)
    if "task_gid" in cols:
        cols = ["task_gid"] + [c for c in cols if c != "task_gid"]
        deals_df = deals_df[cols]

    deals_df.dropna(how="all", inplace=True)
    deals_out = os.path.splitext(merged_out_path)[0] + "_deals.xlsx"
    deals_df.to_excel(deals_out, index=False, engine='openpyxl')
    print("Wrote deals file:", deals_out)

    # --- company ---
    company_df = pd.DataFrame()
    company_df["Company Name"] = merged.get("Company Name", merged.get("Organization", pd.Series([np.nan] * len(merged))))
    company_df["Company Owner"] = COMPANY_OWNER_EMAIL

    # Build Company Domain Name by preferring contact email domains
    merged_comp = merged.copy()
    merged_comp["__cmp_norm"] = merged_comp["Company Name"].astype(str).fillna("").str.strip().str.lower()

    comp_to_domain = {}
    if "Contact Email" in merged_comp.columns:
        for idx, row in merged_comp.iterrows():
            cname = row.get("__cmp_norm","")
            email = row.get("Contact Email", "")
            if not cname:
                continue
            if cname not in comp_to_domain:
                domain = extract_domain_from_email(email) if isinstance(email, str) and email.strip() else ""
                if domain:
                    comp_to_domain[cname] = domain

    for idx, row in merged_comp.iterrows():
        cname = row.get("__cmp_norm","")
        if not cname:
            continue
        if cname not in comp_to_domain or not comp_to_domain[cname]:
            orig_name = row.get("Company Name", "") or row.get("Organization", "")
            comp_to_domain[cname] = comp_to_domain.get(cname) or sanitize_company_to_domain(orig_name)

    # Apply mapping to company_df (one row per merged row)
    company_df["Company Domain Name"] = merged_comp["Company Name"].astype(str).fillna("").str.strip().str.lower().map(
        lambda v: comp_to_domain.get(v.strip().lower(), "") if isinstance(v, str) and v.strip() else ""
    )

    company_df.dropna(how="all", inplace=True)
    comp_out = os.path.splitext(merged_out_path)[0] + "_company.xlsx"
    company_df.to_excel(comp_out, index=False, engine='openpyxl')
    print("Wrote company file:", comp_out)

    # --- contact ---
    contact_df = pd.DataFrame()

    # 1️⃣ Use Name.1 as source for contact name when available, otherwise fallback to Name
    if "Name.1" in merged.columns:
        name_source = merged["Name.1"]
    else:
        name_source = merged.get("Name", pd.Series([np.nan] * len(merged)))
    
    contact_df["Name"] = name_source
    
    # 2️⃣ Split Name into First Name and Last Name
    first_names = []
    last_names = []
    
    for val in contact_df["Name"].fillna("").astype(str):
        s = val.strip()
        if not s:
            first_names.append("")
            last_names.append("")
            continue
    
        parts = s.split()
        if len(parts) == 1:
            first_names.append(parts[0])
            last_names.append("")
        else:
            first_names.append(parts[0])
            last_names.append(" ".join(parts[1:]))
    
    contact_df["First Name"] = first_names
    contact_df["Last Name"] = last_names
    
    # 3️⃣ Email (renamed from Contact Email)
    contact_df["Email"] = merged.get("Contact Email", pd.Series([np.nan] * len(merged)))
    
    # 4️⃣ Other fields
    contact_df["Phone Number"] = merged.get("Phone Number", pd.Series([np.nan] * len(merged)))
    
    if "Organization" in merged.columns:
        contact_df["Company Name"] = merged["Organization"]
    elif "Company Name" in merged.columns:
        contact_df["Company Name"] = merged["Company Name"]
    else:
        contact_df["Company Name"] = pd.Series([np.nan] * len(merged))
    
    contact_df["Contact Owner"] = CONTACT_OWNER_EMAIL
    
    # 5️⃣ Remove rows where First Name, Last Name, and Email are all empty
    contact_df[["First Name", "Last Name", "Email"]] = contact_df[["First Name", "Last Name", "Email"]].fillna("")
    
    mask_empty = (
        (contact_df["First Name"].str.strip() == "") &
        (contact_df["Last Name"].str.strip() == "") &
        (contact_df["Email"].str.strip() == "")
    )
    
    contact_df = contact_df[~mask_empty]
    
    # 6️⃣ Final column order
    ordered_cols = ["First Name", "Last Name", "Email", "Phone Number", "Company Name", "Contact Owner"]
    contact_df = contact_df[ordered_cols]
    
    contact_out = os.path.splitext(merged_out_path)[0] + "_contact.xlsx"
    contact_df.to_excel(contact_out, index=False, engine='openpyxl')
    print("Wrote contact file:", contact_out)

    print("All done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge CSVs and split into deals/company/contact with domain inference.")
    parser.add_argument("--input_folder", "-i", required=True, help="Folder containing CSV files")
    parser.add_argument("--output", "-o", default="merged.xlsx", help="Output master Excel filename")
    parser.add_argument("--mapping", "-m", default=None, help="Optional JSON mapping file")
    parser.add_argument("--fuzzy", "-f", type=float, default=0.75, help="Fuzzy matching cutoff 0..1 (set 0 to disable)")
    parser.add_argument("--date_threshold", "-d", type=float, default=0.8, help="Fraction of non-empty values that parse as dates to drop column")
    args = parser.parse_args()

    if args.fuzzy and args.fuzzy <= 0:
        args.fuzzy = None

    main(args)

