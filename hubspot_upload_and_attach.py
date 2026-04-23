#!/usr/bin/env python3
"""
hubspot_files_uploader.py

Reads attachments_manifest.csv (produced by the downloader), uploads files to HubSpot Files API,
creates a Note with hs_attachment_ids referencing the uploaded file, and associates the Note to a Deal.

Updates:
 - tolerant column detection: accepts many common variants of column names for task_gid, attachment_name, local_path, etc.
 - optional mapping JSON to explicitly map manifest column names to canonical names.

Usage:
  export HUBSPOT_TOKEN="...your private app token..."
  python hubspot_files_uploader.py --manifest attachments_manifest.csv --file-folder "/AsanaUploads"
"""
import os
import time
import argparse
import requests
import csv
import json
from pathlib import Path
from tqdm import tqdm

HUBSPOT_API_BASE = "https://api.hubapi.com"

# Common synonyms the uploader will accept when reading manifest
TASK_GID_CANDIDATES = ["task_gid","task gid","task id","Task ID","Task Gid","Task GID"]
ATTACHMENT_GID_CANDIDATES = ["attachment_gid","attachment gid","attachment id","Attachment GID","Attachment Id"]
ATTACHMENT_NAME_CANDIDATES = ["attachment_name","attachment name","Attachment Name","Attachment"]
LOCAL_PATH_CANDIDATES = ["local_path","local path","local_path","localpath","local","file_path","file path","localPath"]

from requests_toolbelt.multipart.encoder import MultipartEncoder

import re
from pathlib import Path

_filename_clean_re = re.compile(r'[^A-Za-z0-9\-\._() \[\]]+')

def sanitize_filename(name: str, fallback="file"):
    """Return a safe filename by removing problematic characters and trimming length."""
    if not name:
        return fallback
    # Remove leading/trailing whitespace and nulls
    s = str(name).strip()
    # If name looks like "some/path/file.txt", take the basename
    s = Path(s).name
    # Remove unwanted characters
    s = _filename_clean_re.sub('', s)
    # collapse spaces
    s = re.sub(r'\s+', ' ', s).strip()
    # limit length
    if len(s) > 200:
        # keep extension if present
        p = Path(s)
        ext = p.suffix
        base = p.stem[:200 - len(ext)]
        s = base + ext
    if not s:
        return fallback
    return s

def make_unique_name(desired: str, used_names: set):
    """
    If desired not in used_names return desired.
    Otherwise append _1, _2 ... before extension to make unique.
    """
    desired = sanitize_filename(desired)
    if desired not in used_names:
        used_names.add(desired)
        return desired
    p = Path(desired)
    base = p.stem
    ext = p.suffix
    i = 1
    while True:
        candidate = f"{base}_{i}{ext}"
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
        i += 1


def get_hs_headers(token, extra=None):
    h = {"Authorization": f"Bearer {token}"}
    if extra:
        h.update(extra)
    return h

def upload_file_to_hubspot(local_path: Path, token: str,
                            folder_path="/",
                            access="PUBLIC_INDEXABLE",
                            desired_name: str = None,
                            max_retries=3,
                            debug=False):
    """
    Upload file to HubSpot preserving desired_name as the uploaded filename.
    - local_path: Path to local file
    - desired_name: the filename to present in HubSpot's Files UI (will be sanitized)
    - folder_path: folderPath multipart field (e.g. "/")
    - access: PUBLIC_INDEXABLE / PRIVATE etc.
    Returns (file_id, response_json_or_text)
    """
    endpoint = f"{HUBSPOT_API_BASE}/files/v3/files"

    if not folder_path:
        raise ValueError("folder_path must be non-empty (e.g. '/')")

    options = {"access": access}
    options_str = json.dumps(options)

    # Determine the filename to send: prefer desired_name, else base local filename
    if desired_name and str(desired_name).strip():
        filename_to_send = sanitize_filename(desired_name, fallback=local_path.name)
    else:
        filename_to_send = sanitize_filename(local_path.name, fallback="file")

    for attempt in range(1, max_retries + 1):
        with open(local_path, "rb") as fh:
            mp = MultipartEncoder(
                fields={
                    # set the filename in the multipart tuple to filename_to_send
                    "file": (filename_to_send, fh, "application/octet-stream"),
                    "folderPath": folder_path,
                    "options": options_str
                }
            )
            headers = get_hs_headers(token, {"Content-Type": mp.content_type})
            try:
                resp = requests.post(endpoint, headers=headers, data=mp, timeout=120)
            except requests.RequestException as e:
                if debug:
                    print(f"[upload attempt {attempt}] exception:", e)
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                return None, str(e)

        if resp.status_code in (200, 201):
            try:
                j = resp.json()
            except Exception:
                return None, resp.text
            file_id = j.get("id") or j.get("objectId") or (j.get("metadata") or {}).get("id")
            return file_id, j
        elif resp.status_code == 429:
            retry = resp.headers.get("Retry-After")
            wait = int(retry) if retry and retry.isdigit() else 2 ** attempt
            if debug:
                print("Rate limited; waiting", wait)
            time.sleep(wait)
            continue
        else:
            if debug:
                try:
                    print("Upload failed:", resp.status_code, resp.json())
                except Exception:
                    print("Upload failed:", resp.status_code, resp.text)
            return None, resp.text

    return None, "max retries exceeded"

def create_note_with_attachment(file_id, attachment_name, token, note_body=None):
    endpoint = f"{HUBSPOT_API_BASE}/crm/v3/objects/notes"
    headers = get_hs_headers(token, {"Content-Type": "application/json"})
    payload = {
        "properties": {
            "hs_note_body": note_body or f"Imported attachment {attachment_name}",
            "hs_timestamp": str(int(time.time() * 1000)),
            "hs_attachment_ids": str(file_id)
        }
    }
    resp = requests.post(endpoint, headers=headers, json=payload)
    if resp.status_code in (200,201):
        j = resp.json()
        note_id = j.get("id") or j.get("objectId")
        return note_id, j
    else:
        return None, resp.text

def associate_note_to_deal(note_id, deal_id, token):
    headers = get_hs_headers(token)
    endpoints = [
        f"{HUBSPOT_API_BASE}/crm/v3/objects/notes/{note_id}/associations/deal/{deal_id}/214",
        f"{HUBSPOT_API_BASE}/crm/v3/objects/notes/{note_id}/associations/deals/{deal_id}",
        f"{HUBSPOT_API_BASE}/crm/v3/objects/notes/{note_id}/associations/deals/{deal_id}/214",
    ]
    for ep in endpoints:
        try:
            resp = requests.put(ep, headers=headers)
            if resp.status_code in (200,201,204):
                return True, resp.text
        except Exception:
            pass
    try:
        ep2 = f"{HUBSPOT_API_BASE}/crm/v4/objects/notes/{note_id}/associations/deals"
        body = {"toObjectId": deal_id, "associationTypeId": "2"}
        resp2 = requests.post(ep2, headers=headers, json=body)
        if resp2.status_code in (200,201,204):
            return True, resp2.text
    except Exception:
        pass
    return False, "association_failed"

def find_deal_by_task_gid(task_gid, token, deal_task_prop_candidates=None):
    headers = get_hs_headers(token, {"Content-Type": "application/json"})
    endpoint = f"{HUBSPOT_API_BASE}/crm/v3/objects/deals/search"
    if deal_task_prop_candidates is None:
        deal_task_prop_candidates = ["task_gid","task id","task gid","Task ID","Task Gid"]
    results_ids = []
    for prop in deal_task_prop_candidates:
        payload = {
            "filterGroups":[{"filters":[{"propertyName": prop, "operator":"EQ", "value": str(task_gid)}]}],
            "properties": ["dealname"],
            "limit": 10
        }
        resp = requests.post(endpoint, headers=headers, json=payload)
        if resp.status_code not in (200,201):
            continue
        j = resp.json()
        results = j.get("results", [])
        if results:
            return [r.get("id") for r in results]
    return []

def detect_column(row0, candidates):
    """Given first CSV header row or dict keys, detect a key matching candidates (case-insensitive)."""
    keys = [k for k in row0.keys()]
    low_map = {k.lower(): k for k in keys}
    for cand in candidates:
        if cand in low_map:
            return low_map[cand]
        # try lowercase match of candidate
        if cand.lower() in low_map:
            return low_map[cand.lower()]
    # try fuzzy-ish: any key whose lowercase equals candidate with punctuation removed
    cleaned_keys = {re.sub(r'[^a-z0-9]', '', k.lower()): k for k in keys}
    for cand in candidates:
        s = re.sub(r'[^a-z0-9]', '', cand.lower())
        if s in cleaned_keys:
            return cleaned_keys[s]
    return None

import re

def best_field_name(row0, candidates):
    # row0 is a dict-like of header->value or just the header keys; this function returns the actual header present
    # Accepts candidates as list of possible alternatives (lowercase accepted)
    keys = [k for k in row0.keys()]
    # exact candidate match (case-insensitive)
    for cand in candidates:
        for k in keys:
            if k.strip().lower() == cand.strip().lower():
                return k
    # case-insensitive substring match
    for cand in candidates:
        for k in keys:
            if cand.strip().lower() in k.strip().lower():
                return k
    # normalized match
    norm = lambda s: re.sub(r'[^a-z0-9]', '', s.strip().lower())
    nkeys = {norm(k): k for k in keys}
    for cand in candidates:
        nc = norm(cand)
        if nc in nkeys:
            return nkeys[nc]
    return None

def read_manifest(manifest_path, mapping=None):
    rows = []
    with open(manifest_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        hdrs = reader.fieldnames or []
        # mapping: optional dict mapping canonical names -> manifest column names
        for r in reader:
            rows.append(r)
    return rows, hdrs

def find_manifest_column_name(headers, preferred_list):
    # return header name from headers that matches any candidate in preferred_list
    hdrs = list(headers)
    # try exact lower matches
    for cand in preferred_list:
        for h in hdrs:
            if h.strip().lower() == cand.strip().lower():
                return h
    # substring match
    for cand in preferred_list:
        for h in hdrs:
            if cand.strip().lower() in h.strip().lower():
                return h
    # normalized match
    norm = lambda s: re.sub(r'[^a-z0-9]', '', s.strip().lower())
    normalized = {norm(h): h for h in hdrs}
    for cand in preferred_list:
        nc = norm(cand)
        if nc in normalized:
            return normalized[nc]
    return None

def main(args):
    token = os.environ.get("HUBSPOT_TOKEN")
    if not token:
        raise SystemExit("Please set HUBSPOT_TOKEN environment variable (private app token).")

    manifest = Path(args.manifest)
    if not manifest.exists():
        raise SystemExit("manifest not found")

    folder_path = args.file_folder or "/AsanaUploads"
    dry_run = args.dry_run
    mapping_file = args.mapping

    # load manifest rows and headers
    with open(manifest, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    # if mapping json provided, load explicit mapping (canonical->manifestcol)
    explicit_map = {}
    if mapping_file:
        mf = Path(mapping_file)
        if mf.exists():
            explicit_map = json.loads(mf.read_text(encoding='utf-8'))
            # normalize keys/values to strings
            explicit_map = {str(k).strip(): str(v).strip() for k,v in explicit_map.items()}

    # determine which manifest columns to use for required fields
    # 1) task_gid column in manifest
    if "task_gid" in explicit_map:
        manifest_task_col = explicit_map["task_gid"]
    else:
        manifest_task_col = find_manifest_column_name(headers, TASK_GID_CANDIDATES) or None

    # 2) attachment_gid
    if "attachment_gid" in explicit_map:
        manifest_att_gid_col = explicit_map["attachment_gid"]
    else:
        manifest_att_gid_col = find_manifest_column_name(headers, ATTACHMENT_GID_CANDIDATES) or None

    # 3) attachment_name
    if "attachment_name" in explicit_map:
        manifest_att_name_col = explicit_map["attachment_name"]
    else:
        manifest_att_name_col = find_manifest_column_name(headers, ATTACHMENT_NAME_CANDIDATES) or None

    # 4) local_path
    if "local_path" in explicit_map:
        manifest_local_col = explicit_map["local_path"]
    else:
        manifest_local_col = find_manifest_column_name(headers, LOCAL_PATH_CANDIDATES) or None

    # If any required column not found, error with helpful message listing headers
    missing = []
    if not manifest_task_col:
        missing.append("task_gid (candidates: {})".format(", ".join(TASK_GID_CANDIDATES)))
    if not manifest_att_gid_col:
        missing.append("attachment_gid (candidates: {})".format(", ".join(ATTACHMENT_GID_CANDIDATES)))
    if not manifest_att_name_col:
        missing.append("attachment_name (candidates: {})".format(", ".join(ATTACHMENT_NAME_CANDIDATES)))
    if not manifest_local_col:
        missing.append("local_path (candidates: {})".format(", ".join(LOCAL_PATH_CANDIDATES)))
    if missing:
        raise SystemExit("Manifest missing required columns: {}\nAvailable headers: {}".format("; ".join(missing), ", ".join(headers)))

    out_log = Path(args.log) if args.log else Path("uploader_log.csv")
    out_fields = ["task_gid","attachment_gid","attachment_name","local_path","hubspot_file_id","note_id","associated_deal_id","status","detail"]
    with open(out_log, "w", newline='', encoding="utf-8") as logfile:
        writer = csv.DictWriter(logfile, fieldnames=out_fields)
        writer.writeheader()

        for r in tqdm(rows, desc="Uploading files"):
            # read fields using detected manifest column names
            task_gid = r.get(manifest_task_col) or r.get("task_gid") or r.get("Task ID") or r.get("Task Gid")
            att_gid = r.get(manifest_att_gid_col)
            att_name = r.get(manifest_att_name_col)
            local_path = r.get(manifest_local_col) or ""

            if not task_gid or str(task_gid).strip() == "":
                writer.writerow({"task_gid":"","attachment_gid":att_gid,"attachment_name":att_name,"local_path":local_path,"status":"skipped","detail":"no task_gid"})
                continue

            if not local_path:
                writer.writerow({"task_gid":task_gid,"attachment_gid":att_gid,"attachment_name":att_name,"local_path":local_path,"status":"skipped","detail":"no local file path"})
                continue
            p = Path(local_path)
            if not p.exists():
                writer.writerow({"task_gid":task_gid,"attachment_gid":att_gid,"attachment_name":att_name,"local_path":local_path,"status":"skipped","detail":"file missing"})
                continue

            # find hubspot deal(s) with property task_gid; try candidate property names
            deal_prop_candidates = [args.deal_task_prop] if args.deal_task_prop else []
            # include common variants (lower/upper)
            deal_prop_candidates += ["task_gid","task id","task gid","Task ID","Task Gid"]
            deal_ids = find_deal_by_task_gid(task_gid, token, deal_prop_candidates)
            if not deal_ids:
                writer.writerow({"task_gid":task_gid,"attachment_gid":att_gid,"attachment_name":att_name,"local_path":local_path,"status":"no_deal","detail":"no matching deal for task_gid"})
                continue

            if dry_run:
                writer.writerow({"task_gid":task_gid,"attachment_gid":att_gid,"attachment_name":att_name,"local_path":local_path,"status":"dry_run","detail":"would upload"})
                continue

            # upload file
            fid, resp = upload_file_to_hubspot(p, token, folder_path)
            if not fid:
                writer.writerow({"task_gid":task_gid,"attachment_gid":att_gid,"attachment_name":att_name,"local_path":local_path,"status":"upload_failed","detail":str(resp)[:200]})
                continue
            hubspot_file_id = str(fid)

            # create note with attachment
            nid, note_resp = create_note_with_attachment(hubspot_file_id, att_name or p.name, token)
            if not nid:
                writer.writerow({"task_gid":task_gid,"attachment_gid":att_gid,"attachment_name":att_name,"local_path":local_path,"hubspot_file_id":hubspot_file_id,"status":"note_failed","detail":str(note_resp)[:200]})
                continue
            note_id = str(nid)

            # associate note to first matching deal
            associated = False
            assoc_detail = None
            for deal_id in deal_ids:
                ok, detail = associate_note_to_deal(note_id, deal_id, token)
                if ok:
                    associated = True
                    assoc_detail = deal_id
                    break
                else:
                    assoc_detail = detail
            status = "ok" if associated else "assoc_failed"
            writer.writerow({
                "task_gid":task_gid,
                "attachment_gid":att_gid,
                "attachment_name":att_name,
                "local_path":local_path,
                "hubspot_file_id":hubspot_file_id,
                "note_id":note_id,
                "associated_deal_id":assoc_detail if associated else "",
                "status":status,
                "detail": ("" if associated else str(assoc_detail))[:400]
            })

    print("Upload complete. Log:", out_log)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--manifest", "-m", default="attachments_manifest.csv", help="Manifest CSV produced by downloader")
    p.add_argument("--file-folder", "-f", default="/AsanaUploads", help="folderPath in HubSpot Files (folderPath will be created if not exists)")
    p.add_argument("--log", "-l", help="output CSV log (default uploader_log.csv)")
    p.add_argument("--dry-run", action="store_true", help="do everything except actually upload to HubSpot")
    p.add_argument("--deal-task-prop", default="task_gid", help="HubSpot deal property name that stores the Asana task_gid (default 'task_gid')")
    p.add_argument("--mapping", "-map", help="Optional JSON file mapping canonical names to manifest columns, e.g. {\"task_gid\":\"Task Gid\",\"local_path\":\"local_path\"}")
    args = p.parse_args()
    main(args)

