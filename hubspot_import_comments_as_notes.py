#!/usr/bin/env python3
"""
hubspot_import_comments_as_notes.py

Read Asana comments CSV(s) and create HubSpot Notes associated to Deals (matched by task_gid).

Behavior (updated):
 - Create the Note object first (no associations).
 - Then associate the Note to the Deal using separate association calls (more compatible across portals).
 - Robust to rate limits and returns a CSV log.

Usage:
  export HUBSPOT_TOKEN="your_hubspot_private_app_token"
  python hubspot_import_comments_as_notes.py --input ./asana_csvs --log notes_import_log.csv

Requirements:
  pip install requests python-dateutil tqdm
"""
from pathlib import Path
import os
import csv
import time
import json
import argparse
import requests
from dateutil import parser as dateparser
from tqdm import tqdm
from typing import Tuple, Any, List

# --- Config / constants ---
HUBSPOT_API_BASE = "https://api.hubapi.com"

# Candidate header names for fields in comments CSV
TASK_GID_CANDIDATES = ["task_gid", "task gid", "task id", "Task ID", "Task Gid", "Task GID"]
COMMENT_TEXT_CANDIDATES = ["comment_text", "comment", "text", "comment_text"]
AUTHOR_CANDIDATES = ["author", "created_by", "author_name", "created_by.name"]
CREATED_AT_CANDIDATES = ["created_at", "created at", "created", "created_at"]

# --- Helpers ---
def get_hs_headers(token: str, extra: dict = None) -> dict:
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if extra:
        h.update(extra)
    return h

def find_column_name(headers: List[str], candidates: List[str]):
    """Return the header name from headers matching any candidate, or None."""
    if not headers:
        return None
    lower_map = {h.lower(): h for h in headers}
    for c in candidates:
        if c in lower_map:
            return lower_map[c]
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    # substring fallback
    for c in candidates:
        for h in headers:
            if c.lower() in h.lower():
                return h
    return None

def iso_to_epoch_ms(iso_str: str) -> int:
    """Convert ISO timestamp string to milliseconds since epoch. If fails, return current ms."""
    if not iso_str:
        return int(time.time() * 1000)
    try:
        dt = dateparser.parse(iso_str)
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)

# --- HubSpot API interactions ---

def search_deals_by_task_gid(token: str, task_gid: str, prop_candidates: List[str] = None, limit: int = 10) -> List[str]:
    """
    Use the Search API to find deals where any of prop_candidates equals task_gid.
    Returns list of deal ids (strings).
    """
    if prop_candidates is None:
        prop_candidates = ["task_gid", "task id", "Task ID", "Task Gid", "task gid"]
    endpoint = f"{HUBSPOT_API_BASE}/crm/v3/objects/deals/search"
    headers = get_hs_headers(token)
    for prop in prop_candidates:
        payload = {
            "filterGroups": [
                {"filters": [{"propertyName": prop, "operator": "EQ", "value": str(task_gid)}]}
            ],
            "properties": ["dealname"],
            "limit": limit
        }
        resp = requests.post(endpoint, headers=headers, json=payload)
        if resp.status_code in (200,201):
            j = resp.json()
            results = j.get("results", [])
            if results:
                return [str(r.get("id")) for r in results]
        else:
            # handle rate-limit
            if resp.status_code == 429:
                retry = resp.headers.get("Retry-After")
                wait = int(retry) if retry and retry.isdigit() else 2
                time.sleep(wait)
                continue
            # if 400 or other, try next candidate property
    return []

def create_note_only(token: str, hs_note_body: str, hs_timestamp_ms: int) -> Tuple[bool, Any]:
    """
    Create a HubSpot Note WITHOUT associations.
    POST /crm/v3/objects/notes
    Returns (ok, response_json_or_text)
    """
    endpoint = f"{HUBSPOT_API_BASE}/crm/v3/objects/notes"
    headers = get_hs_headers(token)
    payload = {
        "properties": {
            "hs_note_body": hs_note_body,
            "hs_timestamp": str(hs_timestamp_ms)
        }
    }
    resp = requests.post(endpoint, headers=headers, json=payload)
    if resp.status_code in (200,201):
        try:
            return True, resp.json()
        except Exception:
            return True, resp.text
    elif resp.status_code == 429:
        retry = resp.headers.get("Retry-After")
        wait = int(retry) if retry and retry.isdigit() else 2
        time.sleep(wait)
        # one retry
        resp2 = requests.post(endpoint, headers=headers, json=payload)
        if resp2.status_code in (200,201):
            try:
                return True, resp2.json()
            except Exception:
                return True, resp2.text
        return False, resp2.text
    else:
        return False, resp.text

def associate_note_to_deal(token: str, note_id: str, deal_id: str) -> Tuple[bool, Any]:
    """
    Associate an existing note to a deal. Try multiple endpoints/patterns.
    Returns (ok, detail)
    """
    headers = get_hs_headers(token)
    tried = []

    # 1) Common PUT pattern attempts
    endpoints = [
        f"{HUBSPOT_API_BASE}/crm/v3/objects/notes/{note_id}/associations/deal/{deal_id}/214",
        f"{HUBSPOT_API_BASE}/crm/v3/objects/notes/{note_id}/associations/deals/{deal_id}",
        f"{HUBSPOT_API_BASE}/crm/v3/objects/notes/{note_id}/associations/deals/{deal_id}/214"
    ]
    for ep in endpoints:
        try:
            resp = requests.put(ep, headers=headers)
            if resp.status_code in (200,201,204):
                return True, resp.text
            tried.append((ep, resp.status_code, (resp.text[:300] if resp.text else "")))
        except Exception as e:
            tried.append((ep, "exception", str(e)))

    # 2) Associations v4 create with body (some portals require this)
    try:
        ep2 = f"{HUBSPOT_API_BASE}/crm/v4/objects/notes/{note_id}/associations/deals"
        body = {"toObjectId": str(deal_id), "associationTypeId": "2"}
        resp2 = requests.post(ep2, headers=headers, json=body)
        if resp2.status_code in (200,201,204):
            return True, resp2.text
        tried.append((ep2, resp2.status_code, (resp2.text[:300] if resp2.text else "")))
    except Exception as e:
        tried.append(("v4_create", "exception", str(e)))

    return False, {"tried": tried}

# --- Processing CSV(s) ---

def process_single_csv(token: str, csv_path: Path, deal_task_prop_candidates: List[str], out_writer: csv.DictWriter, dry_run: bool = False):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        task_col = find_column_name(headers, TASK_GID_CANDIDATES)
        comment_col = find_column_name(headers, COMMENT_TEXT_CANDIDATES)
        author_col = find_column_name(headers, AUTHOR_CANDIDATES)
        created_col = find_column_name(headers, CREATED_AT_CANDIDATES)

        if not task_col:
            print(f"[skip] {csv_path}: no task id column found among {TASK_GID_CANDIDATES}")
            return
        if not comment_col:
            print(f"[skip] {csv_path}: no comment text column found among {COMMENT_TEXT_CANDIDATES}")
            return

        for row in tqdm(list(reader), desc=f"Importing comments from {csv_path.name}", unit="comment"):
            task_gid = (row.get(task_col) or "").strip()
            if not task_gid:
                out_writer.writerow({"csv_file": csv_path.name, "task_gid": "", "status":"skipped","detail":"no task_gid"})
                continue
            comment_text = (row.get(comment_col) or "").strip()
            if not comment_text:
                out_writer.writerow({"csv_file": csv_path.name, "task_gid": task_gid, "status":"skipped","detail":"empty comment text"})
                continue
            author = (row.get(author_col) or "").strip() if author_col else ""
            created_at = (row.get(created_col) or "").strip() if created_col else ""
            timestamp_ms = iso_to_epoch_ms(created_at)

            # build note body: include author and original timestamp
            body_lines = []
            if author:
                body_lines.append(f"Author: {author}")
            if created_at:
                body_lines.append(f"Original created_at: {created_at}")
            body_lines.append("")  # blank line
            body_lines.append(comment_text)
            hs_note_body = "\n".join(body_lines)

            # find matching deals
            deal_ids = search_deals_by_task_gid(token, task_gid, prop_candidates=deal_task_prop_candidates)
            if not deal_ids:
                out_writer.writerow({
                    "csv_file": csv_path.name,
                    "task_gid": task_gid,
                    "status":"no_deal",
                    "detail":"no matching deal for this task_gid"
                })
                continue

            # For each matched deal try to create & associate
            created_any = False
            for did in deal_ids:
                if dry_run:
                    out_writer.writerow({
                        "csv_file": csv_path.name,
                        "task_gid": task_gid,
                        "status":"dry_run",
                        "detail":f"would create note and associate to deal {did}"
                    })
                    created_any = True
                    break

                # 1) create note (no associations)
                ok_create, resp_create = create_note_only(token, hs_note_body, timestamp_ms)
                if not ok_create:
                    out_writer.writerow({
                        "csv_file": csv_path.name,
                        "task_gid": task_gid,
                        "status":"create_failed",
                        "associated_deal_id": did,
                        "detail": str(resp_create)[:400]
                    })
                    # try next deal if any
                    continue

                # extract note id
                note_id = None
                if isinstance(resp_create, dict):
                    note_id = resp_create.get("id") or resp_create.get("objectId")
                else:
                    # if response is text, can't reliably get id
                    note_id = None

                if not note_id:
                    out_writer.writerow({
                        "csv_file": csv_path.name,
                        "task_gid": task_gid,
                        "status":"create_no_note_id",
                        "associated_deal_id": did,
                        "detail":"note created but no id returned"
                    })
                    continue

                # 2) associate created note to deal
                ok_assoc, assoc_detail = associate_note_to_deal(token, str(note_id), str(did))
                if ok_assoc:
                    out_writer.writerow({
                        "csv_file": csv_path.name,
                        "task_gid": task_gid,
                        "status":"ok",
                        "associated_deal_id": did,
                        "note_id": note_id,
                        "detail":"created and associated"
                    })
                    created_any = True
                    # if you want only a single note per comment, break here:
                    break
                else:
                    out_writer.writerow({
                        "csv_file": csv_path.name,
                        "task_gid": task_gid,
                        "status":"assoc_failed",
                        "associated_deal_id": did,
                        "note_id": note_id,
                        "detail": str(assoc_detail)[:400]
                    })
                    # try next matching deal

            if not created_any:
                out_writer.writerow({
                    "csv_file": csv_path.name,
                    "task_gid": task_gid,
                    "status":"all_failed",
                    "detail":"all create/associate attempts failed"
                })


def main(args):
    token = os.environ.get("HUBSPOT_TOKEN")
    if not token:
        raise SystemExit("Please set HUBSPOT_TOKEN environment variable (your HubSpot private app token)")

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit("Input path not found: " + str(input_path))

    log_path = Path(args.log or "notes_import_log.csv")
    with open(log_path, "w", newline='', encoding='utf-8') as lf:
        fieldnames = ["csv_file","task_gid","status","associated_deal_id","note_id","detail"]
        writer = csv.DictWriter(lf, fieldnames=fieldnames)
        writer.writeheader()

        # collect CSV files to process
        csv_files = []
        if input_path.is_dir():
            csv_files = sorted([p for p in input_path.glob("*_comments.csv")])
            if not csv_files:
                csv_files = sorted(list(input_path.glob("*.csv")))
        else:
            csv_files = [input_path]

        if not csv_files:
            print("No CSV files found to import.")
            return

        deal_task_prop_candidates = args.deal_task_prop.split(",") if args.deal_task_prop else ["task_gid","Task ID","Task Gid"]

        for csvf in csv_files:
            try:
                process_single_csv(token, csvf, deal_task_prop_candidates, writer, dry_run=args.dry_run)
            except Exception as e:
                print(f"Error processing {csvf}: {e}")
                writer.writerow({"csv_file": csvf.name, "task_gid": "", "status":"error","detail":str(e)})

    print("Done. Log:", log_path)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", "-i", default="asana_csvs", help="Input CSV file or folder containing comments CSV(s)")
    p.add_argument("--log", "-l", help="Output CSV log (default notes_import_log.csv)")
    p.add_argument("--deal-task-prop", default="task_gid", help="Comma-separated candidate HubSpot deal property names which store the Asana task id (default 'task_gid').")
    p.add_argument("--dry-run", action="store_true", help="Don't create notes, just log what would be done.")
    args = p.parse_args()
    main(args)

