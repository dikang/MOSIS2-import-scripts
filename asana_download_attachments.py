#!/usr/bin/env python3
"""
asana_attachments_downloader.py

Downloads attachments from Asana for a list of project GIDs and writes attachments_manifest.csv

Changes:
 - Manifest contains canonical columns AND alias columns for compatibility:
    - `task_gid` (canonical), plus `Task ID` and `Task Gid` (aliases)
    - `attachment_name` (canonical), plus `Attachment Name` (alias)
Usage:
  export ASANA_TOKEN="...your token..."
  python asana_attachments_downloader.py --projects projects.txt --outdir ./asana_downloads --manifest attachments_manifest.csv
"""
import os
import time
import argparse
import requests
import csv
from pathlib import Path
from tqdm import tqdm

ASANA_API_BASE = "https://app.asana.com/api/1.0"

def get_headers(token):
    return {"Authorization": f"Bearer {token}"}

def safe_get(url, token, params=None, stream=False, max_retries=5):
    headers = get_headers(token)
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, params=params, stream=stream)
        if resp.status_code == 429:
            retry = resp.headers.get("Retry-After")
            wait = int(retry) if retry and retry.isdigit() else (2 ** attempt)
            print(f"Rate limited, sleeping {wait}s (attempt {attempt+1}/{max_retries})")
            time.sleep(wait)
            continue
        return resp
    raise RuntimeError(f"Failed GET {url} after {max_retries} retries")

def list_tasks_for_project(project_gid, token):
    url = f"{ASANA_API_BASE}/projects/{project_gid}/tasks"
    params = {"opt_fields": "gid,name,memberships.section"}
    resp = safe_get(url, token, params=params)
    resp.raise_for_status()
    return resp.json().get("data", [])

def list_attachments_for_task(task_gid, token):
    url = f"{ASANA_API_BASE}/tasks/{task_gid}/attachments"
    resp = safe_get(url, token)
    resp.raise_for_status()
    return resp.json().get("data", [])

def get_attachment_info(attachment_gid, token):
    url = f"{ASANA_API_BASE}/attachments/{attachment_gid}"
    params = {"opt_fields": "gid,name,download_url,host"}
    resp = safe_get(url, token, params=params)
    resp.raise_for_status()
    return resp.json().get("data", {})

def download_file(download_url, local_path, token, max_retries=5):
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(max_retries):
        r = requests.get(download_url, headers=headers, stream=True)
        if r.status_code == 429:
            retry = r.headers.get("Retry-After")
            wait = int(retry) if retry and retry.isdigit() else 2 ** attempt
            print(f"Rate limited by Asana when downloading. Sleeping {wait}s")
            time.sleep(wait)
            continue
        if r.status_code in (200, 201):
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        else:
            print(f"Download failed {r.status_code} for {download_url}")
            time.sleep(1)
    return False

def main(args):
    token = os.environ.get("ASANA_TOKEN")
    if not token:
        raise SystemExit("Please set ASANA_TOKEN environment variable.")

    projects_file = Path(args.projects)
    if not projects_file.exists():
        raise SystemExit("projects file not found")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    manifest_path = Path(args.manifest)
    # canonical plus aliases
    fields = [
        "project_gid","project_name",
        "task_gid","Task ID","Task Gid",
        "task_name",
        "attachment_gid",
        "attachment_name","Attachment Name",
        "download_url","local_path"
    ]
    rows = []

    project_gids = [line.strip() for line in projects_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    print(f"Found {len(project_gids)} projects to scan")

    for project_gid in project_gids:
        print(f"Listing tasks for project {project_gid} ...")
        tasks = list_tasks_for_project(project_gid, token)
        for t in tqdm(tasks, desc=f"Project {project_gid} tasks"):
            task_gid = t.get("gid")
            task_name = t.get("name", "")
            try:
                atts = list_attachments_for_task(task_gid, token)
            except Exception as e:
                print(f"Failed to list attachments for task {task_gid}: {e}")
                atts = []
            for a in atts:
                att_gid = a.get("gid")
                try:
                    info = get_attachment_info(att_gid, token)
                except Exception as e:
                    print(f"Failed to get attachment info {att_gid}: {e}")
                    continue
                download_url = info.get("download_url") or info.get("permanent_url") or ""
                att_name = info.get("name") or f"{att_gid}"
                safe_name = "".join(c for c in att_name if c.isprintable()).replace("/", "_").replace("\\", "_")
                #local_fname = f"{task_gid}__{att_gid}__{safe_name}"
                local_fname = f"{safe_name}__{task_gid}__{att_gid}"
                local_path = outdir / local_fname
                success = False
                if download_url:
                    success = download_file(download_url, local_path, token)
                    if not success:
                        time.sleep(0.5)
                        try:
                            info2 = get_attachment_info(att_gid, token)
                            download_url2 = info2.get("download_url") or info2.get("permanent_url") or ""
                            if download_url2 and download_url2 != download_url:
                                success = download_file(download_url2, local_path, token)
                        except Exception:
                            pass
                else:
                    print(f"No download_url for attachment {att_gid}, skipping.")
                local_path_str = str(local_path) if success else ""
                rows.append({
                    "project_gid": project_gid,
                    "project_name": "",
                    "task_gid": task_gid,
                    "Task ID": task_gid,
                    "Task Gid": task_gid,
                    "task_name": task_name,
                    "attachment_gid": att_gid,
                    "attachment_name": att_name,
                    "Attachment Name": att_name,
                    "download_url": download_url,
                    "local_path": local_path_str
                })

    # write manifest CSV (use canonical order but include aliases as columns)
    write_fields = fields
    with open(manifest_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=write_fields)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print("Done. Manifest written to", manifest_path)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--projects", "-p", default="projects.txt", help="Text file with project GIDs (one per line)")
    p.add_argument("--outdir", "-o", default="asana_downloads", help="Directory to save attachments")
    p.add_argument("--manifest", "-m", default="attachments_manifest.csv", help="Output manifest CSV")
    args = p.parse_args()
    main(args)

