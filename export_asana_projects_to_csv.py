#!/usr/bin/env python3
"""
export_asana_projects_to_csv.py

Reads a list of Asana project GIDs from a text file (default projects.txt) and exports:
 - per-project tasks CSV: <project_basename>.csv
 - per-project comments CSV: <project_basename>_comments.csv

Usage:
  export ASANA_TOKEN="your_asana_pat"
  python export_asana_projects_to_csv.py --projects projects.txt --outdir ./asana_csvs

Notes:
 - projects.txt should contain one Asana project GID per line (blank lines ignored).
 - Requires ASANA_TOKEN env var (personal access token).
 - Handles pagination and rate limits (Retry-After).
"""
import os
import time
import argparse
import requests
import csv
from pathlib import Path
from tqdm import tqdm

ASANA_API_BASE = "https://app.asana.com/api/1.0"
# default opt_fields for listing tasks; keeps it reasonably compact but includes some useful info
DEFAULT_TASK_FIELDS = "gid,name,notes,assignee,assignee.name,memberships.section,created_at,modified_at,custom_fields"

def get_headers(token):
    return {"Authorization": f"Bearer {token}"}

def safe_get(url, token, params=None, stream=False, max_retries=6):
    headers = get_headers(token)
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, stream=stream, timeout=60)
        except requests.RequestException as e:
            # network-level retry
            if attempt < max_retries:
                wait = min(2**attempt, 30)
                print(f"Network error fetching {url}: {e}. Retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise
        if resp.status_code == 429:
            retry = resp.headers.get("Retry-After")
            wait = int(retry) if retry and retry.isdigit() else (2 ** attempt)
            wait = min(wait, 60)
            print(f"Rate limited on GET {url} — sleeping {wait}s (attempt {attempt}/{max_retries})")
            time.sleep(wait)
            continue
        return resp
    raise RuntimeError(f"Failed GET {url} after {max_retries} retries")

def list_tasks_for_project(project_gid, token, opt_fields=DEFAULT_TASK_FIELDS):
    """
    Return a list of tasks (shallow dicts) for the project, paging until done.
    """
    tasks = []
    url = f"{ASANA_API_BASE}/projects/{project_gid}/tasks"
    params = {"opt_fields": opt_fields, "limit": 100}
    while True:
        resp = safe_get(url, token, params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to list tasks for project {project_gid}: HTTP {resp.status_code} {resp.text}")
        j = resp.json()
        data = j.get("data", [])
        tasks.extend(data)
        next_page = j.get("next_page") or {}
        if next_page and next_page.get("uri"):
            url = next_page["uri"]
            params = None
            continue
        break
    return tasks

def get_project_info(project_gid, token):
    url = f"{ASANA_API_BASE}/projects/{project_gid}"
    params = {"opt_fields": "gid,name"}
    resp = safe_get(url, token, params=params)
    if resp.status_code != 200:
        return {"gid": project_gid, "name": f"project_{project_gid}"}
    return resp.json().get("data", {"gid": project_gid, "name": f"project_{project_gid}"})

def list_stories_for_task(task_gid, token, max_retries=6):
    """
    Return all stories for the task (pages through results).
    """
    stories = []
    url = f"{ASANA_API_BASE}/tasks/{task_gid}/stories"
    params = {"limit": 100}
    while True:
        resp = safe_get(url, token, params=params)
        if resp.status_code != 200:
            # warn and return what we have
            print(f"Warning: could not fetch stories for task {task_gid}: HTTP {resp.status_code}")
            return stories
        j = resp.json()
        data = j.get("data", [])
        stories.extend(data)
        next_page = j.get("next_page") or {}
        if next_page and next_page.get("uri"):
            url = next_page["uri"]
            params = None
            continue
        break
    return stories

def is_comment_story(story):
    """
    Recognize comment-like stories. Asana comment stories commonly have:
      - type == 'comment'
      - or resource_subtype containing 'comment'
    """
    if not story:
        return False
    if str(story.get("type", "")).lower() == "comment":
        return True
    rs = str(story.get("resource_subtype") or "")
    if "comment" in rs.lower():
        return True
    return False

def fetch_and_write_comments_for_task(task_gid, task_name, token, csv_writer):
    stories = list_stories_for_task(task_gid, token)
    for s in stories:
        if not is_comment_story(s):
            continue
        comment_gid = s.get("gid")
        created_at = s.get("created_at")
        author_obj = s.get("created_by") or s.get("author") or {}
        author_name = ""
        if isinstance(author_obj, dict):
            author_name = author_obj.get("name") or author_obj.get("email") or ""
        comment_text = s.get("text") or s.get("html_text") or ""
        csv_writer.writerow({
            "task_gid": task_gid,
            "task_name": task_name,
            "comment_gid": comment_gid,
            "author": author_name,
            "created_at": created_at,
            "comment_text": comment_text
        })

def sanitize_basename(s):
    safe = "".join(ch for ch in s if ch.isalnum() or ch in " _-").strip()
    return safe or None

def read_project_list(projects_file: Path):
    txt = projects_file.read_text(encoding="utf-8")
    gids = [line.strip() for line in txt.splitlines() if line.strip() and not line.strip().startswith("#")]
    return gids

def write_tasks_csv(tasks, path: Path):
    # write shallow fields discovered across tasks
    if tasks:
        headers = set()
        for t in tasks:
            headers.update(t.keys())
        headers = list(sorted(headers))
    else:
        headers = ["gid", "name"]
    with open(path, "w", newline='', encoding="utf-8") as tf:
        writer = csv.DictWriter(tf, fieldnames=headers)
        writer.writeheader()
        for t in tasks:
            row = {}
            for h in headers:
                v = t.get(h)
                if isinstance(v, (dict, list)):
                    v = str(v)
                row[h] = v
            writer.writerow(row)

def main(args):
    token = os.environ.get("ASANA_TOKEN")
    if not token:
        raise SystemExit("Please set ASANA_TOKEN environment variable with your Asana PAT")

    projects_file = Path(args.projects)
    if not projects_file.exists():
        raise SystemExit(f"Projects file not found: {projects_file}")

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    project_gids = read_project_list(projects_file)
    if not project_gids:
        raise SystemExit("No project GIDs found in projects file")

    print(f"Exporting {len(project_gids)} projects listed in {projects_file}")

    for proj_gid in project_gids:
        try:
            info = get_project_info(proj_gid, token)
            proj_name = info.get("name") or f"project_{proj_gid}"
            safe_name = sanitize_basename(proj_name) or f"project_{proj_gid}"
            base_fname = f"{safe_name}"
            tasks_csv_path = outdir / f"{base_fname}.csv"
            comments_csv_path = outdir / f"{base_fname}_comments.csv"

            print(f"\nProcessing project {proj_gid} → {safe_name}")

            # List tasks in project
            tasks = list_tasks_for_project(proj_gid, token)
            print(f"  Found {len(tasks)} tasks; writing tasks CSV to {tasks_csv_path}")
            write_tasks_csv(tasks, tasks_csv_path)

            # Export comments per task
            print(f"  Scanning comments for tasks; writing comments CSV to {comments_csv_path}")
            with open(comments_csv_path, "w", newline='', encoding="utf-8") as cf:
                comment_fieldnames = ["task_gid", "task_name", "comment_gid", "author", "created_at", "comment_text"]
                comment_writer = csv.DictWriter(cf, fieldnames=comment_fieldnames)
                comment_writer.writeheader()

                for t in tqdm(tasks, desc=f"Project {safe_name} tasks", unit="task"):
                    task_gid = t.get("gid")
                    task_name = t.get("name", "") or ""
                    if not task_gid:
                        continue
                    try:
                        fetch_and_write_comments_for_task(task_gid, task_name, token, comment_writer)
                    except Exception as e:
                        print(f"    Warning: failed to fetch comments for task {task_gid}: {e}")
                        continue

            print(f"  Finished project {safe_name}")

        except Exception as e:
            print(f"Error processing project {proj_gid}: {e}")
            continue

    print("\nAll projects processed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Asana projects to CSV (tasks + per-project comments CSV).")
    parser.add_argument("--projects", "-p", default="projects.txt", help="Text file with one Asana project GID per line")
    parser.add_argument("--outdir", "-o", default="asana_csvs", help="Output directory for CSV files")
    args = parser.parse_args()
    main(args)

