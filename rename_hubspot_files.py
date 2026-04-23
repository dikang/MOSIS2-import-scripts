import os
import re
import time
from typing import Dict, Iterator, List, Optional, Set

import requests


# ----------------------------
# Config
# ----------------------------
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN")
BASE_URL = "https://api.hubapi.com"

TARGET_FOLDER_NAME = "AsanaUploads"

# HubSpot v3 endpoints
FILES_SEARCH_URL = f"{BASE_URL}/files/v3/files/search"
FOLDERS_SEARCH_URL = f"{BASE_URL}/files/v3/folders/search"
UPDATE_FILE_URL = f"{BASE_URL}/files/v3/files"

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

# Match names like:
#   file name__task gid__attach gid
# and keep only "file name"
SUFFIX_RE = re.compile(r"^(?P<base>.+?)__[^_]+__[^_]+$")


# ----------------------------
# HubSpot API helpers
# ----------------------------
def get_folder_ids(folder_name: str) -> List[str]:
    """
    Find HubSpot file-manager folder IDs by folder name.
    """
    folder_ids: List[str] = []
    after: Optional[str] = None

    while True:
        params = {"limit": 100, "name": folder_name}
        if after:
            params["after"] = after

        resp = requests.get(FOLDERS_SEARCH_URL, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        for folder in data.get("results", []):
            if folder.get("name") == folder_name:
                folder_ids.append(str(folder.get("id")))

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return folder_ids


def iter_files_in_folders(folder_ids: List[str]) -> Iterator[Dict]:
    """
    Iterate all files whose parent folder is one of the target folder IDs.
    """
    after: Optional[str] = None

    while True:
        params = {
            "limit": 100,
            "parentFolderIds": folder_ids,
        }
        if after:
            params["after"] = after

        resp = requests.get(FILES_SEARCH_URL, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("results", []):
            yield item

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break


def rename_file(file_id: str, new_name: str) -> Dict:
    """
    Rename a file in place without changing file ID.
    """
    url = f"{UPDATE_FILE_URL}/{file_id}"
    payload = {"name": new_name}

    resp = requests.patch(url, headers=HEADERS, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


# ----------------------------
# Name collision helper
# ----------------------------
def make_unique_name(base_name: str, existing_names: Set[str]) -> str:
    """
    If base_name already exists, prepend '_' until it becomes unique.
    """
    candidate = base_name
    while candidate in existing_names:
        candidate = "_" + candidate
    return candidate


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("Set HUBSPOT_PRIVATE_APP_TOKEN in your environment first.")

    folder_ids = get_folder_ids(TARGET_FOLDER_NAME)
    if not folder_ids:
        raise RuntimeError(f"No HubSpot folder found with name '{TARGET_FOLDER_NAME}'.")

    print(f"Matched folder IDs: {folder_ids}")

    # First pass: collect every existing filename in the target folder(s)
    all_files = list(iter_files_in_folders(folder_ids))
    existing_names: Set[str] = {
        str(f.get("name", "")).strip()
        for f in all_files
        if str(f.get("name", "")).strip()
    }

    scanned = 0
    renamed = 0
    skipped = 0

    # Optional: deterministic order
    all_files.sort(key=lambda x: str(x.get("name", "")))

    for f in all_files:
        scanned += 1
        file_id = str(f.get("id", "")).strip()
        old_name = str(f.get("name", "")).strip()

        if not file_id or not old_name:
            skipped += 1
            continue

        m = SUFFIX_RE.match(old_name)
        if not m:
            skipped += 1
            continue

        base_name = m.group("base").strip()
        if not base_name:
            skipped += 1
            continue

        # Remove the file's own current name from the taken-name set,
        # so it does not block its own rename.
        existing_names.discard(old_name)

        new_name = make_unique_name(base_name, existing_names)

        if new_name == old_name:
            existing_names.add(old_name)
            skipped += 1
            continue

        print(f"Renaming {file_id}: '{old_name}' -> '{new_name}'")
        try:
            rename_file(file_id, new_name)
            renamed += 1
            existing_names.add(new_name)
        except requests.HTTPError as e:
            # If HubSpot still rejects the name for a collision reason,
            # try one more time with an additional underscore prefix.
            retry_name = "_" + new_name
            if retry_name not in existing_names:
                print(f"Retrying with '{retry_name}'")
                rename_file(file_id, retry_name)
                renamed += 1
                existing_names.add(retry_name)
            else:
                raise e

        time.sleep(0.15)

    print(f"Scanned: {scanned}")
    print(f"Renamed: {renamed}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    main()
