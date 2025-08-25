import os
import sqlite3
import requests
import json
import datetime
import re

# Config
PINOKIO_ORG = "pinokiofactory"
DB_PATH = "pinokio.db"
OUT_JSON = "docs/data.json"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # optional (from repo secrets)

# Headers for API calls
headers = {"Accept": "application/vnd.github.v3+json"}
if GITHUB_TOKEN:
    headers["Authorization"] = f"token {GITHUB_TOKEN}"

# ---------------- DB SETUP ---------------- #
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS repos (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            description TEXT,
            html_url TEXT,
            created_at TEXT,
            updated_at TEXT,
            pushed_at TEXT,
            upstream_url TEXT,
            upstream_created_at TEXT,
            upstream_updated_at TEXT,
            upstream_open_issues INTEGER,
            last_checked TEXT
        )
    """)
    conn.commit()
    return conn

# ---------------- API CALLS ---------------- #
def github_api(url):
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

def list_org_repos(org):
    url = f"https://api.github.com/orgs/{org}/repos?per_page=100"
    repos = []
    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        repos.extend(r.json())
        url = r.links.get("next", {}).get("url")
    return repos

def find_upstream_url(repo_name):
    """Try to extract upstream URL from README"""
    try:
        url = f"https://raw.githubusercontent.com/{PINOKIO_ORG}/{repo_name}/main/README.md"
        r = requests.get(url)
        if r.status_code == 200:
            m = re.search(r"https://github.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", r.text)
            if m:
                return m.group(0)
    except:
        pass
    return None

def fetch_upstream_data(url):
    """Return metadata for upstream repo"""
    try:
        parts = url.rstrip("/").split("/")
        owner, repo = parts[-2], parts[-1]
        data = github_api(f"https://api.github.com/repos/{owner}/{repo}")
        return {
            "created_at": data["created_at"],
            "updated_at": data["updated_at"],
            "open_issues": data["open_issues_count"],
        }
    except Exception:
        return None

# ---------------- MAIN ---------------- #
def main():
    conn = init_db()
    cur = conn.cursor()

    repos = list_org_repos(PINOKIO_ORG)

    for repo in repos:
        name = repo["name"]
        description = repo.get("description", "")
        html_url = repo["html_url"]
        created_at = repo["created_at"]
        updated_at = repo["updated_at"]
        pushed_at = repo["pushed_at"]

        # Check if already in DB
        cur.execute("SELECT updated_at FROM repos WHERE name=?", (name,))
        row = cur.fetchone()

        upstream_url, up_created, up_updated, up_issues = None, None, None, None

        # If new or updated, refresh
        if not row or row[0] != updated_at:
            upstream_url = find_upstream_url(name)
            if upstream_url:
                upstream_data = fetch_upstream_data(upstream_url)
                if upstream_data:
                    up_created = upstream_data["created_at"]
                    up_updated = upstream_data["updated_at"]
                    up_issues = upstream_data["open_issues"]

            cur.execute("""
                INSERT OR REPLACE INTO repos
                (id, name, description, html_url, created_at, updated_at, pushed_at,
                 upstream_url, upstream_created_at, upstream_updated_at, upstream_open_issues, last_checked)
                VALUES (
                    (SELECT id FROM repos WHERE name=?),
                    ?,?,?,?,?,?,?,?,?,?,?
                )
            """, (
                name, name, description, html_url, created_at, updated_at, pushed_at,
                upstream_url, up_created, up_updated, up_issues,
                datetime.datetime.utcnow().isoformat()
            ))
            conn.commit()

    conn.close()

    # Export to JSON
    export_json()

def export_json():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM repos")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

    print(f"✅ Exported {len(rows)} repos to {OUT_JSON}")

if __name__ == "__main__":
    main()
