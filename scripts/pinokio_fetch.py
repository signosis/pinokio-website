import requests, os, base64, json, csv, time, re
from datetime import datetime, timezone

PINOKIO_ORG = "pinokiofactory"
API_URL = "https://api.github.com"
OUTPUT_DIR = "../site"
CSV_FILE = f"{OUTPUT_DIR}/pinokio_repos.csv"
JSON_FILE = f"{OUTPUT_DIR}/pinokio_repos.json"

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

def github_get(url):
    while True:
        r = requests.get(url, headers=headers)
        if r.status_code == 403 and "X-RateLimit-Remaining" in r.headers and r.headers["X-RateLimit-Remaining"]=="0":
            reset = int(r.headers.get("X-RateLimit-Reset", time.time()+60))
            time.sleep(max(reset - int(time.time()), 1))
            continue
        r.raise_for_status()
        return r

def get_repo_info(owner, repo):
    r = github_get(f"{API_URL}/repos/{owner}/{repo}")
    d = r.json()
    return {"name": d["full_name"], "url": d["html_url"], "created_at": d["created_at"],
            "updated_at": d["updated_at"], "open_issues": d["open_issues_count"]}

def list_org_repos(org):
    repos = []
    url = f"{API_URL}/orgs/{org}/repos?per_page=100"
    while url:
        r = github_get(url)
        repos.extend(r.json())
        url = r.links.get("next",{}).get("url")
    return repos

def find_upstream_repo(org, repo_name):
    for file in ["pinokio.json","install.json","README.md"]:
        url = f"{API_URL}/repos/{org}/{repo_name}/contents/{file}"
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            continue
        content = r.json()
        try:
            decoded = base64.b64decode(content["content"]).decode("utf-8")
        except:
            continue
        if file.lower()=="readme.md":
            match = re.search(r'https://github\.com/[\w\-_]+/[\w\-_]+', decoded)
            if match:
                parts = match.group(0).split("github.com/")[-1].strip("/").split("/")
                return get_repo_info(parts[0], parts[1].replace(".git",""))
        else:
            try:
                j = json.loads(decoded)
                def find_url(d):
                    if isinstance(d, dict):
                        for v in d.values():
                            url = find_url(v)
                            if url: return url
                    elif isinstance(d,list):
                        for item in d:
                            url = find_url(item)
                            if url: return url
                    elif isinstance(d,str) and "github.com" in d:
                        return d
                    return None
                url = find_url(j)
                if url:
                    parts = url.split("github.com/")[-1].strip("/").split("/")
                    return get_repo_info(parts[0], parts[1].replace(".git",""))
            except: continue
    return None

# Fetch repos
results=[]
repos=list_org_repos(PINOKIO_ORG)
for repo in repos:
    script_info=get_repo_info(PINOKIO_ORG, repo["name"])
    upstream_info=find_upstream_repo(PINOKIO_ORG, repo["name"])
    results.append({
        "script_name": script_info["name"],
        "script_url": script_info["url"],
        "script_created": script_info["created_at"],
        "script_updated": script_info["updated_at"],
        "script_open_issues": script_info["open_issues"],
        "upstream_name": upstream_info["name"] if upstream_info else "",
        "upstream_url": upstream_info["url"] if upstream_info else "",
        "upstream_created": upstream_info["created_at"] if upstream_info else "",
        "upstream_updated": upstream_info["updated_at"] if upstream_info else "",
        "upstream_open_issues": upstream_info["open_issues"] if upstream_info else ""
    })

# Save JSON
os.makedirs(OUTPUT_DIR, exist_ok=True)
with open(JSON_FILE,"w",encoding="utf-8") as f:
    json.dump(results,f,indent=2)
# Save CSV
with open(CSV_FILE,"w",newline="",encoding="utf-8") as f:
    writer = csv.DictWriter(f,fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)
print("✅ JSON and CSV updated")
