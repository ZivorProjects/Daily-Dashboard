"""
get_ebay_oauth_tokens.py  —  Generate eBay OAuth refresh tokens (one per store).

Each store has its own eBay developer app, so each needs its own RuName and
produces a refresh token tied to that specific app's credentials.

Usage:  python get_ebay_oauth_tokens.py
"""

import json, urllib.parse, webbrowser, requests, base64

CONFIG = "config.json"

STORES = [
    {"key": "zivor", "label": "Zivor Automotive",       "username": "zivor_automotive",        "secret": "EBAY_OAUTH_REFRESH_ZIVOR"},
    {"key": "ams",   "label": "Australian Moto Spares", "username": "australian_moto_spares",   "secret": "EBAY_OAUTH_REFRESH_AMS"},
    {"key": "ats",   "label": "Australian Tow Spares",  "username": "australian_tow_spares",    "secret": "EBAY_OAUTH_REFRESH_ATS"},
]

SCOPE = "https://api.ebay.com/oauth/api_scope/sell.analytics.readonly"


def load_creds(store_key):
    with open(CONFIG, encoding="utf-8") as f:
        c = json.load(f)
    s = c["ebay"]["stores"][store_key]
    return s["oauth_app_id"], s["oauth_cert_id"]


def exchange_code(app_id, cert_id, runame, code):
    creds = base64.b64encode(f"{app_id}:{cert_id}".encode()).decode()
    r = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {creds}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
        data={
            "grant_type":   "authorization_code",
            "code":          code,
            "redirect_uri":  runame,
        },
        timeout=30,
    )
    if not r.ok:
        print(f"\n  eBay error {r.status_code}: {r.text[:500]}")
        r.raise_for_status()
    return r.json()


def get_runame(store):
    print(f"\n  Find the RuName for the {store['label']} app:")
    print(f"  1. Go to https://developer.ebay.com/my/auth/?env=production&index=0")
    print(f"  2. Select app: {store['app_id']}")
    print(f"  3. Click the OAuth tab → copy the RuName field")
    print(f"     (looks like: YourName-AppName-PRD-xxxxxxx-xxxxxxxx)")
    return input(f"\n  Paste RuName for {store['label']}: ").strip()


def main():
    print("=" * 65)
    print("  eBay OAuth Refresh Token Generator  (per-store apps)")
    print("=" * 65)
    print("\nEach store uses its own developer app and RuName.")
    print("Sign in with the CORRECT seller account for each store.\n")

    results = {}

    for store in STORES:
        app_id, cert_id = load_creds(store["key"])
        store["app_id"] = app_id

        print(f"\n{'─'*65}")
        print(f"  Store : {store['label']}  ({store['username']})")
        print(f"  App ID: {app_id}")
        print(f"{'─'*65}")

        runame = get_runame(store)
        if not runame:
            print("  Skipped (no RuName entered).")
            continue

        auth_url = (
            "https://auth.ebay.com/oauth2/authorize"
            f"?client_id={urllib.parse.quote(app_id)}"
            f"&redirect_uri={urllib.parse.quote(runame)}"
            f"&response_type=code"
            f"&scope={urllib.parse.quote(SCOPE)}"
        )

        print(f"\n  Opening browser — sign in as: {store['username']}")
        print(f"  (If a different account is logged in, sign out first)\n")
        webbrowser.open(auth_url)

        print("  After authorising, you'll land on a URL containing ?code=...")
        redirect = input("  Paste the full redirect URL: ").strip()

        code = None
        if "code=" in redirect:
            raw = redirect.split("code=")[1].split("&")[0]
            code = urllib.parse.unquote(raw)

        if not code:
            print("  ERROR: No 'code=' found in URL. Skipping.")
            continue

        print("\n  Exchanging auth code for tokens...")
        try:
            data = exchange_code(app_id, cert_id, runame, code)
        except Exception as e:
            print(f"  FAILED: {e}")
            continue

        refresh_token = data.get("refresh_token", "")
        expires_days  = int(data.get("refresh_token_expires_in", 0)) // 86400

        if not refresh_token:
            print(f"  ERROR: No refresh_token in response: {data}")
            continue

        results[store["key"]] = {
            "secret":        store["secret"],
            "refresh_token": refresh_token,
            "expires_days":  expires_days,
        }
        print(f"  ✅ Token obtained! Expires in ~{expires_days} days ({expires_days//30} months).")

    # Summary
    print(f"\n{'='*65}")
    print("  RESULTS — Update these GitHub Secrets")
    print(f"{'='*65}")
    print("  https://github.com/ZivorProjects/Daily-Dashboard/settings/secrets/actions\n")

    for key, r in results.items():
        print(f"  Secret : {r['secret']}")
        print(f"  Value  : {r['refresh_token']}")
        print(f"  Expires: ~{r['expires_days']} days")
        print()

    missing = [s["secret"] for s in STORES if s["key"] not in results]
    if missing:
        print(f"  ⚠  Missing: {', '.join(missing)} — re-run to retry.")
    else:
        print("  All 3 tokens ready. Update GitHub Secrets then trigger a run.")

    input("\nPress Enter to close...")


if __name__ == "__main__":
    main()
