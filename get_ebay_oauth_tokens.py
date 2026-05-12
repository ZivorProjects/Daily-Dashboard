"""
get_ebay_oauth_tokens.py  —  Generate eBay OAuth refresh tokens (one per store).

Each store has its own developer app, App ID, Cert ID and RuName stored in config.json.
Run once per store to get a refresh token (~18 months). Add them to GitHub Secrets.

Usage:  python get_ebay_oauth_tokens.py
"""

import json, urllib.parse, webbrowser, requests, base64

CONFIG = "config.json"

STORES = [
    {"key": "zivor", "label": "Zivor Automotive",       "username": "zivor_automotive",      "secret": "EBAY_OAUTH_REFRESH_ZIVOR"},
    {"key": "ams",   "label": "Australian Moto Spares", "username": "australian_moto_spares", "secret": "EBAY_OAUTH_REFRESH_AMS"},
    {"key": "ats",   "label": "Australian Tow Spares",  "username": "australian_tow_spares",  "secret": "EBAY_OAUTH_REFRESH_ATS"},
]

SCOPE = "https://api.ebay.com/oauth/api_scope/sell.analytics.readonly"


def load_creds(store_key):
    with open(CONFIG, encoding="utf-8") as f:
        c = json.load(f)
    s = c["ebay"]["stores"][store_key]
    return s["oauth_app_id"], s["oauth_cert_id"], s["oauth_runame"]


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


def main():
    print("=" * 65)
    print("  eBay OAuth Refresh Token Generator")
    print("=" * 65)
    print("\nA browser will open for each seller account.")
    print("Sign in with the CORRECT eBay seller account each time.\n")

    results = {}

    for store in STORES:
        app_id, cert_id, runame = load_creds(store["key"])

        print(f"\n{'─'*65}")
        print(f"  Store  : {store['label']}  ({store['username']})")
        print(f"  App ID : {app_id}")
        print(f"  RuName : {runame}")
        print(f"{'─'*65}")

        skip = input("\n  Press Enter to open browser, or type 'skip' to skip: ").strip().lower()
        if skip == "skip":
            print("  Skipped.")
            continue

        auth_url = (
            "https://auth.ebay.com/oauth2/authorize"
            f"?client_id={urllib.parse.quote(app_id)}"
            f"&redirect_uri={urllib.parse.quote(runame)}"
            f"&response_type=code"
            f"&scope={urllib.parse.quote(SCOPE)}"
        )

        print(f"\n  Opening browser — sign in as: {store['username']}")
        print(f"  (If a different account is already logged in, sign out first)\n")
        webbrowser.open(auth_url)

        print("  After authorising, you'll be redirected to a URL containing ?code=...")
        redirect = input("  Paste the full redirect URL here: ").strip()

        code = None
        if "code=" in redirect:
            raw = redirect.split("code=")[1].split("&")[0]
            code = urllib.parse.unquote(raw)

        if not code:
            print("  ERROR: No 'code=' found in the URL. Skipping.")
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
        print(f"  ✅ Success! Refresh token expires in ~{expires_days} days ({expires_days//30} months).")

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
        print("  All 3 tokens ready. Update GitHub Secrets then trigger a test run.")

    input("\nPress Enter to close...")


if __name__ == "__main__":
    main()
