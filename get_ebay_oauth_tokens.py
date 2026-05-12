"""
get_ebay_oauth_tokens.py  —  One-time helper to generate eBay OAuth refresh tokens.

Generates 3 refresh tokens (one per seller account) all using Zivor's single developer app.
One eBay developer app can authorise multiple seller accounts — this is the correct approach.

Run once, then add the 3 tokens to GitHub Secrets. Tokens last ~18 months.

Usage:
    python get_ebay_oauth_tokens.py

Requirements:
    pip install requests
"""

import json, urllib.parse, webbrowser, requests, base64

CONFIG = "config.json"

STORES = [
    {
        "key":         "zivor",
        "label":       "Zivor Automotive",
        "username":    "zivor_automotive",
        "secret_name": "EBAY_OAUTH_REFRESH_ZIVOR",
    },
    {
        "key":         "ams",
        "label":       "Australian Moto Spares",
        "username":    "australian_moto_spares",
        "secret_name": "EBAY_OAUTH_REFRESH_AMS",
    },
    {
        "key":         "ats",
        "label":       "Australian Tow Spares",
        "username":    "australian_tow_spares",
        "secret_name": "EBAY_OAUTH_REFRESH_ATS",
    },
]

SCOPE = "https://api.ebay.com/oauth/api_scope/sell.analytics.readonly"


def load_main_creds():
    """Always use Zivor's main app credentials — one app authorises all 3 seller accounts."""
    with open(CONFIG, encoding="utf-8") as f:
        c = json.load(f)
    return c["ebay"]["app_id"], c["ebay"]["cert_id"]


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
        print(f"  eBay error: {r.status_code} — {r.text[:400]}")
        r.raise_for_status()
    return r.json()


def main():
    app_id, cert_id = load_main_creds()

    print("=" * 65)
    print("  eBay OAuth Refresh Token Generator")
    print("=" * 65)
    print(f"\nUsing Zivor developer app: {app_id}")
    print("\nThis app will authorise all 3 seller accounts.")
    print("You need the RuName (eBay Redirect URL name) for this app.\n")
    print("Find it at: https://developer.ebay.com/my/auth/?env=production&index=0")
    print("  → Select the Zivor app → OAuth tab → RuName field")
    print("  It looks like: RaviShar-Claude-PRD-xxxxxxx-xxxxxxxx\n")

    runame = input("Paste the RuName for the Zivor app: ").strip()
    if not runame:
        print("No RuName entered. Exiting.")
        return

    results = {}

    for store in STORES:
        print(f"\n{'─'*65}")
        print(f"  Account {STORES.index(store)+1}/3: {store['label']}  ({store['username']})")
        print(f"{'─'*65}")

        auth_url = (
            "https://auth.ebay.com/oauth2/authorize"
            f"?client_id={app_id}"
            f"&redirect_uri={urllib.parse.quote(runame)}"
            f"&response_type=code"
            f"&scope={urllib.parse.quote(SCOPE)}"
        )

        print(f"\n  Opening browser ...")
        print(f"  → Sign in with the eBay seller account: {store['username']}")
        print(f"  → If already logged into a different account, sign out first.\n")
        webbrowser.open(auth_url)

        print("  After authorising you'll land on a URL like:")
        print("  https://signin.ebay.com/...?code=v^1.1%23i...&expires_in=299")
        redirect = input("\n  Paste the full redirect URL here: ").strip()

        # Extract auth code
        code = None
        if "code=" in redirect:
            raw_code = redirect.split("code=")[1].split("&")[0]
            code = urllib.parse.unquote(raw_code)

        if not code:
            print(f"  ERROR: Could not find 'code=' in the URL. Skipping.")
            continue

        print(f"\n  Exchanging auth code for tokens ...")
        try:
            token_data = exchange_code(app_id, cert_id, runame, code)
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        refresh_token = token_data.get("refresh_token", "")
        expires_s     = token_data.get("refresh_token_expires_in", 0)

        if not refresh_token:
            print(f"  ERROR: No refresh_token in response: {token_data}")
            continue

        results[store["key"]] = {
            "secret_name":   store["secret_name"],
            "refresh_token": refresh_token,
            "expires_days":  int(expires_s) // 86400,
        }
        print(f"  ✅ Token obtained! Expires in {int(expires_s)//86400} days.")

    # Summary
    print(f"\n{'='*65}")
    print("  RESULTS — Update these GitHub Secrets")
    print(f"{'='*65}")
    print("  https://github.com/ZivorProjects/Daily-Dashboard/settings/secrets/actions\n")

    for key, r in results.items():
        print(f"  Secret : {r['secret_name']}")
        print(f"  Value  : {r['refresh_token']}")
        print(f"  Expires: ~{r['expires_days']} days")
        print()

    if len(results) < len(STORES):
        missing = [s["secret_name"] for s in STORES if s["key"] not in results]
        print(f"  ⚠  Missing tokens for: {', '.join(missing)}")
        print("  Re-run to retry those accounts.")
    else:
        print("  All 3 tokens generated successfully.")
        print("  Update the 3 GitHub Secrets, then trigger a manual run to verify.")

    input("\nPress Enter to close...")


if __name__ == "__main__":
    main()
