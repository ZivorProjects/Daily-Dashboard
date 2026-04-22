"""
Post daily Zivor Dashboard summary to Microsoft Teams group chat
via Power Automate webhook.
"""
import json
import os
import re
import sys

try:
    import requests
except ImportError:
    print("[WARN] requests not installed — skipping Teams notification")
    sys.exit(0)


def extract_dashboard_data(html_path):
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()
    match = re.search(r"const DASHBOARD_DATA\s*=\s*(\{.*?\});\s*</script>", html, re.DOTALL)
    if not match:
        raise ValueError("DASHBOARD_DATA not found in dashboard.html")
    return json.loads(match.group(1))


def format_message(data):
    trade        = data.get("trade", {})
    stores       = data.get("stores", [])
    last_updated = data.get("lastUpdated", "Unknown")
    current_month = data.get("currentMonth", "")
    svc          = data.get("serviceMetrics", [])

    completed  = trade.get("completedMTD", 0)
    target     = trade.get("target", 250000)
    open_mtd   = trade.get("openMTD", 0)
    pct        = (completed / target * 100) if target else 0

    retail_total = sum(s.get("achieved", 0) for s in stores)

    ebay_lines = []
    for sm in svc:
        name  = sm.get("store", "")
        level = sm.get("seller_level", "")
        if name and level:
            icon = "✅" if "Top Rated" in level else "⚠️"
            ebay_lines.append(f"{icon} {name}: {level}")
    ebay_block = " &nbsp;|&nbsp; ".join(ebay_lines) if ebay_lines else "No data"

    url = "https://dflector-dashboard.pages.dev"

    return (
        f"<b>📊 Zivor Dashboard — {last_updated}</b><br><br>"
        f"<b>D-Flector Trade &nbsp;({current_month})</b><br>"
        f"&nbsp;&nbsp;✅ Completed MTD: <b>AUD ${completed:,.2f}</b> &nbsp;({pct:.1f}% of ${target:,.0f} target)<br>"
        f"&nbsp;&nbsp;📋 Open Pipeline: AUD ${open_mtd:,.2f}<br><br>"
        f"<b>Combined Retail &nbsp;(last 31 days)</b><br>"
        f"&nbsp;&nbsp;🛒 Total: <b>AUD ${retail_total:,.2f}</b><br><br>"
        f"<b>eBay Seller Status</b><br>"
        f"&nbsp;&nbsp;{ebay_block}<br><br>"
        f'🔗 <a href="{url}">View Full Dashboard</a>'
    )


def main():
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
    if not webhook_url:
        print("[INFO] TEAMS_WEBHOOK_URL not set — skipping Teams notification")
        return

    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")

    try:
        data    = extract_dashboard_data(html_path)
        message = format_message(data)
        resp    = requests.post(webhook_url, json={"text": message}, timeout=30)
        resp.raise_for_status()
        print(f"[OK] Teams notification sent (HTTP {resp.status_code})")
    except Exception as e:
        print(f"[WARN] Teams notification failed: {e}")
        # Non-fatal — dashboard deploy already succeeded


if __name__ == "__main__":
    main()
