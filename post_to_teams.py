"""
Post Zivor Dashboard snapshot to Microsoft Teams (Team Huddle group chat)
via Power Automate webhook using an Adaptive Card with image.
"""
import json
import os
import re
import sys
import time

try:
    import requests
except ImportError:
    print("[WARN] requests not installed — skipping Teams notification")
    sys.exit(0)

DASHBOARD_URL  = "https://dflector-dashboard.pages.dev"
SNAPSHOT_URL   = f"{DASHBOARD_URL}/snapshot.png"


def extract_dashboard_data(html_path):
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()
    marker = "const DASHBOARD_DATA = "
    idx = html.find(marker)
    if idx == -1:
        raise ValueError("DASHBOARD_DATA not found in dashboard.html")
    json_start = html.index("{", idx)
    decoder = json.JSONDecoder()
    data, _ = decoder.raw_decode(html[json_start:])
    return data


def build_adaptive_card(data):
    last_updated  = data.get("lastUpdated", "")
    current_month = data.get("currentMonth", "")
    trade         = data.get("trade", {})
    stores        = data.get("stores", [])
    svc           = data.get("serviceMetrics", [])

    completed    = trade.get("completedMTD", 0)
    target       = trade.get("target", 250000)
    open_mtd     = trade.get("openMTD", 0)
    pct          = (completed / target * 100) if target else 0
    retail_total = sum(s.get("achieved", 0) for s in stores)

    ebay_parts = []
    for sm in svc:
        name  = sm.get("store", "")
        level = sm.get("seller_level", "")
        if name and level:
            icon = "✅" if "Top Rated" in level else "⚠️"
            ebay_parts.append(f"{icon} {name}: {level}")
    ebay_text = "  |  ".join(ebay_parts) if ebay_parts else "No data"

    # Add cache-busting timestamp so Teams doesn't show yesterday's image
    ts = int(time.time())

    card = {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.4",
        "body": [
            {
                "type": "TextBlock",
                "text": f"📊 Zivor Dashboard — {last_updated}",
                "weight": "Bolder",
                "size": "Large",
                "color": "Accent",
                "wrap": True
            },
            {
                "type": "Image",
                "url": f"{SNAPSHOT_URL}?t={ts}",
                "altText": "Dashboard snapshot",
                "size": "Stretch",
                "style": "Default"
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": f"D-Flector Completed ({current_month})",
                     "value": f"AUD ${completed:,.2f}  ({pct:.1f}% of target)"},
                    {"title": "Open Pipeline",
                     "value": f"AUD ${open_mtd:,.2f}"},
                    {"title": "Combined Retail (31 days)",
                     "value": f"AUD ${retail_total:,.2f}"},
                    {"title": "eBay Seller Status",
                     "value": ebay_text},
                ]
            }
        ],
        "actions": [
            {
                "type": "Action.OpenUrl",
                "title": "View Full Dashboard",
                "url": DASHBOARD_URL
            }
        ]
    }
    return card


def main():
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
    if not webhook_url:
        print("[INFO] TEAMS_WEBHOOK_URL not set — skipping Teams notification")
        return

    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")

    try:
        data = extract_dashboard_data(html_path)
        card = build_adaptive_card(data)

        # Send card as JSON string � Power Automate expects stringified card
        payload  = {"card": json.dumps(card)}
        resp     = requests.post(webhook_url, json=payload, timeout=30)
        if not resp.ok:
            print(f"[WARN] Teams webhook returned {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
        print(f"[OK] Teams Adaptive Card sent (HTTP {resp.status_code})")
    except Exception as e:
        print(f"[WARN] Teams notification failed: {e}")
        # Non-fatal — dashboard deploy already succeeded


if __name__ == "__main__":
    main()
