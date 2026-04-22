# -*- coding: utf-8 -*-
"""
Post Zivor Dashboard snapshot to Microsoft Teams (Team Huddle group chat)
via Power Automate webhook using an Adaptive Card with image.
"""
import json
import os
import sys
import time

try:
    import requests
except ImportError:
    print("[WARN] requests not installed -- skipping Teams notification")
    sys.exit(0)

DASHBOARD_URL = "https://dflector-dashboard.pages.dev"
SNAPSHOT_URL  = DASHBOARD_URL + "/snapshot.png"


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
            icon = "OK" if "Top Rated" in level else "!!"
            ebay_parts.append(icon + " " + name + ": " + level)
    ebay_text = " | ".join(ebay_parts) if ebay_parts else "No data"

    ts = int(time.time())

    card = {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.2",
        "body": [
            {
                "type": "TextBlock",
                "text": "Zivor Dashboard -- " + last_updated,
                "weight": "Bolder",
                "size": "Large",
                "color": "Accent",
                "wrap": True
            },
            {
                "type": "Image",
                "url": SNAPSHOT_URL + "?t=" + str(ts),
                "altText": "Dashboard snapshot",
                "size": "Stretch"
            },
            {
                "type": "FactSet",
                "facts": [
                    {
                        "title": "D-Flector Completed (" + current_month + ")",
                        "value": "AUD $" + "{:,.2f}".format(completed) + "  (" + "{:.1f}".format(pct) + "% of target)"
                    },
                    {
                        "title": "Open Pipeline",
                        "value": "AUD $" + "{:,.2f}".format(open_mtd)
                    },
                    {
                        "title": "Combined Retail (31 days)",
                        "value": "AUD $" + "{:,.2f}".format(retail_total)
                    },
                    {
                        "title": "eBay Seller Status",
                        "value": ebay_text
                    }
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
        print("[INFO] TEAMS_WEBHOOK_URL not set -- skipping Teams notification")
        return

    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")

    try:
        data    = extract_dashboard_data(html_path)
        card    = build_adaptive_card(data)
        payload = {"card": json.dumps(card)}

        resp = requests.post(webhook_url, json=payload, timeout=30)
        if not resp.ok:
            print("[WARN] Teams webhook returned " + str(resp.status_code) + ": " + resp.text[:500])
            resp.raise_for_status()
        print("[OK] Teams Adaptive Card sent (HTTP " + str(resp.status_code) + ")")

    except Exception as e:
        print("[WARN] Teams notification failed: " + str(e))


if __name__ == "__main__":
    main()
