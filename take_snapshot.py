"""
Screenshot the dashboard hero section (D-Flector + Retail cards)
and save to deploy_tmp/snapshot.png for hosting on Cloudflare Pages.
"""
import os
import sys


def take_snapshot(html_path, output_path):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[WARN] playwright not installed — skipping snapshot")
        return False

    abs_path = os.path.abspath(html_path)
    if not os.path.exists(abs_path):
        print(f"[WARN] dashboard.html not found at {abs_path} — skipping snapshot")
        return False

    print(f"[INFO] Taking snapshot of hero section...")
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        # Load local file directly — fresh data, no caching issues
        page.goto(f"file://{abs_path}", wait_until="networkidle")
        page.wait_for_timeout(3000)  # let Chart.js render

        # Crop to just the hero row (two big cards at the top)
        page.screenshot(
            path=output_path,
            clip={"x": 0, "y": 60, "width": 1440, "height": 400},
            full_page=False
        )
        browser.close()

    print(f"[OK] Snapshot saved: {output_path}")
    return True


if __name__ == "__main__":
    base = os.path.dirname(os.path.abspath(__file__))
    html  = os.path.join(base, "dashboard.html")
    out   = os.path.join(base, "deploy_tmp", "snapshot.png")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    success = take_snapshot(html, out)
    sys.exit(0 if success else 1)
