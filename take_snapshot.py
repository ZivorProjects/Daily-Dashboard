# -*- coding: utf-8 -*-
"""
Screenshot just the two hero cards (D-Flector Completed + Combined Retail)
from the local dashboard.html and save as snapshot.png in the repo root.
"""
import os
import sys


def take_snapshot(html_path, output_path):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[WARN] playwright not installed -- skipping snapshot")
        return False

    abs_path = os.path.abspath(html_path)
    if not os.path.exists(abs_path):
        print("[WARN] dashboard.html not found -- skipping snapshot")
        return False

    print("[INFO] Taking snapshot of hero cards...")
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        # Load local dashboard file directly (fresh data, no caching)
        page.goto("file://" + abs_path, wait_until="networkidle")
        page.wait_for_timeout(3000)  # let Chart.js render

        # Find the hero row element and screenshot just that
        try:
            hero = page.locator(".hero-row").first
            hero.screenshot(path=output_path)
            print("[OK] Hero row element captured")
        except Exception:
            # Fallback: clip to top portion of page (below header)
            page.screenshot(
                path=output_path,
                clip={"x": 0, "y": 120, "width": 1440, "height": 380},
                full_page=False
            )
            print("[OK] Snapshot captured via clip fallback")

        browser.close()

    print("[OK] Snapshot saved: " + output_path)
    return True


if __name__ == "__main__":
    base      = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base, "dashboard.html")
    out_path  = os.path.join(base, "snapshot.png")
    success   = take_snapshot(html_path, out_path)
    sys.exit(0 if success else 1)
