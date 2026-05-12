#!/usr/bin/env python3
"""
eBay API Client — Fetches live seller metrics, feedback, and performance ratings
Integrates with the Zivor Dashboard update_dashboard.py pipeline
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class eBayClient:
    """Connect to eBay API to pull live seller metrics and performance data.

    Supports two token types:
      - Auth'n'Auth token (v^1.1#... format) → uses Trading API (SOAP/XML)
      - OAuth Bearer token                   → uses REST API endpoints
    """

    TRADING_API_URL = "https://api.ebay.com/ws/api.dll"
    TRADING_COMPATIBILITY = "967"
    TRADING_SITE_AU = "15"   # eBay Australia

    def __init__(self, app_id: str, cert_id: str, access_token: str,
                 sandbox_mode: bool = False):
        self.app_id = app_id
        self.cert_id = cert_id
        self.access_token = access_token
        self.sandbox_mode = sandbox_mode
        self.is_authnauth = access_token.startswith("v^")

        # API base URL (sandbox vs production)
        if sandbox_mode:
            self.base_url = "https://api.sandbox.ebay.com"
        else:
            self.base_url = "https://api.ebay.com"

    # ── Trading API (SOAP/XML) — works with Auth'n'Auth tokens ──────────────

    def _trading_api(self, call_name: str, xml_body: str, site_id: str = None) -> str:
        """Make a Trading API SOAP call using Auth'n'Auth token. Returns raw XML string."""
        site = site_id or self.TRADING_SITE_AU
        headers = {
            "X-EBAY-API-CALL-NAME": call_name,
            "X-EBAY-API-APP-NAME": self.app_id,
            "X-EBAY-API-SITEID": site,
            "X-EBAY-API-COMPATIBILITY-LEVEL": self.TRADING_COMPATIBILITY,
            "Content-Type": "text/xml",
        }
        full_xml = (
            f'<?xml version="1.0" encoding="utf-8"?>'
            f'<{call_name}Request xmlns="urn:ebay:apis:eBLBaseComponents">'
            f"<RequesterCredentials><eBayAuthToken>{self.access_token}</eBayAuthToken></RequesterCredentials>"
            f"{xml_body}"
            f"</{call_name}Request>"
        )
        import re as _re
        r = requests.post(self.TRADING_API_URL, data=full_xml.encode("utf-8"),
                          headers=headers, timeout=30)
        r.raise_for_status()
        # Check for eBay-level failure
        ack = _re.search(r"<Ack>(.*?)</Ack>", r.text)
        if ack and ack.group(1) not in ("Success", "Warning"):
            errs = _re.findall(r"<LongMessage>(.*?)</LongMessage>", r.text)
            raise Exception(f"eBay Trading API error ({call_name}): {'; '.join(errs[:2])}")
        return r.text

    def get_feedback_trading(self) -> Dict:
        """Fetch feedback score and positive percent via Trading API GetUser.
        Works with Auth'n'Auth tokens."""
        import re as _re
        try:
            xml = self._trading_api("GetUser", "<DetailLevel>ReturnSummary</DetailLevel>")
            score_m = _re.search(r"<FeedbackScore>(\d+)</FeedbackScore>", xml)
            pct_m   = _re.search(r"<PositiveFeedbackPercent>([\d.]+)</PositiveFeedbackPercent>", xml)
            score = int(score_m.group(1)) if score_m else 0
            pct   = float(pct_m.group(1)) if pct_m else 0.0
            return {"feedback_score": score, "feedback_percent": pct}
        except Exception as e:
            print(f"[ERROR] Trading API GetUser failed: {e}")
            return {"feedback_score": 0, "feedback_percent": 0.0}

    def get_active_listings_trading(self, site_id: str = None,
                                    listings_cache: int = 0) -> int:
        """Fetch count of active listings.

        The Trading API GetMyeBaySelling caps TotalNumberOfEntries at 25,000.
        Strategy:
          1. If listings_cache > 0 (sourced from Seller Hub via Chrome), use that.
          2. Otherwise fall back to the Trading API (capped at 25,000).
        """
        # Prefer Chrome-sourced cache (accurate, no API cap)
        if listings_cache > 0:
            return listings_cache

        import re as _re
        try:
            xml_body = (
                "<ActiveList>"
                "<Include>true</Include>"
                "<Pagination><EntriesPerPage>1</EntriesPerPage><PageNumber>1</PageNumber></Pagination>"
                "</ActiveList>"
                "<HideVariations>true</HideVariations>"
            )
            xml = self._trading_api("GetMyeBaySelling", xml_body, site_id=site_id)
            m = _re.search(r"<TotalNumberOfEntries>(\d+)</TotalNumberOfEntries>", xml)
            count = int(m.group(1)) if m else 0
            # Warn when the known API cap is hit
            if count >= 25000:
                print(f"  [WARN] Listing count capped at {count:,} by Trading API — "
                      f"run update_ebay_listings_cache() via Chrome to get real count")
            return count
        except Exception as e:
            print(f"[ERROR] Trading API GetMyeBaySelling failed: {e}")
            return 0

    # ── Seller Hub HTML Scraping — daily performance auto-fetch ─────────────

    @staticmethod
    def scrape_seller_standards_html(html: str) -> Dict:
        """Parse Seller Dashboard page text/HTML to extract performance metrics.

        Works on: https://sellerstandards.ebay.com.au/dashboard?region=GLOBAL
        Handles both raw HTML and already-extracted plain text.
        Returns a dict with defect_rate, late_ship_rate, cases_rate, seller_level, etc.
        """
        import re as _re
        txt = html
        if "<" in txt:
            txt = _re.sub(r"<[^>]+>", " ", txt)
            txt = _re.sub(r"&amp;", "&", txt)
            txt = _re.sub(r"&nbsp;", " ", txt)
            txt = _re.sub(r"\s+", " ", txt)

        defect_m = _re.search(
            r"Transaction defect rate\s*([\d.]+)%\s*([\d,]+)\s*of\s*([\d,]+)", txt)
        late_m = _re.search(
            r"Late shipment rate\s*([\d.]+)%\s*([\d,]+)\s*of\s*([\d,]+)", txt)
        cases_m = _re.search(
            r"Cases closed without seller resolution\s*([\d.]+)%\s*([\d,]+)\s*of\s*([\d,]+)", txt)
        curr_m = _re.search(
            r"Current seller level\s+(Top[\w\s]+?|Above Standard|Below Standard|Standard)"
            r"\s*(?:As of|as of)", txt, _re.IGNORECASE)
        proj_m = _re.search(
            r"(?:Your seller level would be|If we evaluated you today.*?)\s*"
            r"(Top[\w\s]+?|Above Standard|Below Standard|Standard)\s*Next", txt, _re.IGNORECASE)
        next_m = _re.search(r"Next evaluation on ([\d]+ \w+ \d{4})", txt)

        result: Dict = {}
        if defect_m:
            result.update(defect_rate=float(defect_m.group(1)),
                          defect_count=int(defect_m.group(2).replace(",", "")),
                          defect_total=int(defect_m.group(3).replace(",", "")))
        if late_m:
            result.update(late_ship_rate=float(late_m.group(1)),
                          late_ship_count=int(late_m.group(2).replace(",", "")),
                          late_ship_total=int(late_m.group(3).replace(",", "")))
        if cases_m:
            result.update(cases_rate=float(cases_m.group(1)),
                          cases_count=int(cases_m.group(2).replace(",", "")),
                          cases_total=int(cases_m.group(3).replace(",", "")))
        if curr_m:
            result["seller_level"] = curr_m.group(1).strip()
        if proj_m:
            result["seller_level_projected"] = proj_m.group(1).strip()
        if next_m:
            result["next_evaluation"] = next_m.group(1).strip()
        return result

    @staticmethod
    def scrape_service_metrics_html(html: str, metric_type: str = "INAD") -> Dict:
        """Parse Service Metrics page text/HTML for INAD or INR data.

        Works on: https://www.ebay.com.au/sh/performance/service-metrics
        metric_type: 'INAD' or 'INR'
        """
        import re as _re
        txt = html
        if "<" in txt:
            txt = _re.sub(r"<[^>]+>", " ", txt)
            txt = _re.sub(r"&amp;", "&", txt)
            txt = _re.sub(r"&nbsp;", " ", txt)
            txt = _re.sub(r"\s+", " ", txt)

        prefix = "inad" if metric_type.upper() == "INAD" else "inr"
        count_pattern = (r"Item not as described:\s*([\d,]+)"
                         if metric_type.upper() == "INAD"
                         else r"Not received:\s*([\d,]+)")

        rate_m  = _re.search(r"Rate:\s*([\d.]+)%", txt)
        total_m = _re.search(r"Total transactions:\s*([\d,]+)", txt)
        count_m = _re.search(count_pattern, txt)
        peer_m  = _re.search(r"Peers\s*=\s*([\d.]+)%", txt)
        per_m   = _re.search(
            r"Current rate:\s*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
            r"\s+\d{4}\s*[-–]\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
            r"\s+\d{4})", txt)

        rating = "Average"
        if _re.search(r"\bVery high\b", txt, _re.IGNORECASE):
            rating = "Very High"
        elif _re.search(r"\bHigh\b", txt, _re.IGNORECASE):
            rating = "High"
        elif _re.search(r"\bLow\b", txt, _re.IGNORECASE):
            rating = "Low"

        result: Dict = {f"{prefix}_rating": rating}
        if rate_m:
            result[f"{prefix}_rate"]  = float(rate_m.group(1))
        if total_m:
            result[f"{prefix}_total"] = int(total_m.group(1).replace(",", ""))
        if count_m:
            result[f"{prefix}_count"] = int(count_m.group(1).replace(",", ""))
        if peer_m:
            result[f"{prefix}_peer_rate"] = float(peer_m.group(1))
        if per_m:
            result[f"{prefix}_period"] = per_m.group(1).strip()
        return result

    def fetch_performance_metrics_with_cookies(self) -> Dict:
        """Fetch live seller performance metrics from eBay Seller Hub.

        Uses browser_cookie3 to read Chrome's local cookie store so no manual
        login is needed. Falls back gracefully if the library is unavailable or
        the session has expired.

        Fetches three pages:
          1. sellerstandards.ebay.com.au/dashboard  → defect, late-ship, cases, level
          2. /sh/performance/service-metrics        → INAD
          3. /sh/performance/service-metrics?metric_type=INR → INR

        Returns a merged dict of all scraped fields (empty dict on total failure).
        """
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-AU,en;q=0.9",
        })

        # Load Chrome cookies via browser_cookie3
        try:
            import browser_cookie3  # type: ignore
            for cookie in browser_cookie3.chrome(domain_name=".ebay.com.au"):
                sess.cookies.set(cookie.name, cookie.value, domain=cookie.domain)
            print("  [eBay Scraper] Chrome cookies loaded via browser_cookie3")
        except Exception as e:
            print(f"  [eBay Scraper] browser_cookie3 unavailable ({e}) — skipping live scrape")
            return {}

        merged: Dict = {}

        # 1) Seller Standards
        try:
            r = sess.get(
                "https://sellerstandards.ebay.com.au/dashboard?region=GLOBAL",
                timeout=20)
            if r.status_code == 200 and "Transaction defect" in r.text:
                parsed = self.scrape_seller_standards_html(r.text)
                merged.update(parsed)
                print(f"  [eBay Scraper] Standards: "
                      f"defect={parsed.get('defect_rate')}%  "
                      f"late={parsed.get('late_ship_rate')}%  "
                      f"cases={parsed.get('cases_rate')}%")
            else:
                print(f"  [eBay Scraper] Standards page returned {r.status_code} "
                      f"(session may have expired)")
        except Exception as e:
            print(f"  [eBay Scraper] Standards fetch failed: {e}")

        # 2) INAD
        try:
            r = sess.get(
                "https://www.ebay.com.au/sh/performance/service-metrics",
                timeout=20)
            if r.status_code == 200:
                parsed = self.scrape_service_metrics_html(r.text, "INAD")
                merged.update(parsed)
                print(f"  [eBay Scraper] INAD: {parsed.get('inad_rate')}%  "
                      f"(peers {parsed.get('inad_peer_rate')}%)")
            else:
                print(f"  [eBay Scraper] Service metrics (INAD) returned {r.status_code}")
        except Exception as e:
            print(f"  [eBay Scraper] INAD fetch failed: {e}")

        # 3) INR
        try:
            r = sess.get(
                "https://www.ebay.com.au/sh/performance/service-metrics"
                "?src=filters&metric_type=INR",
                timeout=20)
            if r.status_code == 200:
                parsed = self.scrape_service_metrics_html(r.text, "INR")
                merged.update(parsed)
                print(f"  [eBay Scraper] INR: {parsed.get('inr_rate')}%  "
                      f"(peers {parsed.get('inr_peer_rate')}%)")
            else:
                print(f"  [eBay Scraper] Service metrics (INR) returned {r.status_code}")
        except Exception as e:
            print(f"  [eBay Scraper] INR fetch failed: {e}")

        return merged

    @staticmethod
    def scrape_listing_count_from_seller_hub(page_text: str) -> int:
        """Parse active listing count from eBay Seller Hub page text.
        Call this after navigating Chrome to https://www.ebay.com.au/sh/lst/active
        and passing the page body text.

        Returns 0 if the count cannot be parsed.
        """
        import re as _re
        # "Results: 1-200 of 439,178"
        m = _re.search(r"Results:\s*[\d,\-]+ of ([\d,]+)", page_text)
        if m:
            return int(m.group(1).replace(",", ""))
        # "Manage active listings (439,178)"
        m2 = _re.search(r"Manage active listings \(([\d,]+)\)", page_text)
        if m2:
            return int(m2.group(1).replace(",", ""))
        return 0

    def get_all_metrics_trading(self, listings_cache: int = 0,
                                service_metrics: Dict = None) -> Dict:
        """Fetch all available metrics using the Trading API (for Auth'n'Auth tokens).

        Args:
            listings_cache:   Accurate listing count scraped from eBay Seller Hub via
                              Chrome (bypasses the 25,000 Trading API cap).
            service_metrics:  Dict from config.json service_metrics_override.zivor —
                              contains seller level, defect/late-ship/INAD/INR rates
                              sourced from the Seller Hub Performance page.
        """
        print("  [eBay] Using Trading API (Auth'n'Auth token)...")
        feedback = self.get_feedback_trading()
        listings = self.get_active_listings_trading(listings_cache=listings_cache)
        sm = service_metrics or {}
        return {
            "timestamp":        datetime.utcnow().isoformat(),
            "feedback_score":   feedback["feedback_score"],
            "feedback_percent": feedback["feedback_percent"],
            "listings":         listings,
            # Seller level (current + projected)
            "seller_level":           sm.get("seller_level", ""),
            "seller_level_projected": sm.get("seller_level_projected", ""),
            "next_evaluation":        sm.get("next_evaluation", ""),
            # Transaction defect rate
            "defect_rate":            sm.get("defect_rate", 0.0),
            "defect_count":           sm.get("defect_count", 0),
            "defect_total":           sm.get("defect_total", 0),
            "defect_threshold_top":   sm.get("defect_threshold_top", 0.50),
            "defect_period":          sm.get("defect_period", ""),
            # Late shipment rate
            "late_ship_rate":         sm.get("late_ship_rate", 0.0),
            "late_ship_count":        sm.get("late_ship_count", 0),
            "late_ship_total":        sm.get("late_ship_total", 0),
            "late_ship_threshold_top":sm.get("late_ship_threshold_top", 5.00),
            "late_ship_period":       sm.get("late_ship_period", ""),
            # Cases without resolution
            "cases_rate":             sm.get("cases_rate", 0.0),
            "cases_count":            sm.get("cases_count", 0),
            "cases_total":            sm.get("cases_total", 0),
            "cases_threshold_top":    sm.get("cases_threshold_top", 0.30),
            # INAD
            "inad_rate":              sm.get("inad_rate", 0.0),
            "inad_count":             sm.get("inad_count", 0),
            "inad_total":             sm.get("inad_total", 0),
            "inad_peer_rate":         sm.get("inad_peer_rate", 0.0),
            "inad_rating":            sm.get("inad_rating", ""),
            "inad_period":            sm.get("inad_period", ""),
            # INR
            "inr_rate":               sm.get("inr_rate", 0.0),
            "inr_count":              sm.get("inr_count", 0),
            "inr_total":              sm.get("inr_total", 0),
            "inr_peer_rate":          sm.get("inr_peer_rate", 0.0),
            "inr_rating":             sm.get("inr_rating", ""),
            "inr_period":             sm.get("inr_period", ""),
            # DSR (not available via Trading API)
            "dsr": {"item_description": 0.0, "communication": 0.0,
                    "shipping_time": 0.0, "return_handling": 0.0},
        }

    # ── REST API — works with OAuth Bearer tokens ────────────────────────────

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make authenticated GET request to eBay API."""
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                raise ValueError("eBay API: Invalid or expired access token. Refresh required.")
            elif response.status_code == 403:
                raise ValueError("eBay API: Insufficient permissions. Check OAuth scopes.")
            else:
                raise Exception(f"eBay API Error {response.status_code}: {response.text}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"eBay API Connection Error: {str(e)}")

    def get_seller_profile(self) -> Dict:
        """
        Fetch seller profile data including:
        - Feedback score and percentage
        - Account status
        - Store subscription level
        - Policy compliance status
        """
        try:
            data = self._get("/sell/account/v1/seller_profile")
            return {
                "feedback_score": data.get("feedbackScore", 0),
                "feedback_percent": data.get("positiveFeedbackPercent", 0),
                "status": data.get("status", "Unknown"),
                "subscription_level": data.get("subscriptionLevel", "Unknown"),
                "policy_compliant": data.get("policyCompliance", {}).get("compliant", True),
            }
        except Exception as e:
            print(f"[ERROR] Failed to fetch eBay seller profile: {e}")
            return {
                "feedback_score": 0,
                "feedback_percent": 0.0,
                "status": "Error",
                "subscription_level": "Unknown",
                "policy_compliant": False,
            }

    def get_seller_standards(self) -> Dict:
        """
        Fetch Detailed Seller Ratings (DSR) metrics:
        - Item as described rating
        - Communication rating
        - Shipping time rating
        - Return handling rating

        All ratings are on a scale of 1.0-5.0
        """
        try:
            data = self._get("/analytics/v1/seller_standards_profile")

            dimensions = {dim["name"]: dim["value"] for dim in data.get("dimensions", [])}

            return {
                "item_as_described": float(dimensions.get("ItemAsDescribedRating", 0.0)),
                "communication": float(dimensions.get("CommunicationRating", 0.0)),
                "shipping_time": float(dimensions.get("ShippingTimeRating", 0.0)),
                "return_handling": float(dimensions.get("ReturnHandlingRating", 0.0)),
            }
        except Exception as e:
            print(f"[ERROR] Failed to fetch eBay seller standards: {e}")
            return {
                "item_as_described": 0.0,
                "communication": 0.0,
                "shipping_time": 0.0,
                "return_handling": 0.0,
            }

    def get_active_listings_count(self) -> int:
        """
        Fetch count of active listings.
        Uses a simple count API or inventory endpoint.
        """
        try:
            # Try inventory endpoint (returns pagination with total)
            data = self._get("/sell/inventory/v1/inventory_item", {"limit": 1})
            return data.get("total", 0)
        except Exception as e:
            print(f"[ERROR] Failed to fetch eBay active listings: {e}")
            return 0

    def get_sales_metrics(self, days_back: int = 31) -> Dict:
        """
        Fetch sales metrics for the last N days using traffic reports.
        Falls back to order count if traffic data is unavailable.

        Returns:
        - daily_sales_count: average items sold per day
        - monthly_sales_count: total items sold in period
        - conversion_rate: click-to-sale percentage
        """
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days_back)

            params = {
                "metric_type": "TRAFFIC_SALES",
                "date_range": f"{start_date.strftime('%Y-%m-%d')}Z..{end_date.strftime('%Y-%m-%d')}Z",
            }

            data = self._get("/analytics/v1/traffic_reports", params=params)

            # Extract sales and traffic data
            records = data.get("records", [])
            total_sales = sum(r.get("sales", 0) for r in records)
            total_traffic = sum(r.get("clicks", 0) for r in records)

            conversion_rate = 0.0
            if total_traffic > 0:
                conversion_rate = (total_sales / total_traffic) * 100

            return {
                "monthly_sales_count": total_sales,
                "daily_avg_sales": total_sales / max(days_back, 1),
                "conversion_rate": conversion_rate,
                "total_traffic": total_traffic,
            }
        except Exception as e:
            print(f"[ERROR] Failed to fetch eBay sales metrics: {e}")
            return {
                "monthly_sales_count": 0,
                "daily_avg_sales": 0,
                "conversion_rate": 0.0,
                "total_traffic": 0,
            }

    def get_all_metrics(self, listings_cache: int = 0,
                        service_metrics: Dict = None) -> Dict:
        """
        Fetch all available metrics in one call.
        Auto-detects token type: Auth'n'Auth → Trading API; OAuth → REST API.

        Args:
            listings_cache:  Accurate listing count from Seller Hub Chrome scrape.
            service_metrics: Dict from config.json service_metrics_override.zivor.
        """
        print("[eBay] Fetching seller metrics...")

        if self.is_authnauth:
            # Auth'n'Auth token: use Trading API (SOAP)
            metrics = self.get_all_metrics_trading(
                listings_cache=listings_cache,
                service_metrics=service_metrics,
            )
        else:
            # OAuth token: use REST API
            profile   = self.get_seller_profile()
            standards = self.get_seller_standards()
            listings  = self.get_active_listings_count()
            sales     = self.get_sales_metrics(days_back=31)
            metrics = {
                "timestamp":        datetime.utcnow().isoformat(),
                "feedback_score":   profile.get("feedback_score", 0),
                "feedback_percent": profile.get("feedback_percent", 0),
                "listings":         listings,
                "dsr": {
                    "item_description": standards.get("item_as_described", 0.0),
                    "communication":    standards.get("communication", 0.0),
                    "shipping_time":    standards.get("shipping_time", 0.0),
                    "return_handling":  standards.get("return_handling", 0.0),
                },
            }

        print("[eBay] Metrics fetched successfully")
        return metrics

    # ── Sell Analytics API — OAuth tokens, live seller standards ────────────

    @staticmethod
    def refresh_oauth_access_token(app_id: str, cert_id: str, refresh_token: str) -> str:
        """Exchange an OAuth refresh token for a new short-lived access token.

        Called at the start of each GitHub Actions run. The pipeline stores only
        the long-lived refresh token (18-month expiry) in GitHub Secrets; access
        tokens (2-hour expiry) are obtained fresh on every run.
        """
        import base64 as _b64
        credentials = _b64.b64encode(f"{app_id}:{cert_id}".encode()).decode()
        r = requests.post(
            "https://api.ebay.com/identity/v1/oauth2/token",
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type":  "application/x-www-form-urlencoded",
            },
            data={
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
                "scope":         "https://api.ebay.com/oauth/api_scope/sell.analytics.readonly",
            },
            timeout=30,
        )
        if not r.ok:
            raise ValueError(f"eBay token refresh failed {r.status_code}: {r.text[:400]}")
        token = r.json().get("access_token", "")
        if not token:
            raise ValueError(f"No access_token in response: {r.text[:200]}")
        return token

    def get_seller_standards_analytics(self, marketplace_id: str = "EBAY_AU") -> Dict:
        """Fetch live seller standards and service metrics via eBay Sell Analytics API.

        Requires an OAuth Bearer token (NOT Auth'n'Auth) with scope:
            https://api.ebay.com/oauth/api_scope/sell.analytics.readonly

        Returns a dict with the same keys as service_metrics_override so it can be
        merged directly into the pipeline's smo dict. Falls back gracefully:
        missing fields are omitted and config.json values are kept as fallback.
        """
        headers = {
            "Authorization":           f"Bearer {self.access_token}",
            "Content-Type":            "application/json",
            "X-EBAY-C-MARKETPLACE-ID": marketplace_id,
        }
        result: Dict = {}

        # ── 1. Seller Standards Profile (seller level, defect, late ship, cases) ──
        try:
            r = requests.get(
                f"{self.base_url}/sell/analytics/v1/seller_standards_profile",
                headers=headers, timeout=30,
            )
            r.raise_for_status()
            data = r.json()

            profiles    = data.get("standardsProfiles", [])
            current_p   = next((p for p in profiles
                                if p.get("cycle", {}).get("cycleType") == "CURRENT"),  None)
            projected_p = next((p for p in profiles
                                if p.get("cycle", {}).get("cycleType") == "PROJECTED"), None)

            def _fmt_date(iso: str) -> str:
                try:
                    return datetime.strptime(iso[:10], "%Y-%m-%d").strftime("%d %b %Y")
                except Exception:
                    return iso[:10]

            def _period(m: Dict) -> str:
                s = _fmt_date(m.get("lookbackStartDate", ""))
                e = _fmt_date(m.get("lookbackEndDate", ""))
                return f"{s} \u2013 {e}" if s and e else ""

            if current_p:
                level = current_p.get("sellerLevel", "")
                result["seller_level"] = level.replace("_", " ").title() if level else ""

                eval_date = current_p.get("cycle", {}).get("evaluationDate", "")
                if eval_date:
                    result["next_evaluation"] = eval_date[:10]

                by_name = {m["name"]: m for m in current_p.get("metrics", [])}

                tdr = by_name.get("TRANSACTION_DEFECT_RATE", {})
                if "value" in tdr:
                    result["defect_rate"]          = round(float(tdr["value"]) * 100, 2)
                    result["defect_threshold_top"] = 0.5
                    result["defect_period"]        = _period(tdr)

                lsr = by_name.get("LATE_SHIPMENT_RATE", {})
                if "value" in lsr:
                    result["late_ship_rate"]          = round(float(lsr["value"]) * 100, 2)
                    result["late_ship_threshold_top"] = 5.0
                    result["late_ship_period"]        = _period(lsr)

                cas = by_name.get("CASES_AS_A_PERCENTAGE_OF_TOTAL_TRANSACTIONS", {})
                if "value" in cas:
                    result["cases_rate"]          = round(float(cas["value"]) * 100, 2)
                    result["cases_threshold_top"] = 0.3

            if projected_p:
                level = projected_p.get("sellerLevel", "")
                result["seller_level_projected"] = level.replace("_", " ").title() if level else ""

            print(f"  [Analytics] Seller level: {result.get('seller_level', '?')} "
                  f"-> projected {result.get('seller_level_projected', '?')}")
            print(f"  [Analytics] Defect: {result.get('defect_rate', '?')}%  "
                  f"Late: {result.get('late_ship_rate', '?')}%  "
                  f"Cases: {result.get('cases_rate', '?')}%")

        except Exception as e:
            print(f"  [WARN] seller_standards_profile failed: {e}")

        # ── 2. Customer Service Metrics (INAD + INR) ──────────────────────────
        for metric_type, prefix in [
            ("ITEM_NOT_AS_DESCRIBED", "inad"),
            ("ITEM_NOT_RECEIVED",     "inr"),
        ]:
            try:
                r = requests.get(
                    f"{self.base_url}/sell/analytics/v1/customer_service_metric_summary",
                    headers=headers,
                    params={
                        "customer_service_metric_type": metric_type,
                        "evaluation_marketplace_id":    marketplace_id,
                        "evaluation_type":              "CURRENT",
                    },
                    timeout=30,
                )
                r.raise_for_status()
                d = r.json()

                rate       = float(d.get("metricPercent",        d.get("ratePercent", 0))) * 100
                peer_rate  = float(d.get("peerBenchmarkPercent", d.get("benchmarkPercent", 0))) * 100
                raw_rating = d.get("rating", "AVERAGE")
                rating     = raw_rating.replace("_", " ").title()

                cycle = d.get("evaluationCycle", d.get("cycle", {}))
                s = cycle.get("startDate", "")[:7]
                e = cycle.get("endDate",   "")[:7]
                try:
                    fmt = lambda x: datetime.strptime(x, "%Y-%m").strftime("%b %Y") if x else ""
                    period = f"{fmt(s)} \u2013 {fmt(e)}" if s and e else ""
                except Exception:
                    period = ""

                result[f"{prefix}_rate"]      = round(rate, 2)
                result[f"{prefix}_peer_rate"] = round(peer_rate, 2)
                result[f"{prefix}_rating"]    = rating
                result[f"{prefix}_period"]    = period

                print(f"  [Analytics] {prefix.upper()}: {round(rate, 2)}%  "
                      f"(peer {round(peer_rate, 2)}%, {rating})")

            except Exception as e:
                print(f"  [WARN] customer_service_metric {metric_type} failed: {e}")

        return result


def format_ebay_metrics_for_dashboard(metrics: Dict) -> Dict:
    """
    Format raw eBay API metrics into dashboard-friendly format.
    Handles both Trading API (Auth'n'Auth) and REST API (OAuth) metric shapes.
    Used by update_dashboard.py to inject into the DASHBOARD_DATA object.
    """
    pct = metrics.get("feedback_percent", 0)
    dsr = metrics.get("dsr", {})

    return {
        # ── Feedback ──────────────────────────────────────────────────────────
        "feedback_score":   metrics.get("feedback_score", 0),
        "feedback_percent": pct,
        # rating: 0.0–1.0 scale (99.7% → 0.997) — used by store cards
        "rating":           round(pct / 100, 4),
        "listings":         metrics.get("listings", 0),

        # ── Seller Level (current + projected) ────────────────────────────────
        "seller_level":           metrics.get("seller_level", ""),
        "seller_level_projected": metrics.get("seller_level_projected", ""),
        "next_evaluation":        metrics.get("next_evaluation", ""),

        # ── Transaction Defect Rate ───────────────────────────────────────────
        "defect_rate":            metrics.get("defect_rate", 0.0),
        "defect_count":           metrics.get("defect_count", 0),
        "defect_total":           metrics.get("defect_total", 0),
        "defect_threshold_top":   metrics.get("defect_threshold_top", 0.50),
        "defect_period":          metrics.get("defect_period", ""),

        # ── Late Shipment Rate ────────────────────────────────────────────────
        "late_ship_rate":          metrics.get("late_ship_rate", 0.0),
        "late_ship_count":         metrics.get("late_ship_count", 0),
        "late_ship_total":         metrics.get("late_ship_total", 0),
        "late_ship_threshold_top": metrics.get("late_ship_threshold_top", 5.00),
        "late_ship_period":        metrics.get("late_ship_period", ""),

        # ── Cases Without Seller Resolution ───────────────────────────────────
        "cases_rate":             metrics.get("cases_rate", 0.0),
        "cases_count":            metrics.get("cases_count", 0),
        "cases_total":            metrics.get("cases_total", 0),
        "cases_threshold_top":    metrics.get("cases_threshold_top", 0.30),

        # ── INAD ──────────────────────────────────────────────────────────────
        "inad_rate":      metrics.get("inad_rate", 0.0),
        "inad_count":     metrics.get("inad_count", 0),
        "inad_total":     metrics.get("inad_total", 0),
        "inad_peer_rate": metrics.get("inad_peer_rate", 0.0),
        "inad_rating":    metrics.get("inad_rating", ""),
        "inad_period":    metrics.get("inad_period", ""),

        # ── INR ───────────────────────────────────────────────────────────────
        "inr_rate":       metrics.get("inr_rate", 0.0),
        "inr_count":      metrics.get("inr_count", 0),
        "inr_total":      metrics.get("inr_total", 0),
        "inr_peer_rate":  metrics.get("inr_peer_rate", 0.0),
        "inr_rating":     metrics.get("inr_rating", ""),
        "inr_period":     metrics.get("inr_period", ""),

        # ── DSR ───────────────────────────────────────────────────────────────
        "dsr": {
            "item_description": dsr.get("item_description", 0.0),
            "communication":    dsr.get("communication", 0.0),
            "shipping_time":    dsr.get("shipping_time", 0.0),
            "return_handling":  dsr.get("return_handling", 0.0),
        },
        "last_updated": metrics.get("timestamp", ""),
    }


if __name__ == "__main__":
    # Quick test script
    import sys

    if len(sys.argv) < 4:
        print("Usage: python ebay_client.py <app_id> <cert_id> <access_token>")
        sys.exit(1)

    client = eBayClient(
        app_id=sys.argv[1],
        cert_id=sys.argv[2],
        access_token=sys.argv[3],
        sandbox_mode=False
    )

    try:
        metrics = client.get_all_metrics()
        print(json.dumps(metrics, indent=2))
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
