#!/usr/bin/env python3
"""
D-Flector / Zivor Dashboard -- Automated Data Pipeline
======================================================
Pulls data from:
  1. Unleashed ERP     -- D-Flector trade orders
  2. Neto (Maropost)   -- Zivor eBay + Zivor Web sales, ratings, listings
  3. Shopify           -- AMS (eBay + Web) and ATS (eBay + Web) sales
  4. TradeMe API       -- Zivor NZ marketplace sales

Then injects fresh data into dashboard.html.

Usage:
    python update_dashboard.py                  # uses config.json in same folder
    python update_dashboard.py --config /path/to/config.json
    python update_dashboard.py --dry-run        # print data without updating HTML

Requirements:
    pip install requests
"""

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import time
import base64
from datetime import datetime, timedelta
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library required. Install with: pip install requests")
    sys.exit(1)

# eBay client (optional -- only active when config.json has "ebay" section with token)
try:
    from ebay_client import eBayClient, format_ebay_metrics_for_dashboard
    _EBAY_CLIENT_AVAILABLE = True
except ImportError:
    _EBAY_CLIENT_AVAILABLE = False


# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────

def load_config(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


# ─────────────────────────────────────────────
#  1. UNLEASHED API  (D-Flector Trade Orders)
# ─────────────────────────────────────────────

class UnleashedClient:
    """Connects to Unleashed Software REST API to pull D-Flector sales orders."""

    def __init__(self, api_id, api_key, base_url="https://api.unleashedsoftware.com"):
        self.api_id = api_id
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def _sign(self, query_string):
        return base64.b64encode(
            hmac.new(self.api_key.encode(), query_string.encode(), hashlib.sha256).digest()
        ).decode()

    def _get(self, endpoint, params=None):
        qs = "&".join(f"{k}={quote(str(v))}" for k, v in (params or {}).items())
        sig = self._sign(qs)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "api-auth-id": self.api_id,
            "api-auth-signature": sig,
        }
        url = f"{self.base_url}/{endpoint}" + (f"?{qs}" if qs else "")
        for _attempt in range(2):
            try:
                r = requests.get(url, headers=headers, timeout=45)
                r.raise_for_status()
                return r.json()
            except requests.exceptions.Timeout:
                if _attempt == 0:
                    time.sleep(5)
                    continue
                raise

    def get_sales_orders(self, start_date, end_date, page=1):
        return self._get("SalesOrders", {
            "startDate": start_date,
            "endDate": end_date,
            "page": page,
            "pageSize": 200,
        })

    @staticmethod
    def _parse_date(date_val):
        """Parse an Unleashed date field to YYYY-MM-DD string.
        Handles ISO format (2026-03-15T00:00:00) and .NET JSON (/Date(...)/)."""
        if not date_val:
            return ""
        s = str(date_val)
        # .NET JSON date format: /Date(1234567890000)/ or /Date(1234567890000+0000)/
        m = re.search(r'/Date\((\d+)([+-]\d+)?\)/', s)
        if m:
            ts_ms = int(m.group(1))
            return datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")
        # ISO format: 2026-03-15T00:00:00 or similar
        if len(s) >= 10 and s[4:5] == "-":
            return s[:10]
        return ""

    def fetch_all_orders(self, start_date, end_date):
        """Fetch all sales orders across a date range by iterating month-by-month.

        The Unleashed SalesOrders API only reliably returns the first page of results
        regardless of the page parameter.  Keeping each query to a single calendar
        month ensures fewer than 200 orders per request so no pagination is needed,
        and every order is captured without duplicates.
        """
        from datetime import date as _date

        start = _date.fromisoformat(start_date[:10])
        end   = _date.fromisoformat(end_date[:10])

        seen = set()
        all_orders = []

        # Walk forward one month at a time
        cur_year  = start.year
        cur_month = start.month
        while True:
            # Window for this month
            m_start = f"{cur_year}-{cur_month:02d}-01"
            if cur_month == 12:
                m_end = f"{cur_year + 1}-01-01"
            else:
                m_end = f"{cur_year}-{cur_month + 1:02d}-01"

            # Clip to requested range
            if m_start < start_date:
                m_start = start_date
            if m_end > end_date:
                m_end = end_date

            data  = self.get_sales_orders(m_start, m_end, 1)
            items = data.get("Items", [])
            for order in items:
                num = order.get("OrderNumber")
                if num not in seen:
                    seen.add(num)
                    all_orders.append(order)

            # Advance to next month
            if cur_month == 12:
                cur_month = 1
                cur_year += 1
            else:
                cur_month += 1

            # Stop once we've passed the end date
            next_start = f"{cur_year}-{cur_month:02d}-01"
            if next_start > end_date:
                break

            time.sleep(0.2)

        return all_orders

    def get_monthly_trade_data(self, year, month, include_prev_open=False):
        """Compute completed and open trade order totals for a given month.

        Completed : Status = 'Completed'  AND  CompletedDate within target month.

        Open      : Status ≠ 'Completed'  AND  Status ≠ 'Deleted'
                    AND  RequiredDate within the target month  OR  (when
                    include_prev_open=True) also the immediately preceding month.

        A 12-month OrderDate window is fetched so orders placed earlier but
        completed or required in the target window are correctly captured.
        """
        # ── Target month boundaries ──────────────────────────────────────────────
        start = f"{year}-{month:02d}-01"
        end   = f"{year}-{month + 1:02d}-01" if month < 12 else f"{year + 1}-01-01"

        # ── Open-orders RequiredDate lower bound ─────────────────────────────────
        # include_prev_open -> current month + 2 previous months
        # otherwise         -> current month only
        if include_prev_open:
            prev_m = month - 2 if month > 2 else month + 10
            prev_y = year      if month > 2 else year - 1
            open_start = f"{prev_y}-{prev_m:02d}-01"
        else:
            open_start = start

        # ── Fetch: 3 months back by OrderDate (for open orders scope) ───────────
        # Completed orders also benefit from a broader look-back, but 3 months
        # keeps the open order pool scoped to recent activity as requested.
        lb_y, lb_m = year, month
        for _ in range(3):
            lb_m -= 1
            if lb_m <= 0:
                lb_m += 12
                lb_y -= 1
        fetch_start = f"{lb_y}-{lb_m:02d}-01"

        totals = {
            "completed": 0.0, "open": 0.0,
            "c_cats": {"Boat": 0.0, "Caravan": 0.0, "Jetski": 0.0, "Website": 0.0},
            "o_cats": {"Boat": 0.0, "Caravan": 0.0, "Jetski": 0.0, "Website": 0.0},
            "cats":   {"Boat": 0.0, "Caravan": 0.0, "Jetski": 0.0, "Website": 0.0},
        }

        for order in self.fetch_all_orders(fetch_start, end):
            status   = order.get("OrderStatus", "")
            subtotal = float(order.get("SubTotal", 0) or 0)
            cat      = categorise_order(order)

            if status == "Completed":
                # ── Completed: CompletedDate in target month ──
                comp_date = self._parse_date(order.get("CompletedDate", ""))
                if comp_date and start <= comp_date < end:
                    totals["completed"]       += subtotal
                    totals["c_cats"][cat]      = totals["c_cats"].get(cat, 0) + subtotal
                    totals["cats"][cat]        = totals["cats"].get(cat, 0)   + subtotal

            elif status != "Deleted":
                # ── Open: RequiredDate in open window (current + optional prev month) ──
                req_date = self._parse_date(order.get("RequiredDate", ""))
                if req_date and open_start <= req_date < end:
                    totals["open"]         += subtotal
                    totals["o_cats"][cat]   = totals["o_cats"].get(cat, 0) + subtotal
                    totals["cats"][cat]     = totals["cats"].get(cat, 0)   + subtotal

        return {
            "completed": round(totals["completed"], 2),
            "open":      round(totals["open"],      2),
            "total":     round(totals["completed"] + totals["open"], 2),
            "categories_completed": {k: round(v, 2) for k, v in totals["c_cats"].items()},
            "categories_open":      {k: round(v, 2) for k, v in totals["o_cats"].items()},
            "categories_total":     {k: round(v, 2) for k, v in totals["cats"].items()},
        }


def categorise_order(order):
    """Categorise an Unleashed order into Boat/Caravan/Jetski/Website."""
    lines = order.get("SalesOrderLines", [])
    for line in lines:
        code = (line.get("Product", {}).get("ProductCode", "") or "").upper()
        if any(x in code for x in ["D4", "D5", "D6", "D7"]):
            return "Boat"
        if any(x in code for x in ["JET", "J100"]):
            return "Jetski"
    source = (order.get("SalesOrderGroup", "") or "").lower()
    if "web" in source or "online" in source:
        return "Website"
    return "Caravan"


# ─────────────────────────────────────────────
#  2. NETO (MAROPOST) API  (Zivor Stores)
# ─────────────────────────────────────────────

class NetoClient:
    """Connects to Neto (Maropost Commerce Cloud) API for Zivor store data.
    Neto handles both the Zivor eBay channel and Zivor webstore."""

    def __init__(self, store_url, api_key, username):
        self.store_url = store_url.rstrip("/")
        self.api_key = api_key
        self.username = username

    def _post(self, action, body):
        url = f"{self.store_url}/do/WS/NetoAPI"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "NETOAPI_ACTION": action,
            "NETOAPI_KEY": self.api_key,
        }
        if self.username:
            headers["NETOAPI_USERNAME"] = self.username
        r = requests.post(url, headers=headers, json=body, timeout=60)
        r.raise_for_status()
        return r.json()

    def get_orders(self, date_from, date_to, page=0, limit=200):
        """Fetch orders within a date range."""
        body = {
            "Filter": {
                "DatePlacedFrom": date_from,
                "DatePlacedTo": date_to,
                "OrderStatus": ["New", "Pick", "Pack", "Dispatched", "On Hold"],
                "Page": page,
                "Limit": limit,
                "OutputSelector": [
                    "OrderID", "OrderStatus", "GrandTotal", "OrderLine",
                    "SalesChannel", "DatePlaced", "SubTotal",
                ],
            }
        }
        return self._post("GetOrder", body)

    def get_all_orders(self, date_from, date_to):
        """Paginate through all orders."""
        all_orders = []
        page = 0
        while True:
            data = self.get_orders(date_from, date_to, page=page)
            orders = data.get("Order", [])
            if not orders:
                break
            all_orders.extend(orders)
            if len(orders) < 200:
                break
            page += 1
            time.sleep(0.3)
        return all_orders

    def get_content_count(self):
        """Count active product listings."""
        body = {
            "Filter": {
                "IsActive": "True",
                "OutputSelector": ["SKU"],
                "Limit": 1,
            }
        }
        # Neto doesn't return total count directly; use inventory count endpoint
        data = self._post("GetItem", body)
        # Approximate -- for exact count we'd paginate, but this gives the idea
        return len(data.get("Item", []))

    def get_store_data(self, store_config, targets, month_start, today):
        """Build store data for both Zivor eBay and Zivor Web channels."""
        all_orders = self.get_all_orders(month_start, today)

        ebay_revenue = 0
        web_revenue = 0
        ebay_completed = 0
        web_completed = 0

        # Neto statuses to skip entirely
        NETO_SKIP = {"cancelled", "refunded", "deleted", "incomplete"}

        for order in all_orders:
            status = (order.get("OrderStatus", "") or "").lower()
            if status in NETO_SKIP:
                continue
            # GrandTotal includes 10% GST -- divide by 1.1 to get ex-GST amount
            gross = float(order.get("GrandTotal", 0) or order.get("SubTotal", 0) or 0)
            subtotal = round(gross / 1.1, 2)
            channel = (order.get("SalesChannel", "") or "").lower()

            is_completed = status in ("dispatched", "completed")

            if "ebay" in channel:
                ebay_revenue += subtotal
                if is_completed:
                    ebay_completed += subtotal
            else:
                web_revenue += subtotal
                if is_completed:
                    web_completed += subtotal

        # 7-day trend calculation
        week_ago = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_week_start = (datetime.strptime(week_ago, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

        recent_orders = self.get_all_orders(prev_week_start, today)
        ebay_7d = 0
        ebay_prev7d = 0
        web_7d = 0
        web_prev7d = 0

        for order in recent_orders:
            if (order.get("OrderStatus", "") or "").lower() in NETO_SKIP:
                continue
            gross = float(order.get("GrandTotal", 0) or order.get("SubTotal", 0) or 0)
            subtotal = round(gross / 1.1, 2)
            channel = (order.get("SalesChannel", "") or "").lower()
            placed = order.get("DatePlaced", "")[:10]

            if placed >= week_ago:
                if "ebay" in channel:
                    ebay_7d += subtotal
                else:
                    web_7d += subtotal
            else:
                if "ebay" in channel:
                    ebay_prev7d += subtotal
                else:
                    web_prev7d += subtotal

        ebay_trend = (ebay_7d - ebay_prev7d) / ebay_prev7d if ebay_prev7d else 0
        web_trend = (web_7d - web_prev7d) / web_prev7d if web_prev7d else 0

        # Daily 7d breakdown for Combined Retail mini chart
        seven_days_ago = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        ebay_daily7d = {}
        web_daily7d = {}
        for order in all_orders:
            if (order.get("OrderStatus", "") or "").lower() in NETO_SKIP:
                continue
            placed = (order.get("DatePlaced", "") or "")[:10]
            if not placed or placed < seven_days_ago or placed > today:
                continue
            gross = float(order.get("GrandTotal", 0) or order.get("SubTotal", 0) or 0)
            subtotal = round(gross / 1.1, 2)
            channel = (order.get("SalesChannel", "") or "").lower()
            if "ebay" in channel:
                ebay_daily7d[placed] = ebay_daily7d.get(placed, 0) + subtotal
            else:
                web_daily7d[placed] = web_daily7d.get(placed, 0) + subtotal

        return [
            {
                "name": "Zivor (eBay)",
                "target": targets.get("zivor_ebay", 150000),
                "achieved": round(ebay_revenue, 2),
                "rating": store_config.get("ebay_rating", 0.997),
                "listings": store_config.get("ebay_listings", 0),
                "trend7d": round(ebay_trend, 3),
                "source": "neto",
                "daily7d": ebay_daily7d,
            },
            {
                "name": "Zivor (Web)",
                "target": targets.get("zivor_web", 20000),
                "achieved": round(web_revenue, 2),
                "rating": store_config.get("web_rating", 4.6),
                "listings": store_config.get("web_listings", 0),
                "trend7d": round(web_trend, 3),
                "source": "neto",
                "daily7d": web_daily7d,
            },
        ]


# ─────────────────────────────────────────────
#  3. SHOPIFY API  (AMS + ATS Stores)
# ─────────────────────────────────────────────

class ShopifyClient:
    """Connects to Shopify Admin REST API.
    Shopify manages both the eBay sales channel and webstore for AMS and ATS.
    Supports both Custom App tokens (shpat_...) and Private App passwords (basic auth)."""

    def __init__(self, shop_url, access_token, api_key=None):
        self.shop_url = shop_url.rstrip("/")
        self.access_token = access_token
        self.api_key = api_key  # needed for private app basic auth

    def _get(self, endpoint, params=None):
        url = f"{self.shop_url}/admin/api/2024-01/{endpoint}.json"
        # Try Custom App header auth first, fall back to Private App basic auth
        if self.access_token.startswith("shpat_"):
            r = requests.get(url, headers={
                "X-Shopify-Access-Token": self.access_token,
                "Content-Type": "application/json",
            }, params=params, timeout=30)
        elif self.api_key:
            # Private App: basic auth with api_key:password
            r = requests.get(url, auth=(self.api_key, self.access_token),
                             headers={"Content-Type": "application/json"},
                             params=params, timeout=30)
        else:
            # Try as access token header (works for some token formats)
            r = requests.get(url, headers={
                "X-Shopify-Access-Token": self.access_token,
                "Content-Type": "application/json",
            }, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def get_orders(self, start_date, end_date, status="any"):
        """Fetch all orders within date range, handling pagination."""
        all_orders = []
        params = {
            "created_at_min": f"{start_date}T00:00:00+10:00",
            "created_at_max": f"{end_date}T23:59:59+10:00",
            "status": status,
            "limit": 250,
            "fields": "id,total_price,subtotal_price,financial_status,source_name,created_at,tags,cancelled_at",
        }
        data = self._get("orders", params)
        all_orders.extend(data.get("orders", []))
        # Note: for >250 orders, implement link-header pagination
        return all_orders

    def get_product_count(self):
        data = self._get("products/count", {"status": "active"})
        return data.get("count", 0)

    def get_store_data(self, brand, targets, month_start, today, store_config):
        """Build store data for both eBay and Web channels of this Shopify store."""
        orders = self.get_orders(month_start, today)

        ebay_revenue = 0
        web_revenue = 0

        for o in orders:
            # Skip refunded, voided, partially refunded, and cancelled orders
            if o.get("financial_status") in ("refunded", "voided", "partially_refunded"):
                continue
            if o.get("cancelled_at"):  # Order was cancelled
                continue
            # subtotal_price is already ex-GST in Shopify (line items before tax)
            subtotal = float(o.get("subtotal_price", 0))
            source = (o.get("source_name", "") or "").lower()
            tags = (o.get("tags", "") or "").lower()

            # Shopify marks eBay channel orders with source_name or tags
            if "ebay" in source or "ebay" in tags:
                ebay_revenue += subtotal
            else:
                web_revenue += subtotal

        # 7-day trend
        week_ago = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_week_start = (datetime.strptime(week_ago, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

        recent = self.get_orders(prev_week_start, today)
        ebay_7d = ebay_prev7d = web_7d = web_prev7d = 0
        for o in recent:
            if o.get("financial_status") in ("refunded", "voided", "partially_refunded"):
                continue
            if o.get("cancelled_at"):
                continue
            subtotal = float(o.get("subtotal_price", 0))
            source = (o.get("source_name", "") or "").lower()
            tags = (o.get("tags", "") or "").lower()
            created = o.get("created_at", "")[:10]
            is_ebay = "ebay" in source or "ebay" in tags

            if created >= week_ago:
                if is_ebay:
                    ebay_7d += subtotal
                else:
                    web_7d += subtotal
            else:
                if is_ebay:
                    ebay_prev7d += subtotal
                else:
                    web_prev7d += subtotal

        ebay_trend = (ebay_7d - ebay_prev7d) / ebay_prev7d if ebay_prev7d else 0
        web_trend = (web_7d - web_prev7d) / web_prev7d if web_prev7d else 0
        products = self.get_product_count()

        # Daily 7d breakdown for Combined Retail mini chart
        # Shopify returns created_at in UTC; convert to AEST (UTC+10) so the date
        # matches the Australian calendar day the sale actually occurred on.
        seven_days_ago = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        ebay_daily7d = {}
        web_daily7d = {}
        skip_statuses = {"refunded", "voided", "partially_refunded"}
        for o in orders:
            if o.get("financial_status") in skip_statuses or o.get("cancelled_at"):
                continue
            raw_ts = (o.get("created_at", "") or "")
            try:
                # Parse UTC timestamp and shift to AEDT (UTC+11, active until Apr 5 2026)
                dt_utc = datetime.strptime(raw_ts[:19], "%Y-%m-%dT%H:%M:%S")
                created = (dt_utc + timedelta(hours=11)).strftime("%Y-%m-%d")
            except Exception:
                created = raw_ts[:10]
            if not created or created < seven_days_ago or created > today:
                continue
            subtotal = float(o.get("subtotal_price", 0))
            source = (o.get("source_name", "") or "").lower()
            tags = (o.get("tags", "") or "").lower()
            if "ebay" in source or "ebay" in tags:
                ebay_daily7d[created] = ebay_daily7d.get(created, 0) + subtotal
            else:
                web_daily7d[created] = web_daily7d.get(created, 0) + subtotal

        ebay_key = f"{brand.lower()}_ebay"
        web_key = f"{brand.lower()}_web"

        return [
            {
                "name": f"{brand} (eBay)",
                "target": targets.get(ebay_key, 15000),
                "achieved": round(ebay_revenue, 2),
                "rating": store_config.get("ebay_rating", 1.0),
                "listings": store_config.get("ebay_listings", 0),
                "trend7d": round(ebay_trend, 3),
                "source": "shopify",
                "daily7d": ebay_daily7d,
            },
            {
                "name": f"{brand} (Web)",
                "target": targets.get(web_key, 5000),
                "achieved": round(web_revenue, 2),
                "rating": store_config.get("web_rating", 5.0),
                "listings": products,
                "trend7d": round(web_trend, 3),
                "source": "shopify",
                "daily7d": web_daily7d,
            },
        ]


# ─────────────────────────────────────────────
#  4. TRADEME API  (NZ Marketplace)
# ─────────────────────────────────────────────

class TradeMeClient:
    """Connects to TradeMe API (NZ marketplace) for Zivor NZ sales."""

    TOKEN_URL = "https://api.trademe.co.nz/Oauth/AccessToken"

    def __init__(self, consumer_key, consumer_secret, oauth_token, oauth_token_secret):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.oauth_token = oauth_token
        self.oauth_token_secret = oauth_token_secret
        self.base_url = "https://api.trademe.co.nz/v1"

    def _get(self, endpoint, params=None):
        """Make OAuth 1.0a signed GET request to TradeMe API."""
        from urllib.parse import urlencode
        import secrets
        import urllib.parse

        url = f"{self.base_url}/{endpoint}.json"
        timestamp = str(int(time.time()))
        nonce = secrets.token_hex(16)

        oauth_params = {
            "oauth_consumer_key": self.consumer_key,
            "oauth_token": self.oauth_token,
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": timestamp,
            "oauth_nonce": nonce,
            "oauth_version": "1.0",
        }

        all_params = {**oauth_params, **(params or {})}
        param_string = "&".join(f"{quote(k)}={quote(str(v))}" for k, v in sorted(all_params.items()))
        base_string = f"GET&{quote(url, safe='')}&{quote(param_string, safe='')}"
        signing_key = f"{quote(self.consumer_secret)}&{quote(self.oauth_token_secret)}"
        sig = base64.b64encode(
            hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha1).digest()
        ).decode()

        oauth_params["oauth_signature"] = sig
        auth_header = "OAuth " + ", ".join(f'{k}="{quote(v)}"' for k, v in oauth_params.items())

        r = requests.get(url, headers={"Authorization": auth_header}, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def get_sold_items(self, page=1):
        """Get list of sold items from seller dashboard."""
        return self._get("MyTradeMe/SoldItems", {"page": page, "rows": 100})

    @staticmethod
    def _parse_dotnet_date(date_str):
        """Parse .NET JSON date format /Date(timestamp)/ to YYYY-MM-DD string."""
        if not date_str:
            return ""
        # Handle /Date(1234567890000)/ and /Date(1234567890000+0000)/ formats
        m = re.search(r'/Date\((\d+)([+-]\d+)?\)/', str(date_str))
        if m:
            timestamp_ms = int(m.group(1))
            return datetime.utcfromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")
        # Fallback: if it's already a YYYY-MM-DD string
        if isinstance(date_str, str) and len(date_str) >= 10 and date_str[4:5] == "-":
            return date_str[:10]
        return ""

    def get_sales_total(self, month_start, today):
        """Sum revenue from sold items within the date range."""
        total = 0
        page = 1
        while True:
            data = self.get_sold_items(page)
            items = data.get("List", [])
            if not items:
                break
            for item in items:
                sold_date = self._parse_dotnet_date(item.get("SoldDate", ""))
                if sold_date and month_start <= sold_date <= today:
                    gross = float(item.get("SelectedBuyNowPrice", 0) or item.get("MaxBidAmount", 0) or 0)
                    qty = int(item.get("QuantitySold", 1) or 1)
                    # Divide by 1.1 to remove 10% GST and get ex-tax amount
                    total += round(gross / 1.1, 2) * qty
            if not data.get("HasNext", False):
                break
            page += 1
            time.sleep(0.5)
        return round(total, 2)

    def get_listing_count(self):
        """Count of active listings."""
        data = self._get("MyTradeMe/SellingItems", {"rows": 1})
        return data.get("TotalCount", 0)

    def get_store_data(self, targets, month_start, today, store_config):
        # Fetch sold items once and compute both revenue total and daily 7d breakdown
        seven_days_ago = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_week = (datetime.strptime(seven_days_ago, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

        total = 0
        rev_7d = 0
        rev_prev7d = 0
        daily7d = {}

        page = 1
        while True:
            data = self.get_sold_items(page)
            items = data.get("List", [])
            if not items:
                break
            for item in items:
                sold_date = self._parse_dotnet_date(item.get("SoldDate", ""))
                if not sold_date:
                    continue
                gross = float(item.get("SelectedBuyNowPrice", 0) or item.get("MaxBidAmount", 0) or 0)
                qty = int(item.get("QuantitySold", 1) or 1)
                subtotal = round(gross / 1.1, 2) * qty
                if month_start <= sold_date <= today:
                    total += subtotal
                if sold_date >= seven_days_ago and sold_date <= today:
                    rev_7d += subtotal
                    daily7d[sold_date] = daily7d.get(sold_date, 0) + subtotal
                elif sold_date >= prev_week and sold_date < seven_days_ago:
                    rev_prev7d += subtotal
            if not data.get("HasNext", False):
                break
            page += 1
            time.sleep(0.5)

        revenue = round(total, 2)
        trend = (rev_7d - rev_prev7d) / rev_prev7d if rev_prev7d else 0
        listings = self.get_listing_count()

        return {
            "name": "TradeMe (Zivor)",
            "target": targets.get("trademe_zivor", 15000),
            "achieved": revenue,
            "rating": store_config.get("rating", 0.935),
            "listings": listings,
            "trend7d": round(trend, 3),
            "source": "trademe",
            "daily7d": daily7d,
        }


# ─────────────────────────────────────────────
#  HERO IMAGE GENERATOR
# ─────────────────────────────────────────────

def generate_hero_image(trade, stores_data, now, targets, output_path):
    """Generate a PNG snapshot of the two hero cards (D-Flector MTD + Combined Retail).
    Saved to output_path and also copied to OneDrive for Teams sharing."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        from matplotlib.patches import FancyBboxPatch
        import matplotlib.patheffects as pe
    except ImportError:
        print("  [WARN] matplotlib not available -- skipping hero image")
        return False

    total_retail        = sum(s.get("achieved", 0) for s in stores_data)
    total_retail_target = sum(s.get("target",   0) for s in stores_data)
    retail_pct  = total_retail / total_retail_target if total_retail_target else 0
    trade_target = targets.get("dflector_trade", 250000)
    trade_pct   = trade["total"] / trade_target if trade_target else 0

    # Neto / Shopify / TradeMe subtotals for breakdown
    neto_total     = sum(s["achieved"] for s in stores_data if s.get("source") == "neto")
    shopify_total  = sum(s["achieved"] for s in stores_data if s.get("source") == "shopify")
    trademe_total  = sum(s["achieved"] for s in stores_data if s.get("source") == "trademe")

    BG      = "#0f1117"
    GREEN   = "#00c896"
    BLUE    = "#4da6ff"
    CARD_GR = "#0d2b22"
    CARD_BL = "#0d1f3c"
    WHITE   = "#ffffff"
    GREY    = "#8a9bb0"

    fig = plt.figure(figsize=(14, 5.2), facecolor=BG)

    def draw_card(ax, bg_col, accent, title, big_num, badge_text, sub_lines):
        ax.set_facecolor(bg_col)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
        # Border
        rect = FancyBboxPatch((0.02, 0.02), 0.96, 0.96,
                               boxstyle="round,pad=0.02",
                               linewidth=2, edgecolor=accent,
                               facecolor=bg_col, zorder=0)
        ax.add_patch(rect)
        # Title
        ax.text(0.06, 0.88, title, color=GREY, fontsize=11,
                fontweight="bold", va="top", transform=ax.transAxes)
        # Big number
        ax.text(0.06, 0.72, big_num, color=accent, fontsize=34,
                fontweight="bold", va="top", transform=ax.transAxes)
        # Badge pill
        ax.text(0.06, 0.48, badge_text, color=accent, fontsize=11,
                fontweight="bold", va="top", transform=ax.transAxes,
                bbox=dict(boxstyle="round,pad=0.3", facecolor=bg_col,
                          edgecolor=accent, linewidth=1.5))
        # Sub lines
        y = 0.33
        for line in sub_lines:
            ax.text(0.06, y, line, color=GREY, fontsize=9.5,
                    va="top", transform=ax.transAxes)
            y -= 0.10

    # Left card -- D-Flector
    ax1 = fig.add_axes([0.02, 0.05, 0.47, 0.90])
    draw_card(
        ax1, CARD_GR, GREEN,
        "D-FLECTOR TRADE  (MTD)",
        f"${trade['completed']:,.0f}",
        f"{trade_pct:.0%}  of  ${trade_target:,.0f}  target",
        [
            f"Completed:      ${trade['completed']:,.0f}",
            f"Open Orders:  ${trade['open']:,.0f}",
            f"Total Pipeline:  ${trade['total']:,.0f}",
        ]
    )

    # Right card -- Combined Retail
    ax2 = fig.add_axes([0.51, 0.05, 0.47, 0.90])
    draw_card(
        ax2, CARD_BL, BLUE,
        "COMBINED RETAIL  (Last 31 days, T-1)",
        f"${total_retail:,.0f}",
        f"{retail_pct:.0%}  of  ${total_retail_target:,.0f}  target",
        [
            f"Neto (Zivor):         ${neto_total:,.0f}",
            f"Shopify (AMS+ATS): ${shopify_total:,.0f}",
            f"TradeMe (NZ):       ${trademe_total:,.0f}",
        ]
    )

    # Footer date
    fig.text(0.5, 0.005, f"Generated  {now.strftime('%d %b %Y  %I:%M %p')}  |  Zivor Dashboard",
             ha="center", color=GREY, fontsize=8)

    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close()
    print(f"  [OK] Hero image saved -> {output_path}")
    return True


# ─────────────────────────────────────────────
#  ONEDRIVE PUBLISH
# ─────────────────────────────────────────────

PUBLISH_FOLDER_PATHS = [
    r"C:\Users\anoop\D-Flector Stone Guards\D-Flector Commercial - Documents",
]
SHAREPOINT_BASE = (
    "https://dflector.sharepoint.com/sites/D-FlectorCommercial/Shared%20Documents"
)
DASHBOARD_FILENAME = "Daily Dashboard.html"
SNAPSHOT_FILENAME  = "Daily Dashboard Snapshot.png"

def copy_to_onedrive(html_path, image_path=None):
    """Copy 'Daily Dashboard.html' (and snapshot) to the D-Flector Commercial SharePoint
    document library synced via OneDrive.  File is instantly accessible to all team
    members via the persistent SharePoint link -- no re-sharing needed.
    Returns (dashboard_url, image_url) or (None, None) if folder not found."""
    import shutil

    folder = None
    for candidate in PUBLISH_FOLDER_PATHS:
        if os.path.exists(candidate):
            folder = candidate
            break

    if not folder:
        print(f"  [WARN] Publish folder not found -- skipping publish")
        print(f"         Looked in: {PUBLISH_FOLDER_PATHS[0]}")
        return None, None

    # Copy HTML as "Daily Dashboard.html"
    dest_html = os.path.join(folder, DASHBOARD_FILENAME)
    shutil.copy2(html_path, dest_html)
    dashboard_url = f"{SHAREPOINT_BASE}/{DASHBOARD_FILENAME.replace(' ', '%20')}"
    print(f"  [OK] Dashboard published -> {dashboard_url}")

    # Copy snapshot image
    image_url = None
    if image_path and os.path.exists(image_path):
        dest_img = os.path.join(folder, SNAPSHOT_FILENAME)
        shutil.copy2(image_path, dest_img)
        image_url = f"{SHAREPOINT_BASE}/{SNAPSHOT_FILENAME.replace(' ', '%20')}"
        print(f"  [OK] Snapshot published  -> {image_url}")

    return dashboard_url, image_url


# ─────────────────────────────────────────────
#  NETLIFY DEPLOY  (browser-friendly hosting)
# ─────────────────────────────────────────────

def deploy_to_cloudflare_pages(html_path, api_token, account_id, project_name):
    """Deploy the dashboard HTML to Cloudflare Pages.

    Tries wrangler CLI first (most reliable), then falls back to the
    Direct Upload API if wrangler is not available.

    Requires:
        config.json -> cloudflare.api_token    (API token with Pages Write + User Details Read + Memberships Read)
        config.json -> cloudflare.account_id   (Cloudflare account ID)
        config.json -> cloudflare.project_name (Pages project name, e.g. dflector-dashboard)

    Returns the live site URL string, or None on failure.
    """
    import hashlib
    import subprocess
    import shutil
    import tempfile

    site_url = f"https://{project_name}.pages.dev"

    # --- Try wrangler CLI first (most reliable) ---
    # On Windows, prefer .cmd over .ps1 so subprocess can run it without execution policy issues
    _npm_global = os.path.expandvars(r"%APPDATA%\npm")
    wrangler_candidates = [
        os.path.join(_npm_global, "wrangler.cmd"),                                                  # global npm (Windows)
        os.path.join(_npm_global, "wrangler"),                                                       # global npm (Unix-like)
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "node_modules", ".bin", "wrangler.cmd"),  # local (Windows)
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "node_modules", ".bin", "wrangler"),      # local (Unix)
        shutil.which("wrangler.cmd") or "",
        shutil.which("wrangler") or "",
    ]
    wrangler_bin = next((p for p in wrangler_candidates if p and os.path.isfile(p)), None)
    if wrangler_bin:
        deploy_dir = tempfile.mkdtemp(prefix="cf_deploy_")
        try:
            shutil.copy(html_path, os.path.join(deploy_dir, "index.html"))
            env = os.environ.copy()
            env["CLOUDFLARE_API_TOKEN"] = api_token
            res = subprocess.run(
                [wrangler_bin, "pages", "deploy", deploy_dir,
                 "--project-name", project_name, "--commit-dirty=true"],
                capture_output=True, text=True, timeout=120, env=env
            )
            if res.returncode == 0:
                print(f"  [OK] Cloudflare Pages deploy complete (wrangler) -> {site_url}")
                return site_url
            print(f"  [WARN] wrangler deploy failed: {(res.stdout + res.stderr)[:300]}")
        finally:
            shutil.rmtree(deploy_dir, ignore_errors=True)
    else:
        print(f"  [INFO] wrangler not found locally -- trying Direct Upload API")

    # --- Fallback: Direct Upload API ---
    with open(html_path, "rb") as f:
        content = f.read()

    file_hash = hashlib.sha256(content).hexdigest()
    manifest = json.dumps({"index.html": file_hash})
    base_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/pages/projects/{project_name}"
    headers = {"Authorization": f"Bearer {api_token}"}

    # Step 1: POST manifest + file content as multipart
    files = [
        ("manifest", (None, manifest, "application/json")),
        (file_hash, (None, content, "application/octet-stream")),
    ]
    r = requests.post(f"{base_url}/deployments", headers=headers, files=files, timeout=120)

    if r.status_code in (200, 201):
        data = r.json()
        if data.get("success"):
            # Verify the deployment is actually live (not a false-positive success)
            result = data.get("result") or {}
            dep_url = result.get("url", "")
            if dep_url:
                import time
                time.sleep(3)
                try:
                    check = requests.get(dep_url, timeout=10)
                    if check.status_code == 200:
                        print(f"  [OK] Cloudflare Pages deploy complete -> {site_url}")
                        return site_url
                    print(f"  [WARN] Deploy reported success but preview URL returned {check.status_code}")
                except Exception:
                    pass
            print(f"  [OK] Cloudflare Pages deploy complete -> {site_url}")
            return site_url
        # Handle two-step: missing hashes + JWT
        result = data.get("result") or {}
        jwt = result.get("jwt")
        missing = result.get("missing", [])
        if jwt and missing:
            up_r = requests.post(
                "https://api.cloudflare.com/client/v4/pages/assets/upload",
                headers={**headers, "Authorization": f"Bearer {jwt}"},
                json=[{"key": file_hash, "value": content.decode("utf-8", errors="replace")}],
                timeout=120,
            )
            if up_r.status_code in (200, 201):
                r2 = requests.post(f"{base_url}/deployments", headers=headers, files=files, timeout=120)
                if r2.status_code in (200, 201) and r2.json().get("success"):
                    print(f"  [OK] Cloudflare Pages deploy complete -> {site_url}")
                    return site_url

    print(f"  [WARN] Cloudflare Pages deploy failed: HTTP {r.status_code} -- {r.text[:300]}")
    return None


def deploy_to_netlify(html_path, access_token, site_id):
    """Deploy the dashboard HTML to Netlify as index.html.
    This gives a persistent public URL (e.g. https://d-flector-dashboard.netlify.app)
    that any team member can open directly in a browser -- no SharePoint restrictions.

    Requires:
        config.json -> netlify.access_token  (Personal Access Token from netlify.com)
        config.json -> netlify.site_id       (Site ID from Site Settings on netlify.com)

    Returns the live site URL string, or None on failure.
    """
    import io
    import zipfile

    # Build an in-memory ZIP containing index.html + _headers (forces text/html MIME type)
    headers_content = "/\n  Content-Type: text/html; charset=UTF-8\n\n/index.html\n  Content-Type: text/html; charset=UTF-8\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(html_path, "index.html")
        zf.writestr("_headers", headers_content)
    buf.seek(0)
    zip_bytes = buf.getvalue()

    url = f"https://api.netlify.com/api/v1/sites/{site_id}/deploys"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/zip",
    }

    r = requests.post(url, headers=headers, data=zip_bytes, timeout=60)
    if r.status_code in (200, 201):
        data = r.json()
        site_url = data.get("ssl_url") or data.get("url") or f"https://{site_id}.netlify.app"
        print(f"  [OK] Netlify deploy complete -> {site_url}")
        return site_url
    else:
        print(f"  [WARN] Netlify deploy failed: HTTP {r.status_code}")
        print(f"         {r.text[:200]}")
        return None


# ─────────────────────────────────────────────
#  TEAMS WEBHOOK
# ─────────────────────────────────────────────

def post_teams_webhook(webhook_url, trade, stores_data, now, targets,
                       dashboard_url, image_url=None):
    """Post a rich Adaptive Card to a Teams incoming webhook.
    Includes D-Flector MTD + Combined Retail hero metrics, store breakdown,
    an optional snapshot image, and a button to open the full dashboard."""

    total_retail        = sum(s.get("achieved", 0) for s in stores_data)
    total_retail_target = sum(s.get("target",   0) for s in stores_data)
    retail_pct  = total_retail / total_retail_target if total_retail_target else 0
    trade_target = targets.get("dflector_trade", 250000)
    trade_pct   = trade["total"] / trade_target if trade_target else 0

    def fmt(v): return f"${v:,.0f}"

    # ── Card body ──────────────────────────────────────────────────────────
    body = []

    # Optional hero snapshot image at the top
    if image_url:
        body.append({
            "type": "Image",
            "url": image_url,
            "size": "Stretch",
            "altText": "Daily Dashboard Snapshot",
        })

    # Header row
    body.append({
        "type": "ColumnSet",
        "columns": [
            {
                "type": "Column", "width": "stretch",
                "items": [{
                    "type": "TextBlock",
                    "text": f"📊  Daily Dashboard -- {now.strftime('%d %b %Y')}",
                    "weight": "Bolder", "size": "Large", "color": "Accent",
                }],
            },
            {
                "type": "Column", "width": "auto",
                "items": [{
                    "type": "TextBlock",
                    "text": now.strftime("%I:%M %p"),
                    "color": "Good", "size": "Small", "horizontalAlignment": "Right",
                }],
            },
        ],
    })

    # D-Flector + Retail side-by-side
    body.append({
        "type": "ColumnSet",
        "columns": [
            {
                "type": "Column", "width": "stretch", "style": "emphasis",
                "items": [
                    {"type": "TextBlock", "text": "🏭  D-Flector Trade (MTD)",
                     "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": fmt(trade["completed"]),
                     "size": "ExtraLarge", "weight": "Bolder", "color": "Good",
                     "spacing": "None"},
                    {"type": "TextBlock",
                     "text": f"Completed  •  {trade_pct:.0%} of {fmt(trade_target)} target",
                     "color": "Good", "size": "Small", "spacing": "None"},
                    {"type": "TextBlock", "text": f"Open: {fmt(trade['open'])}",
                     "size": "Small", "spacing": "Small"},
                    {"type": "TextBlock", "text": f"Total Pipeline: {fmt(trade['total'])}",
                     "size": "Small", "spacing": "None"},
                ],
            },
            {
                "type": "Column", "width": "stretch", "style": "accent",
                "items": [
                    {"type": "TextBlock", "text": "🛒  Combined Retail",
                     "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": fmt(total_retail),
                     "size": "ExtraLarge", "weight": "Bolder", "color": "Accent",
                     "spacing": "None"},
                    {"type": "TextBlock",
                     "text": f"31 days to T-1  •  {retail_pct:.0%} of {fmt(total_retail_target)} target",
                     "color": "Accent", "size": "Small", "spacing": "None"},
                    *[
                        {"type": "TextBlock",
                         "text": f"{s['name']}: {fmt(s['achieved'])}",
                         "size": "Small", "spacing": "None"}
                        for s in stores_data
                    ],
                ],
            },
        ],
    })

    # Separator
    body.append({"type": "TextBlock", "text": " ", "spacing": "Small"})

    card_payload = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": body,
                "actions": [{
                    "type": "Action.OpenUrl",
                    "title": "📂  Open Full Dashboard",
                    "url": dashboard_url or "about:blank",
                }],
            },
        }],
    }

    r = requests.post(webhook_url, json=card_payload, timeout=30)
    r.raise_for_status()
    print(f"  [OK] Posted to Teams webhook")


# ─────────────────────────────────────────────
#  DASHBOARD HTML INJECTION
# ─────────────────────────────────────────────

def inject_data_into_html(html_path, dashboard_data):
    """Replace the DASHBOARD_DATA object in the HTML file with fresh data."""
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    # ensure_ascii=False keeps em-dashes etc as real UTF-8 characters,
    # avoiding \uXXXX sequences that confuse re.sub's replacement string parser.
    json_str = json.dumps(dashboard_data, indent=2, default=str, ensure_ascii=False)
    pattern = r"const DASHBOARD_DATA = \{.*?\};"
    # Use a lambda so re.sub never interprets the replacement as a regex template
    replacement = f"const DASHBOARD_DATA = {json_str};"
    new_html = re.sub(pattern, lambda _: replacement, html, flags=re.DOTALL)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(new_html)

    print(f"[OK] Dashboard updated: {html_path}")


# ─────────────────────────────────────────────
#  EBAY SELL ANALYTICS API HELPER
# ─────────────────────────────────────────────

def _fetch_ebay_sell_analytics(app_id: str, cert_id: str, refresh_token: str,
                                marketplace_id: str = "EBAY_AU") -> dict:
    """Exchange an OAuth refresh token for an access token, then call the eBay
    Sell Analytics API to fetch live seller standards (defect rate, late shipment,
    INAD, INR) and return them as a dict compatible with service_metrics_override.

    Falls back gracefully on any error — always returns a (possibly empty) dict.
    """
    # ── 1. Exchange refresh token for access token ────────────────────────
    try:
        creds_b64 = base64.b64encode(f"{app_id}:{cert_id}".encode()).decode()
        r = requests.post(
            "https://api.ebay.com/identity/v1/oauth2/token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {creds_b64}",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "scope": (
                    "https://api.ebay.com/oauth/api_scope/sell.analytics.readonly "
                    "https://api.ebay.com/oauth/api_scope/sell.account.readonly"
                ),
            },
            timeout=30,
        )
        r.raise_for_status()
        token_data = r.json()
        access_token = token_data.get("access_token") or token_data.get("token")
        if not access_token:
            print(f"      [Analytics] Token exchange returned no access_token: {token_data}")
            return {}
        print(f"      [Analytics] OAuth token obtained (expires in {token_data.get('expires_in', '?')}s)")
    except Exception as e:
        print(f"      [Analytics] Token exchange failed: {e}")
        return {}

    auth_headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    base_url = "https://api.ebay.com"
    result: dict = {}

    # ── 2. Seller Standards Profile (defect rate, late ship, cases, seller level) ──
    try:
        r = requests.get(
            f"{base_url}/sell/analytics/v1/seller_standards_profile",
            headers=auth_headers,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        # The API returns a list of program objects — find EBAY_AU or the default
        programs = data.get("standardsProfiles") or data.get("programs") or []
        default_prog = None
        for prog in programs:
            if prog.get("evaluationMarketplaceId") == marketplace_id or prog.get("program") == marketplace_id:
                default_prog = prog
                break
        if default_prog is None and programs:
            default_prog = programs[0]
        if default_prog is None:
            default_prog = data.get("defaultProgram") or data

        level_map = {
            "TOP_RATED": "Top Rated",
            "ABOVE_STANDARD": "Above Standard",
            "STANDARD": "Standard",
            "BELOW_STANDARD": "Below Standard",
        }
        raw_level = default_prog.get("standardsLevel") or default_prog.get("sellerStandardsLevel", "")
        if raw_level:
            result["seller_level"] = level_map.get(raw_level, raw_level.replace("_", " ").title())
            result["seller_level_projected"] = result["seller_level"]

        cycle = default_prog.get("cycle") or {}
        if cycle.get("evaluationDate"):
            result["next_evaluation"] = cycle["evaluationDate"][:10]

        for m in default_prog.get("metrics", []):
            name = (m.get("name") or "").upper()
            try:
                val = float(str(m.get("value", "0")).rstrip("%"))
            except (ValueError, TypeError):
                val = 0.0

            if "DEFECT" in name:
                result["defect_rate"] = val
                for t in m.get("thresholds", []):
                    try:
                        result.setdefault("defect_threshold_top",
                                          float(str(t.get("upperThreshold") or t.get("value", 0.5)).rstrip("%")))
                    except Exception:
                        pass
            elif "LATE_SHIPMENT" in name or "LATE_SHIP" in name:
                result["late_ship_rate"] = val
            elif "CASE" in name or "BUYER_RESOLUTION" in name:
                result["cases_rate"] = val

        print(f"      [Analytics] Standards: level={result.get('seller_level')} "
              f"defect={result.get('defect_rate')}% "
              f"late={result.get('late_ship_rate')}% "
              f"cases={result.get('cases_rate')}%")
    except Exception as e:
        print(f"      [Analytics] Standards API error: {e}")

    # ── 3. INAD (Item Not As Described) ──────────────────────────────────
    try:
        r = requests.get(
            f"{base_url}/sell/analytics/v1/customer_service_metric/summary",
            headers=auth_headers,
            params={
                "customer_service_metric_type": "ITEM_NOT_AS_DESCRIBED",
                "evaluation_marketplace_id": marketplace_id,
                "evaluation_type": "CURRENT",
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        def _pct(v):
            try:
                return round(float(str(v).rstrip("% ")), 2)
            except Exception:
                return 0.0
        if data.get("metricPercent") is not None:
            result["inad_rate"] = _pct(data["metricPercent"])
        if data.get("metricBenchmarkPercent") is not None:
            result["inad_peer_rate"] = _pct(data["metricBenchmarkPercent"])
        inad_rate = result.get("inad_rate", 0)
        inad_peer = result.get("inad_peer_rate", 0)
        if inad_peer > 0:
            if inad_rate >= inad_peer * 2:
                result["inad_rating"] = "Very High"
            elif inad_rate >= inad_peer * 1.25:
                result["inad_rating"] = "High"
            elif inad_rate <= inad_peer * 0.75:
                result["inad_rating"] = "Low"
            else:
                result["inad_rating"] = "Average"
        print(f"      [Analytics] INAD: {result.get('inad_rate')}% "
              f"(peer avg {result.get('inad_peer_rate')}%, rating={result.get('inad_rating')})")
    except Exception as e:
        print(f"      [Analytics] INAD API error: {e}")

    # ── 4. INR (Item Not Received) ────────────────────────────────────────
    try:
        r = requests.get(
            f"{base_url}/sell/analytics/v1/customer_service_metric/summary",
            headers=auth_headers,
            params={
                "customer_service_metric_type": "ITEM_NOT_RECEIVED",
                "evaluation_marketplace_id": marketplace_id,
                "evaluation_type": "CURRENT",
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        def _pct(v):
            try:
                return round(float(str(v).rstrip("% ")), 2)
            except Exception:
                return 0.0
        if data.get("metricPercent") is not None:
            result["inr_rate"] = _pct(data["metricPercent"])
        if data.get("metricBenchmarkPercent") is not None:
            result["inr_peer_rate"] = _pct(data["metricBenchmarkPercent"])
        inr_rate = result.get("inr_rate", 0)
        inr_peer = result.get("inr_peer_rate", 0)
        if inr_peer > 0:
            if inr_rate >= inr_peer * 2:
                result["inr_rating"] = "Very High"
            elif inr_rate >= inr_peer * 1.25:
                result["inr_rating"] = "High"
            elif inr_rate <= inr_peer * 0.75:
                result["inr_rating"] = "Low"
            else:
                result["inr_rating"] = "Average"
        print(f"      [Analytics] INR: {result.get('inr_rate')}% "
              f"(peer avg {result.get('inr_peer_rate')}%, rating={result.get('inr_rating')})")
    except Exception as e:
        print(f"      [Analytics] INR API error: {e}")

    return result


# ─────────────────────────────────────────────
#  MAIN PIPELINE
# ─────────────────────────────────────────────

def run_pipeline(config_path, dry_run=False):
    cfg = load_config(config_path)
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    month_start = now.strftime("%Y-%m-01")
    # Retail stores (Zivor, AMS, ATS, TradeMe) use last 31 days ending T-1 (yesterday)
    # T-1 avoids partial-day data from today skewing the rolling 31-day window
    retail_end = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    retail_start = (now - timedelta(days=32)).strftime("%Y-%m-%d")
    targets = cfg.get("targets", {})

    print(f"[INFO] Running dashboard pipeline -- {now.strftime('%d %b %Y, %I:%M %p')}")
    print(f"[INFO] D-Flector date range: {month_start} to {today} (MTD + prior open orders)")
    print(f"[INFO] Retail date range: {retail_start} to {retail_end} (last 31 days, ends T-1)")

    stores_data = []
    service_metrics = []

    # ── 1. Unleashed (D-Flector Trade) ──
    print("\n[1/4] Fetching Unleashed trade data (D-Flector)...")
    ucfg = cfg["unleashed"]
    unleashed = UnleashedClient(ucfg["api_id"], ucfg["api_key"], ucfg.get("base_url", ""))

    trade = unleashed.get_monthly_trade_data(now.year, now.month, include_prev_open=True)
    print(f"  Completed: AUD ${trade['completed']:,.2f} | Open: AUD ${trade['open']:,.2f} | Total: AUD ${trade['total']:,.2f}")

    # Monthly history (last 10 months)
    monthly_labels = []
    monthly_completed = []
    for i in range(9, -1, -1):
        d = now - timedelta(days=30 * i)
        label = d.strftime("%b %y")
        monthly_labels.append(label)
        if i == 0:
            monthly_completed.append(trade["completed"])
        else:
            try:
                m_data = unleashed.get_monthly_trade_data(d.year, d.month)
                monthly_completed.append(m_data["completed"])
            except Exception as _me:
                print(f"  [WARN] Unleashed monthly data failed for {label}: {_me}")
                monthly_completed.append(0)
            time.sleep(0.5)

    # ── 2. Neto (Zivor) ──
    print("\n[2/4] Fetching Neto data (Zivor eBay + Web)...")
    try:
        ncfg = cfg["neto"]
        neto = NetoClient(ncfg["store_url"], ncfg["api_key"], ncfg["username"])
        zivor_stores = neto.get_store_data(ncfg, targets, retail_start, retail_end)
        stores_data.extend(zivor_stores)
        for s in zivor_stores:
            print(f"  {s['name']}: ${s['achieved']:,.2f}")
    except Exception as e:
        print(f"  [ERROR] Neto failed: {e}")
        # Add placeholder entries so the dashboard still renders
        stores_data.extend([
            {"name": "Zivor (eBay)", "target": targets.get("zivor_ebay", 150000), "achieved": 0,
             "rating": ncfg.get("ebay_rating", 0.997), "listings": ncfg.get("ebay_listings", 0),
             "trend7d": 0, "source": "neto"},
            {"name": "Zivor (Web)", "target": targets.get("zivor_web", 20000), "achieved": 0,
             "rating": ncfg.get("web_rating", 4.6), "listings": ncfg.get("web_listings", 0),
             "trend7d": 0, "source": "neto"},
        ])

    # ── 3. Shopify (AMS + ATS) ──
    print("\n[3/4] Fetching Shopify data (AMS + ATS)...")
    for brand_key, brand_name in [("ams", "AMS"), ("ats", "ATS")]:
        scfg = cfg.get("shopify", {}).get(brand_key, {})
        if not scfg.get("shop_url") or "YOUR_" in scfg.get("access_token", "YOUR_"):
            print(f"  [SKIP] {brand_name} -- not configured")
            continue
        try:
            shopify = ShopifyClient(scfg["shop_url"], scfg["access_token"], scfg.get("api_key"))
            brand_stores = shopify.get_store_data(brand_name, targets, retail_start, retail_end, scfg)
            stores_data.extend(brand_stores)
            for s in brand_stores:
                print(f"  {s['name']}: ${s['achieved']:,.2f}")
        except requests.exceptions.HTTPError as e:
            print(f"  [ERROR] {brand_name} Shopify failed ({e.response.status_code}): {e}")
            if e.response.status_code == 401:
                print(f"  [HINT] The token for {brand_name} may be a Private App password.")
                print(f"         Add 'api_key' to config.json under shopify.{brand_key},")
                print(f"         OR create a Custom App in Shopify admin to get a shpat_ token.")
            # Add placeholders
            ebay_key = f"{brand_name.lower()}_ebay"
            web_key = f"{brand_name.lower()}_web"
            stores_data.extend([
                {"name": f"{brand_name} (eBay)", "target": targets.get(ebay_key, 15000), "achieved": 0,
                 "rating": scfg.get("ebay_rating", 1.0), "listings": scfg.get("ebay_listings", 0),
                 "trend7d": 0, "source": "shopify"},
                {"name": f"{brand_name} (Web)", "target": targets.get(web_key, 5000), "achieved": 0,
                 "rating": scfg.get("web_rating", 5.0), "listings": 0,
                 "trend7d": 0, "source": "shopify"},
            ])
        except Exception as e:
            print(f"  [ERROR] {brand_name} Shopify failed: {e}")
        time.sleep(0.3)

    # ── 4. TradeMe ──
    print("\n[4/4] Fetching TradeMe data (Zivor NZ)...")
    tcfg = cfg.get("trademe", {})
    if tcfg.get("consumer_key") and "NEEDS_SETUP" not in tcfg.get("oauth_token", "NEEDS_SETUP"):
        try:
            trademe = TradeMeClient(
                tcfg["consumer_key"], tcfg["consumer_secret"],
                tcfg["oauth_token"], tcfg["oauth_token_secret"],
            )
            tm_data = trademe.get_store_data(targets, retail_start, retail_end, tcfg)
            stores_data.append(tm_data)
            print(f"  TradeMe (Zivor): ${tm_data['achieved']:,.2f}")
        except Exception as e:
            print(f"  [ERROR] TradeMe failed: {e}")
    else:
        print("  [SKIP] TradeMe -- OAuth tokens not yet configured")

    # ── 4.5. eBay live metrics (feedback score, listings) ─────────────────
    ebay_live = {}  # Will hold metrics keyed by store: {"zivor": {...}, "ams": {...}, "ats": {...}}
    ecfg = cfg.get("ebay", {})
    stores_cfg = ecfg.get("stores", {})
    _has_ebay = (
        _EBAY_CLIENT_AVAILABLE
        and stores_cfg
        and not ecfg.get("sandbox_mode", False)
    )
    if _has_ebay:
        print("\n[4.5] Fetching eBay live metrics (feedback, listings, performance)...")
        listings_cache_dict = ecfg.get("listings_cache", {})

        for store_key in ["zivor", "ams", "ats"]:
            store_cfg = stores_cfg.get(store_key, {})
            if not store_cfg.get("access_token"):
                continue

            try:
                print(f"  [{store_key.upper()}] Fetching metrics...")
                ebay_cl = eBayClient(
                    app_id=ecfg["app_id"],
                    cert_id=ecfg["cert_id"],
                    access_token=store_cfg["access_token"],
                    sandbox_mode=False,
                )
                # Get listings cache for this store
                listings_cache = listings_cache_dict.get(store_key, 0) if isinstance(listings_cache_dict, dict) else 0
                store_sm = cfg.get("service_metrics_override", {}).get(store_key, {})

                # [4.5b] Live seller standards via Sell Analytics API (OAuth refresh token)
                refresh_env = f"EBAY_OAUTH_REFRESH_{store_key.upper()}"
                refresh_token = os.environ.get(refresh_env, "")
                if refresh_token:
                    print(f"    [4.5b] eBay Analytics API ({store_key.upper()})...")
                    live_analytics = _fetch_ebay_sell_analytics(
                        app_id=ecfg["app_id"],
                        cert_id=ecfg["cert_id"],
                        refresh_token=refresh_token,
                    )
                    if live_analytics:
                        store_sm = {**store_sm, **live_analytics}
                        print(f"    [{store_key.upper()}] Live standards merged OK")
                    else:
                        print(f"    [{store_key.upper()}] Analytics API returned no data — using config.json fallback")
                else:
                    print(f"    [WARN] {refresh_env} not set — seller standards from config.json (static)")

                # Only attempt live scrape for Zivor (single browser session)
                if store_key == "zivor":
                    print("    Attempting live Seller Hub scrape (browser_cookie3)...")
                    live_scraped = ebay_cl.fetch_performance_metrics_with_cookies()
                    if live_scraped:
                        merged_sm = {**store_sm, **live_scraped}
                        cfg.setdefault("service_metrics_override", {})[store_key] = merged_sm
                        try:
                            import json as _json
                            cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
                            with open(cfg_path, "w", encoding="utf-8") as _f:
                                _json.dump(cfg, _f, indent=2, ensure_ascii=False)
                            print("    Live metrics cached to config.json")
                        except Exception as _ce:
                            print(f"    [WARN] Could not update config.json: {_ce}")
                        store_sm = merged_sm
                    else:
                        print("    Live scrape skipped -- using config.json fallback values")

                raw = ebay_cl.get_all_metrics(
                    listings_cache=listings_cache,
                    service_metrics=store_sm,
                )
                metrics = format_ebay_metrics_for_dashboard(raw)
                ebay_live[store_key] = metrics

                print(f"    Feedback:    {metrics['feedback_score']:,}  ({metrics['feedback_percent']}%)")
                print(f"    Listings:    {metrics['listings']:,}  {'(from cache)' if listings_cache else '(API)'}")
                print(f"    Seller Level: {metrics['seller_level']} -> projected {metrics['seller_level_projected']}")
                print(f"    Defect: {metrics['defect_rate']}%  |  Late Ship: {metrics['late_ship_rate']}%  |  Cases: {metrics['cases_rate']}%")
                print(f"    INAD: {metrics['inad_rate']}% ({metrics['inad_rating']})  |  INR: {metrics['inr_rate']}% ({metrics['inr_rating']})")

                # Patch the store card with live rating + listing count
                store_display_name = {
                    "zivor": "Zivor (eBay)",
                    "ams": "AMS (eBay)",
                    "ats": "ATS (eBay)",
                }.get(store_key)

                for s in stores_data:
                    if s["name"] == store_display_name:
                        s["rating"]   = metrics["rating"]
                        s["listings"] = metrics["listings"]
                        break

            except Exception as e:
                print(f"    [WARN] {store_key.upper()} metrics failed: {e}")
    else:
        print("\n[4.5] eBay live metrics -- skipped (no stores configured)")

    # ── Build service metrics from eBay-channel stores ──
    smo = cfg.get("service_metrics_override", {})
    for s in stores_data:
        if "(eBay)" in s["name"]:
            brand = s["name"].split(" (")[0]
            brand_key = brand.lower()
            override = smo.get(brand_key, {})
            entry = {
                "store":  brand,
                # Legacy flat fields (backward compat)
                "inad":   override.get("inad_count", override.get("inad", 0)),
                "inr":    override.get("inr_count",  override.get("inr",  0)),
                "orders": override.get("orders", override.get("defect_total", 0)),
            }
            # Enrich all eBay stores (Zivor, AMS, ATS) with full live metrics if available
            if ebay_live and brand_key in ebay_live:
                store_metrics = ebay_live[brand_key]
                entry.update({
                    "feedback_score":           store_metrics["feedback_score"],
                    "feedback_percent":          store_metrics["feedback_percent"],
                    "seller_level":              store_metrics["seller_level"],
                    "seller_level_projected":    store_metrics["seller_level_projected"],
                    "next_evaluation":           store_metrics["next_evaluation"],
                    # Defect
                    "defect_rate":               store_metrics["defect_rate"],
                    "defect_count":              store_metrics["defect_count"],
                    "defect_total":              store_metrics["defect_total"],
                    "defect_threshold_top":      store_metrics["defect_threshold_top"],
                    "defect_period":             store_metrics["defect_period"],
                    # Late shipment
                    "late_ship_rate":            store_metrics["late_ship_rate"],
                    "late_ship_count":           store_metrics["late_ship_count"],
                    "late_ship_total":           store_metrics["late_ship_total"],
                    "late_ship_threshold_top":   store_metrics["late_ship_threshold_top"],
                    "late_ship_period":          store_metrics["late_ship_period"],
                    # Cases
                    "cases_rate":                store_metrics["cases_rate"],
                    "cases_count":               store_metrics["cases_count"],
                    "cases_total":               store_metrics["cases_total"],
                    "cases_threshold_top":       store_metrics["cases_threshold_top"],
                    # INAD
                    "inad_rate":                 store_metrics["inad_rate"],
                    "inad_count":                store_metrics["inad_count"],
                    "inad_total":                store_metrics["inad_total"],
                    "inad_peer_rate":             store_metrics["inad_peer_rate"],
                    "inad_rating":               store_metrics["inad_rating"],
                    "inad_period":               store_metrics["inad_period"],
                    # INR
                    "inr_rate":                  store_metrics["inr_rate"],
                    "inr_count":                 store_metrics["inr_count"],
                    "inr_total":                 store_metrics["inr_total"],
                    "inr_peer_rate":              store_metrics["inr_peer_rate"],
                    "inr_rating":                store_metrics["inr_rating"],
                    "inr_period":                store_metrics["inr_period"],
                })
            service_metrics.append(entry)

    # ── Build dashboard data object ──
    cat_labels = list(trade["categories_total"].keys())

    # Previous month completed (second-to-last entry in monthly history)
    prev_month_completed = monthly_completed[-2] if len(monthly_completed) >= 2 else 0
    prev_month_label = monthly_labels[-2] if len(monthly_labels) >= 2 else "Prev Month"

    # 7-day daily retail totals (T-7 to T-1) for Combined Retail mini chart
    # Broken down by source so the chart can show a stacked Neto / Shopify / TradeMe view
    seven_day_labels = []
    seven_day_totals = []
    seven_day_neto    = []
    seven_day_shopify = []
    seven_day_trademe = []
    for i in range(6, -1, -1):
        d = (now - timedelta(days=i + 1)).date()
        date_str = d.strftime("%Y-%m-%d")
        label = d.strftime("%a %d")
        seven_day_labels.append(label)
        neto_day    = sum(s.get("daily7d", {}).get(date_str, 0) for s in stores_data if s.get("source") == "neto")
        shopify_day = sum(s.get("daily7d", {}).get(date_str, 0) for s in stores_data if s.get("source") == "shopify")
        trademe_day = sum(s.get("daily7d", {}).get(date_str, 0) for s in stores_data if s.get("source") == "trademe")
        seven_day_neto.append(round(neto_day, 2))
        seven_day_shopify.append(round(shopify_day, 2))
        seven_day_trademe.append(round(trademe_day, 2))
        seven_day_totals.append(round(neto_day + shopify_day + trademe_day, 2))

    dashboard_data = {
        "lastUpdated": now.strftime("%d %b %Y, %I:%M %p"),
        "currentMonth": now.strftime("%B %Y"),
        "currency": "AUD",
        "retailDateRange": f"{retail_start} to {retail_end} (Last 31 days, ends T-1)",
        "tradeDateRange": f"{month_start} to {today} (MTD + prior open)",
        "trade": {
            "target": targets.get("dflector_trade", 250000),
            "completedMTD": trade["completed"],
            "openMTD": trade["open"],
            "totalMTD": trade["total"],
            "achievedPct": round(trade["total"] / max(targets.get("dflector_trade", 250000), 1), 3),
            "prevMonthCompleted": prev_month_completed,
            "prevMonthLabel": prev_month_label,
            "monthlyHistory": {
                "labels": monthly_labels,
                "completed": monthly_completed,
            },
        },
        "retail7d": {
            "labels": seven_day_labels,
            "totals": seven_day_totals,
            "neto":    seven_day_neto,
            "shopify": seven_day_shopify,
            "trademe": seven_day_trademe,
        },
        "categories": {
            "labels": cat_labels,
            "completed": [trade["categories_completed"].get(c, 0) for c in cat_labels],
            "open": [trade["categories_open"].get(c, 0) for c in cat_labels],
            "total": [trade["categories_total"].get(c, 0) for c in cat_labels],
        },
        "stores": [{
            "name": s["name"], "target": s["target"], "achieved": s["achieved"],
            "rating": s["rating"], "listings": s["listings"],
            "trend7d": s["trend7d"], "source": s["source"],
        } for s in stores_data],
        "serviceMetrics": service_metrics,
        "suppliers": cfg.get("suppliers_static", []),
        "weeklySales": {"labels": [], "series": {}},
    }

    if dry_run:
        print("\n[DRY RUN] Dashboard data:")
        print(json.dumps(dashboard_data, indent=2, default=str))
        return

    html_path = os.path.join(os.path.dirname(config_path), cfg.get("dashboard_path", "dashboard.html"))
    inject_data_into_html(html_path, dashboard_data)

    # ── 5. Generate hero snapshot image ──────────────────────────────────
    print("\n[5/6] Publishing...")
    image_path = os.path.join(os.path.dirname(config_path), "dashboard_snapshot.png")
    generate_hero_image(trade, stores_data, now, targets, image_path)

    # ── 6a. Deploy to Cloudflare Pages (primary browser-friendly URL) ────────
    cf_url = None
    cf_cfg = cfg.get("cloudflare", {})
    cf_token   = cf_cfg.get("api_token", "")
    cf_account = cf_cfg.get("account_id", "")
    cf_project = cf_cfg.get("project_name", "")
    if cf_token and cf_account and cf_project:
        try:
            cf_url = deploy_to_cloudflare_pages(html_path, cf_token, cf_account, cf_project)
        except Exception as e:
            print(f"  [ERROR] Cloudflare Pages deploy failed: {e}")
    else:
        # Fall back to Netlify
        netlify_cfg = cfg.get("netlify", {})
        n_token = netlify_cfg.get("access_token", "")
        n_site  = netlify_cfg.get("site_id", "")
        if n_token and n_site and "PASTE_YOUR" not in n_token and "PASTE_YOUR" not in n_site:
            try:
                cf_url = deploy_to_netlify(html_path, n_token, n_site)
            except Exception as e:
                print(f"  [ERROR] Netlify deploy failed: {e}")
        else:
            print("  [SKIP] No cloud deploy configured -- add cloudflare section to config.json")

    # ── 6b. Copy to D-Flector Commercial SharePoint (backup / local access) ─
    dashboard_url, image_url = copy_to_onedrive(html_path, image_path)

    # Prefer Cloudflare Pages URL as the authoritative link (opens directly in browser)
    best_url = cf_url or dashboard_url

    # ── 7. Post to Teams webhook ──────────────────────────────────────────
    teams_cfg = cfg.get("teams", {})
    webhook_url = teams_cfg.get("webhook_url", "")
    if webhook_url and "PASTE_YOUR" not in webhook_url:
        try:
            post_teams_webhook(
                webhook_url, trade, stores_data, now, targets,
                dashboard_url=best_url or teams_cfg.get("dashboard_url", ""),
                image_url=image_url,
            )
        except Exception as e:
            print(f"  [ERROR] Teams webhook failed: {e}")
            print("          Check your webhook URL in config.json -> teams.webhook_url")
    else:
        print("  [SKIP] Teams webhook not configured -- add webhook_url to config.json -> teams")

    if cf_url:
        print(f"\n  *** Share this link with your team: {cf_url} ***")

    print(f"\n[DONE] Dashboard updated successfully at {now.strftime('%I:%M %p')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="D-Flector / Zivor Dashboard Pipeline")
    parser.add_argument("--config", default=os.path.join(os.path.dirname(__file__), "config.json"),
                        help="Path to config.json")
    parser.add_argument("--dry-run", action="store_true", help="Print data without updating HTML")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"ERROR: Config file not found: {args.config}")
        print("Copy config.example.json to config.json and fill in your API credentials.")
        sys.exit(1)

    try:
        run_pipeline(args.config, dry_run=args.dry_run)
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
