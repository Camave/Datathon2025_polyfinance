import time
import csv
import traceback
from typing import Optional, Dict

import pandas as pd
import yfinance as yf
import requests

OUTPUT_CSV = r"c:\poly\polyfinance\Datathon\sp500_yfinance.csv"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
REQUEST_SLEEP = 0.6  # seconds between requests to avoid rate limits

COLUMNS = [
    "Symbol",
    "Company_Name",
    "Sector",
    "Revenue",
    "Net_Income",
    "Total_Assets",
    "Shareholders_Equity",
    "Total_Debt",
    "Market_Cap",
    "Current_Assets",
    "Current_Liabilities",
    "Book_Value",
    "Shares_Outstanding",
    "Revenue_Previous_Year",
]


def get_sp500_tickers() -> pd.Series:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/115.0 Safari/537.36"
    }
    try:
        resp = requests.get(WIKI_URL, headers=headers, timeout=15)
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        df = tables[0]
        symbols = df["Symbol"].str.replace(r"\.", "-", regex=True).str.strip()
        return symbols
    except Exception as e:
        print(f"Failed to fetch S&P500 list: {e}")
        raise


def find_row_value(financials: pd.DataFrame, keywords: list) -> Optional[pd.Series]:
    if financials is None or financials.empty:
        return None
    idx_lower = [str(i).lower() for i in financials.index]
    for kw in keywords:
        for i, label in enumerate(idx_lower):
            if kw in label:
                return financials.iloc[i]
    return None


def get_total_revenue_previous(financials: pd.DataFrame) -> Optional[float]:
    # financials columns are periods (most recent first). We want the second column if present.
    try:
        row = find_row_value(financials, ["total revenue", "totalrevenue", "revenue"])
        if row is None:
            return None
        if len(row.values) >= 2:
            val = row.values[1]
            return float(val) if pd.notna(val) else None
        return None
    except Exception:
        return None


def safe_info_get(info: dict, *keys):
    for k in keys:
        if k in info and info[k] is not None:
            return info[k]
    return None


def fetch_data_for_symbol(symbol: str) -> Dict[str, Optional[object]]:
    out = {k: None for k in COLUMNS}
    out["Symbol"] = symbol
    try:
        t = yf.Ticker(symbol)
        
        # Get financial statements
        try:
            balance_sheet = t.balance_sheet
            income_stmt = t.income_stmt
        except:
            balance_sheet = None
            income_stmt = None
            
        info = {}
        try:
            info = t.info or {}
        except Exception:
            info = {}

        # Basic info
        out["Company_Name"] = safe_info_get(info, "longName", "shortName", "short_name")
        out["Sector"] = safe_info_get(info, "sector")
        out["Market_Cap"] = safe_info_get(info, "marketCap")
        out["Book_Value"] = safe_info_get(info, "bookValue")
        out["Shares_Outstanding"] = safe_info_get(info, "sharesOutstanding")
        
        # Balance sheet items
        if balance_sheet is not None and not balance_sheet.empty:
            latest = balance_sheet.iloc[:, 0]  # Most recent period
            out["Total_Assets"] = latest.get("Total Assets", None)
            out["Current_Assets"] = latest.get("Current Assets", None)
            out["Current_Liabilities"] = latest.get("Current Liabilities", None)
            out["Total_Debt"] = latest.get("Total Debt", None)
            out["Shareholders_Equity"] = latest.get("Stockholders Equity", None)

        # Income statement items
        if income_stmt is not None and not income_stmt.empty:
            latest = income_stmt.iloc[:, 0]  # Most recent period
            previous = income_stmt.iloc[:, 1] if income_stmt.shape[1] > 1 else None
            
            out["Revenue"] = latest.get("Total Revenue", None)
            out["Net_Income"] = latest.get("Net Income", None)
            
            if previous is not None:
                out["Revenue_Previous_Year"] = previous.get("Total Revenue", None)

        # Fallback to info if statements didn't work
        if out["Total_Assets"] is None:
            out["Total_Assets"] = safe_info_get(info, "totalAssets")
        if out["Current_Assets"] is None:
            out["Current_Assets"] = safe_info_get(info, "currentAssets")
        if out["Current_Liabilities"] is None:
            out["Current_Liabilities"] = safe_info_get(info, "currentLiabilities")
        if out["Shareholders_Equity"] is None:
            out["Shareholders_Equity"] = safe_info_get(
                info, "totalStockholderEquity", "totalStockholdersEquity"
            )
        if out["Revenue"] is None:
            out["Revenue"] = safe_info_get(info, "totalRevenue")
        if out["Net_Income"] is None:
            out["Net_Income"] = safe_info_get(info, "netIncome")

    except Exception:
        traceback.print_exc()
    
    return out


def main():
    tickers = get_sp500_tickers()
    rows = []
    total = len(tickers)
    print(f"Fetching all {total} S&P 500 companies...")

    for i, sym in enumerate(tickers, start=1):
        print(f"[{i}/{total}] {sym} ...", end=" ", flush=True)
        data = fetch_data_for_symbol(sym)
        rows.append(data)
        print("ok")
        time.sleep(REQUEST_SLEEP)

    # Write CSV 
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"Done. CSV written to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()