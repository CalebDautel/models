

import os
import re
import time
import datetime
import requests
import pandas as pd
from dateutil import parser as dateparser
from bs4 import BeautifulSoup
from sec_edgar_api import EdgarClient

###############################################################################
# Configuration
###############################################################################
TICKER = "EA"
CIK = "0000712515"
USER_AGENT = "Mozilla/5.0 (compatible; MyCorp EDGAR Scraper/1.0; +http://mycorp.com; contact@mycorp.com)"
FILING_COUNT = 5  # Last 5 filings (10-K and 10-Q)
TARGET_PHRASE = "balance sheets"  # We'll look for <ShortName> containing this
OUTPUT_FOLDER = r"C:\Users\cd22234\Downloads"
OUTPUT_FILENAME = "EA_Last5Filings_BalanceSheets_withMaster.xlsx"

###############################################################################
# Step 1: Retrieve Last 5 10-K or 10-Q
###############################################################################
def get_last_filings_10k_10q(cik: str, user_agent: str, count: int = 5):
    """
    Returns a list of the last `count` filings (10-K or 10-Q) for the given CIK.
    Each item is a dict: {"form_type", "accession_number", "filing_date"}.
    """
    edgar = EdgarClient(user_agent=user_agent)
    data = edgar.get_submissions(cik=cik)
    forms = data["filings"]["recent"]["form"]
    acc_nums = data["filings"]["recent"]["accessionNumber"]
    filing_dates = data["filings"]["recent"]["filingDate"]
    results = []
    for form, acc, fdate in zip(forms, acc_nums, filing_dates):
        form_upper = form.strip().upper()
        if form_upper in ("10-K", "10-Q"):
            results.append({
                "form_type": form_upper,
                "accession_number": acc,
                "filing_date": fdate  # in YYYY-MM-DD
            })
            if len(results) >= count:
                break
    return results

def remove_dashes(accession_number: str) -> str:
    return accession_number.replace("-", "")

def fetch_index_json(cik: str, accession_number: str, user_agent: str) -> dict:
    """
    Downloads index.json for a filing.
    """
    no_dash_acc = remove_dashes(accession_number)
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{no_dash_acc}/index.json"
    resp = requests.get(url, headers={"User-Agent": user_agent})
    if resp.status_code == 200:
        return resp.json()
    return {}

def find_filing_summary_url(index_json: dict) -> str:
    """
    From index.json, locate FilingSummary.xml.
    """
    base_dir = index_json.get("directory", {}).get("name", "")
    items = index_json.get("directory", {}).get("item", [])
    if not base_dir or not items:
        return None
    for item in items:
        if item.get("name") == "FilingSummary.xml":
            return f"https://www.sec.gov/{base_dir}/{item['name']}"
    return None

def find_statement_url_for_balance_sheets(summary_url: str, user_agent: str, target_phrase: str) -> str:
    """
    Downloads FilingSummary.xml, then looks for a <Report> whose <ShortName>
    (case-insensitive) contains target_phrase. Returns the HTML URL (e.g. R2.htm).
    """
    resp = requests.get(summary_url, headers={"User-Agent": user_agent})
    if resp.status_code != 200:
        return None
    soup = BeautifulSoup(resp.content, "xml")
    myreports = soup.find("MyReports")
    if not myreports:
        return None
    phrase_lower = target_phrase.lower()
    for r in myreports.find_all("Report"):
        short_name_tag = r.find("ShortName")
        if short_name_tag:
            short_name = short_name_tag.get_text(strip=True).lower()
            if phrase_lower in short_name:
                html_file_tag = r.find("HtmlFileName")
                if html_file_tag:
                    html_file = html_file_tag.get_text(strip=True)
                    base = re.sub(r"FilingSummary\.xml$", "", summary_url)
                    return base + html_file
    return None

###############################################################################
# Step 2: Parse the Single Table
###############################################################################
def parse_single_statement_table(statement_url: str, user_agent: str) -> pd.DataFrame:
    """
    Downloads the HTML from statement_url and returns the first non-reference table as a DataFrame.
    Skips tables that contain strings like "Namespace Prefix:" or "Data Type:" or "us-gaap_".
    """
    resp = requests.get(statement_url, headers={"User-Agent": user_agent})
    if resp.status_code != 200:
        return pd.DataFrame()
    soup = BeautifulSoup(resp.content, "lxml")
    tables = soup.find_all("table")
    for t in tables:
        table_text = t.get_text(separator=" ", strip=True)
        if ("Namespace Prefix:" in table_text or "Data Type:" in table_text or "us-gaap_" in table_text):
            continue
        rows = []
        for tr in t.find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
            rows.append(cols)
        if rows:
            return pd.DataFrame(rows)
    return pd.DataFrame()

###############################################################################
# Step 3: Custom override function for sections
###############################################################################
def adjust_line_section(row):
    """
    row is a dict with keys: {"type": "line", "section_name", "line_label", "date_values"}.
    We override row["section_name"] if it matches certain patterns:
      - "accounts payable" => "Current liabilities"
      - "accrued and other current liabilities" => "Current liabilities"
      - "common stock, $0.01 par value" => "Post-Statement"
    etc.
    """
    if row["type"] != "line":
        return row["section_name"]  # no change if it's a section

    line_lower = row["line_label"].lower()

    # 1) Move "accounts payable" or "accrued and other current liabilities" to "Current liabilities"
    if "accounts payable" in line_lower or "accrued and other current liabilities" in line_lower:
        return "Current liabilities"

    # 2) If line starts with "common stock, $0.01 par value", put it post-statement
    if line_lower.startswith("common stock, $0.01 par value"):
        return "Post-Statement"

    return row["section_name"]

###############################################################################
# Step 4: Extracting line items with sections
###############################################################################
def extract_line_items_with_sections(df: pd.DataFrame) -> list:
    """
    Parses the raw HTML table (df) into a list of rows with section and line item info.
    Each row is a dict:
      {
         "type": "section" or "line",
         "section_name": <current section>,
         "line_label": <line item label>,
         "date_values": { raw_date_label: value, ... }
      }
    We apply heuristics to detect section headings, then override some line sections.
    """
    if df.empty or df.shape[1] < 2:
        return []

    # Identify the row with date columns
    date_row_index = None
    for i in range(min(3, len(df))):
        row_values = df.iloc[i].tolist()
        date_count = sum(bool(re.search(r"(20\d{2}|19\d{2}|Dec|Mar|Sep|Jun|Feb|Jan|Jul|Aug|Nov|Oct|Apr|May)", str(x), re.IGNORECASE))
                         for x in row_values)
        if date_count >= (len(row_values) // 2):
            date_row_index = i
            break
    if date_row_index is None:
        date_row_index = 0

    date_labels = df.iloc[date_row_index].tolist()
    date_labels = [(x or "").strip() for x in date_labels]

    output_rows = []
    current_section = ""

    def mostly_caps(s):
        alpha = re.sub(r"[^A-Za-z]+", "", s)
        if not alpha:
            return False
        return sum(ch.isupper() for ch in alpha) / len(alpha) > 0.7

    for row_i in range(date_row_index + 1, len(df)):
        row_data = df.iloc[row_i].tolist()
        if not row_data:
            continue

        label_str = (row_data[0] or "").strip()
        non_empty = sum(1 for x in row_data if x and str(x).strip())

        # Heuristic for a new section
        if non_empty <= 2 and (mostly_caps(label_str) or label_str.endswith(":")):
            current_section = label_str.rstrip(":")
            output_rows.append({
                "type": "section",
                "section_name": current_section,
                "line_label": "",
                "date_values": {}
            })
            continue

        # Otherwise, it's a line item
        date_vals = {}
        for col_i in range(1, len(row_data)):
            if col_i < len(date_labels):
                dlabel = date_labels[col_i]
                cell = row_data[col_i]
                val_str = (cell or "").strip()
                if val_str:
                    date_vals[dlabel] = val_str

        row_dict = {
            "type": "line",
            "section_name": current_section,
            "line_label": label_str or f"Line_{row_i}",
            "date_values": date_vals
        }
        # Override the section if it matches custom rules
        row_dict["section_name"] = adjust_line_section(row_dict)

        output_rows.append(row_dict)

    return output_rows

###############################################################################
# Step 5: Merging into a MasterSheet
###############################################################################
def parse_date_label(dlabel: str) -> str:
    """
    Convert a header date string like 'Mar. 31, 2024 (10-Q_2024-02-06)'
    to a canonical 'YYYY-MM-DD' string if possible.
    """
    base_label = re.sub(r"\(.*?\)", "", dlabel).strip()
    base_label = re.sub(r"\b(\w+)\.", r"\1", base_label)
    try:
        dt = dateparser.parse(base_label, fuzzy=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return base_label

def select_closest_date(date_vals: dict, filing_date_str: str) -> (str, str):
    """
    Among the raw date labels in date_vals, pick the one closest to the filing_date_str (YYYY-MM-DD).
    Return (raw_date_label, canonical_date).
    """
    if not date_vals:
        return None, None
    filing_date = datetime.datetime.strptime(filing_date_str, "%Y-%m-%d")
    best_key = None
    best_diff = None
    best_canon = None
    for raw_date in date_vals.keys():
        canon = parse_date_label(raw_date)
        try:
            dt = datetime.datetime.strptime(canon, "%Y-%m-%d")
        except Exception:
            continue
        diff = abs((filing_date - dt).days)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_key = raw_date
            best_canon = canon
    return best_key, best_canon

def merge_statements_into_master(all_statements):
    """
    all_statements: list of {
      "filing_key": str,  # e.g. "10-Q_2025-02-05"
      "filing_date": str, # "YYYY-MM-DD"
      "rows": [ {type, section_name, line_label, date_values}, ... ]
    }
    We pick the date column in each row that's closest to the filing date,
    unify them by canonical date, and store them in (type, section, line).
    """
    master_order = []
    master_data = {}
    all_dates = set()

    for statement in all_statements:
        filing_key = statement["filing_key"]
        filing_date = statement["filing_date"]
        rows = statement["rows"]

        for row in rows:
            key = (row["type"], row["section_name"], row["line_label"])
            if key not in master_order:
                master_order.append(key)
            if key not in master_data:
                master_data[key] = {}

            # pick the date col closest to the filing date
            best_raw, best_canon = select_closest_date(row["date_values"], filing_date)
            if best_canon is not None:
                val = row["date_values"][best_raw]
                master_data[key][best_canon] = val
                all_dates.add(best_canon)

    # Sort the date columns chronologically
    def parse_dt(s):
        try:
            return datetime.datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return datetime.datetime.max

    sorted_dates = sorted(all_dates, key=parse_dt)

    # Build final DataFrame
    final_rows = []
    header_row = ["Line / Section"] + list(sorted_dates)
    final_rows.append(header_row)

    for key in master_order:
        type_, sect_, line_ = key
        if type_ == "section":
            row_data = [sect_] + [""] * len(sorted_dates)
        else:
            row_data = [f"  {line_}"]
            for d in sorted_dates:
                row_data.append(master_data[key].get(d, ""))
        final_rows.append(row_data)

    return pd.DataFrame(final_rows)

###############################################################################
# Main Execution
###############################################################################
def main():
    filings = get_last_filings_10k_10q(CIK, USER_AGENT, FILING_COUNT)
    if not filings:
        print("No recent 10-K/10-Q filings found.")
        return

    raw_sheets = {}
    all_statements = []

    for i, filing in enumerate(filings, start=1):
        form_type = filing["form_type"]
        acc_num = filing["accession_number"]
        fdate = filing["filing_date"]  # "YYYY-MM-DD"
        sheet_name = f"{form_type}_{fdate}"
        print(f"\n({i}/{len(filings)}) Processing {sheet_name} (Acc#: {acc_num})...")

        idx_json = fetch_index_json(CIK, acc_num, USER_AGENT)
        if not idx_json:
            print(f"Could not fetch index.json for {acc_num}. Skipping.")
            continue

        summary_url = find_filing_summary_url(idx_json)
        if not summary_url:
            print(f"FilingSummary.xml not found for {acc_num}. Skipping.")
            continue

        statement_url = find_statement_url_for_balance_sheets(summary_url, USER_AGENT, TARGET_PHRASE)
        if not statement_url:
            print(f"No statement matching '{TARGET_PHRASE}' in {acc_num}. Skipping.")
            continue

        df_table = parse_single_statement_table(statement_url, USER_AGENT)
        raw_sheets[sheet_name] = df_table

        # Extract line items with sections, applying override rules
        rows_with_sections = extract_line_items_with_sections(df_table)
        all_statements.append({
            "filing_key": sheet_name,
            "filing_date": fdate,
            "rows": rows_with_sections
        })
        time.sleep(1)

    if not all_statements:
        print("No statements extracted from any filing.")
        return

    df_master = merge_statements_into_master(all_statements)

    # Write each raw sheet and the master sheet to Excel
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    out_file = os.path.join(OUTPUT_FOLDER, OUTPUT_FILENAME)
    with pd.ExcelWriter(out_file) as writer:
        # Save raw tables
        for sheet_name, df in raw_sheets.items():
            if df.empty:
                pd.DataFrame({"Info": [f"No data for {sheet_name}"]}).to_excel(writer, sheet_name=sheet_name[:31], index=False)
            else:
                df.to_excel(writer, sheet_name=sheet_name[:31], index=False, header=False)
        # Save master sheet
        df_master.to_excel(writer, sheet_name="MasterSheet", index=False, header=False)

    print(f"\nDone! Wrote {len(raw_sheets)} filings + MasterSheet to {out_file}")

if __name__ == "__main__":
    main()
