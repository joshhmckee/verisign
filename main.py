"""
Scrape Verisign zone file domain name base data and append to data.csv.
Source: https://www.verisign.com/resources/zone-file/
"""

import csv
import json
import ssl
from datetime import date
from typing import Optional
from urllib.request import urlopen

ZONE_COUNTS_URL = "https://www.verisign.com/zone-domain-counts/zone_counts.json"
DATA_CSV = "data.csv"


def fetch_zone_data() -> dict:
    """Fetch domain name base counts from Verisign's zone counts JSON."""
    # Avoid SSL cert errors on macOS (Python.org installs often lack CA certs;
    # run "Install Certificates.command" from your Python folder to fix properly).
    context = ssl._create_unverified_context()
    with urlopen(ZONE_COUNTS_URL, timeout=15, context=context) as response:
        return json.loads(response.read().decode())


def parse_counts(data: dict) -> tuple[int, int, int]:
    """Extract .com, .net, and total domain name base counts."""
    com = data["comDomainNameBase"]["domainNameCounts"]
    net = data["netDomainNameBase"]["domainNameCounts"]
    total = com + net
    return com, net, total


def get_latest_row() -> Optional[tuple[int, int, int]]:
    """Return (com, net, total) from the last data row in data.csv, or None if empty/no data."""
    try:
        with open(DATA_CSV, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
    except FileNotFoundError:
        return None
    if not rows or rows[0][0].lower() == "date":
        data_rows = rows[1:] if len(rows) > 1 else []
    else:
        data_rows = rows
    if not data_rows:
        return None
    last = data_rows[-1]
    if len(last) < 4:
        return None
    try:
        return int(last[1]), int(last[2]), int(last[3])
    except (ValueError, IndexError):
        return None


def append_to_csv(com: int, net: int, total: int) -> None:
    """Append one row (date, com, net, total) to data.csv. Create file with header if needed."""
    today = date.today().isoformat()
    row = [today, com, net, total]

    try:
        with open(DATA_CSV, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            first = next(reader, None)
            has_header = first is not None and first[0].lower() == "date"
    except FileNotFoundError:
        has_header = False

    with open(DATA_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not has_header:
            writer.writerow(["date", "com", "net", "total"])
        writer.writerow(row)


def main() -> None:
    data = fetch_zone_data()
    com, net, total = parse_counts(data)
    latest = get_latest_row()
    if latest is not None and latest == (com, net, total):
        print("No change; counts match latest entry. Skipping save.")
        return
    append_to_csv(com, net, total)
    print(f"Appended: {date.today().isoformat()} | .com: {com:,} | .net: {net:,} | total: {total:,}")


if __name__ == "__main__":
    main()
