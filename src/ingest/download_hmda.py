"""
Download REAL public HMDA Loan Application Register data from the CFPB.

The CFPB publishes the full national HMDA dataset (tens of millions of records per
year) at https://ffiec.cfpb.gov/data-publication/. This script fetches the modified
LAR file for a given year so you can run the pipeline on genuine large-scale data
instead of the synthetic generator.

Note: a single year's national file is multiple GB compressed. Start with one state
(the CFPB API supports state filtering) before pulling the national file.

Usage:
    python src/ingest/download_hmda.py --year 2023 --state CA --output data/raw_hmda
"""
import argparse
import os
import urllib.request

BASE = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--state", default=None, help="Two-letter state code; omit for national")
    p.add_argument("--output", default="data/raw_hmda")
    args = p.parse_args()

    os.makedirs(args.output, exist_ok=True)
    params = f"?years={args.year}"
    if args.state:
        params += f"&states={args.state}"
    url = BASE + params
    dest = os.path.join(args.output, f"hmda_{args.year}_{args.state or 'US'}.csv")

    print(f"Downloading {url}")
    print("This can be large — see the module docstring before pulling national files.")
    urllib.request.urlretrieve(url, dest)
    print(f"Saved to {dest}")


if __name__ == "__main__":
    main()
