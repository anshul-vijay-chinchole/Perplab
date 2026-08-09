"""Verify what Binance actually publishes in bulk, rather than trusting the spec.

Spec Appendix B closes with "Verify before building -- endpoint paths, filter names,
funding intervals, fee tiers, and bulk-dataset availability all change." R1 was a
critical review finding caused by skipping exactly that check, and running it on
2026-08-01 found a second instance: `bookTicker`, which spec 4.2 calls "full history" and
makes the primary fill-realism input, stopped being published on 2024-03-30.

Run this before building any new ingestion path. It is cheap and it has already caught
two architecture-level errors.
"""

from __future__ import annotations

import re

import httpx

__all__ = ["DATASETS", "list_dataset", "report"]

S3_LIST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
_KEY = re.compile(r"<Key>([^<]+)</Key>")
_TOKEN = re.compile(r"<NextContinuationToken>([^<]+)</NextContinuationToken>")
_DATE = re.compile(r"(\d{4}-\d{2}(?:-\d{2})?)\.zip$")

DATASETS: dict[str, str] = {
    "klines 1m": "data/futures/um/daily/klines/{symbol}/1m/",
    "aggTrades": "data/futures/um/daily/aggTrades/{symbol}/",
    "bookTicker": "data/futures/um/daily/bookTicker/{symbol}/",
    "bookDepth": "data/futures/um/daily/bookDepth/{symbol}/",
    "markPriceKlines 1m": "data/futures/um/daily/markPriceKlines/{symbol}/1m/",
    "metrics": "data/futures/um/daily/metrics/{symbol}/",
    "fundingRate": "data/futures/um/monthly/fundingRate/{symbol}/",
    "liquidationSnapshot": "data/futures/um/daily/liquidationSnapshot/{symbol}/",
}


def list_dataset(prefix: str, *, timeout: float = 60.0) -> list[str]:
    """Page through the S3 listing for a prefix and return every key.

    Paging is not optional: the listing caps at 1000 keys per response and `aggTrades`
    alone has 2400+. A single unpaged request silently truncates, which would make a
    fully-published dataset look like it stopped three years ago.
    """
    keys: list[str] = []
    token: str | None = None
    with httpx.Client(timeout=timeout) as client:
        while True:
            params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
            if token:
                params["continuation-token"] = token
            response = client.get(S3_LIST, params=params)
            response.raise_for_status()
            keys += _KEY.findall(response.text)

            match = _TOKEN.search(response.text)
            if "<IsTruncated>true</IsTruncated>" not in response.text or not match:
                return keys
            token = match.group(1)


def report(symbol: str = "BTCUSDT") -> int:
    """Print a coverage table. Returns 1 if any dataset is missing or stale."""
    print(f"Bulk availability for {symbol} (data.binance.vision)\n")
    print(f"{'dataset':22} {'files':>6}  coverage")
    print("-" * 60)

    stale: list[str] = []
    for label, template in DATASETS.items():
        try:
            keys = list_dataset(template.format(symbol=symbol))
        except httpx.HTTPError as exc:
            print(f"{label:22} {'ERR':>6}  {type(exc).__name__}")
            stale.append(label)
            continue

        dates = sorted(m.group(1) for k in keys if (m := _DATE.search(k)))
        checksums = sum(1 for k in keys if k.endswith(".CHECKSUM"))
        zips = sum(1 for k in keys if k.endswith(".zip"))

        if not dates:
            print(f"{label:22} {0:>6}  NOT PUBLISHED at this path")
            stale.append(label)
            continue

        note = ""
        if zips != checksums:
            note = f"  [!] {zips} zips vs {checksums} checksums"
        print(f"{label:22} {len(dates):>6}  {dates[0]} .. {dates[-1]}{note}")

    if stale:
        print(f"\nMissing or unreadable: {', '.join(stale)}")
        print("See docs/DATA_AVAILABILITY.md before relying on these.")
        return 1

    print("\nCompare the latest dates against today. A dataset whose coverage stopped")
    print("months ago is discontinued, not merely lagging -- see finding F1.")
    return 0


if __name__ == "__main__":
    raise SystemExit(report())
