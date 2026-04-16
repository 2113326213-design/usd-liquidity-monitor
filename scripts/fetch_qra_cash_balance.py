#!/usr/bin/env python3
"""
Download Treasury QRA PDF, parse end-of-quarter cash balance, optionally POST to running API.

  QRA_PDF_URL=https://.../qra.pdf \\
  python scripts/fetch_qra_cash_balance.py

  python scripts/fetch_qra_cash_balance.py --url https://.../file.pdf --post-api http://127.0.0.1:8765
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Repo root: usd_liquidity_monitor/
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch & parse Treasury QRA PDF (cash balance).")
    ap.add_argument(
        "--url",
        default=os.environ.get("QRA_PDF_URL", "").strip(),
        help="Direct PDF URL (else env QRA_PDF_URL)",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=_ROOT / "qra_latest.pdf",
        help="Where to save the downloaded PDF",
    )
    ap.add_argument(
        "--post-api",
        metavar="BASE",
        default="",
        help="If set (e.g. http://127.0.0.1:8765), POST /liquidity/qra/refresh on that API",
    )
    args = ap.parse_args()
    if not args.url:
        print("Set --url or QRA_PDF_URL", file=sys.stderr)
        return 1

    try:
        import httpx
    except ImportError:
        print("pip install httpx", file=sys.stderr)
        return 1

    from qra_refresher import parse_qra_pdf_bytes

    print("GET", args.url)
    r = httpx.get(args.url, timeout=120.0, follow_redirects=True)
    r.raise_for_status()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_bytes(r.content)
    print("Wrote", args.out, "bytes=", len(r.content))

    parsed = parse_qra_pdf_bytes(r.content)
    print("parse_status:", parsed.get("status"))
    print("parsed_end_quarter_cash_bn:", parsed.get("parsed_end_quarter_cash_bn"))
    if parsed.get("snippet"):
        print("snippet:", parsed["snippet"][:300].replace("\n", " "), "...")

    base = (args.post_api or "").rstrip("/")
    if base:
        try:
            pr = httpx.post(f"{base}/liquidity/qra/refresh", timeout=120.0)
            pr.raise_for_status()
            print("POST /liquidity/qra/refresh ->", pr.status_code, pr.json())
        except Exception as exc:  # noqa: BLE001
            print("POST failed:", exc, file=sys.stderr)
            return 3

    return 0 if parsed.get("parsed_end_quarter_cash_bn") is not None else 2


if __name__ == "__main__":
    raise SystemExit(main())
