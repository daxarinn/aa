from __future__ import annotations

import argparse
import sys
from pathlib import Path

from aa_app.config import DEFAULT_CSV_PATH, DEFAULT_DB_PATH
from aa_app.scraping import scrape_all
from aa_app.storage import load_dataframe, maybe_copy_to_clipboard, summarize_dataframe
from aa_app.web import build_app, detect_local_ipv4_addresses

def cmd_scrape(args: argparse.Namespace) -> int:
    df = scrape_all(args.db, args.csv, args.copy)
    print(summarize_dataframe(df))
    print(f"SQLite: {args.db}")
    print(f"CSV: {args.csv}")
    if args.copy:
        print("Clipboard: copied")
    return 0


def cmd_preview(args: argparse.Namespace) -> int:
    if not args.db.exists():
        raise FileNotFoundError(f"Gagnagrunnur fannst ekki: {args.db}")

    df = load_dataframe(args.db)
    if args.copy:
        maybe_copy_to_clipboard(df)

    print(summarize_dataframe(df))
    print()
    preview_columns = [
        "source",
        "weekday_is",
        "time_display",
        "meeting_name",
        "subtitle",
        "fellowship",
        "format",
        "gender_restriction",
        "access_restriction",
        "location_text",
        "canonical_location_text",
        "location_nickname",
        "venue_text",
        "zoom_meeting_id",
        "source_record_id",
        "source_page_url",
        "source_locator",
        "source_excerpt",
    ]
    print(df[preview_columns].head(args.limit).to_string(index=False))
    if args.copy:
        print("\nClipboard: copied")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    if not args.db.exists():
        scrape_all(args.db, args.csv, copy_to_clipboard=False)
    app = build_app(args.db)
    print(f"Serving on http://{args.host}:{args.port}")
    if args.host == "0.0.0.0":
        for addr in detect_local_ipv4_addresses():
            print(f"Try in browser: http://{addr}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scraper fyrir AA fundaskrár")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape_parser = subparsers.add_parser("scrape", help="Sækir lifandi gögn og skrifar snapshot")
    scrape_parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    scrape_parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH)
    scrape_parser.add_argument("--copy", action="store_true")
    scrape_parser.set_defaults(func=cmd_scrape)

    preview_parser = subparsers.add_parser("preview", help="Sýnir yfirlit úr núverandi SQLite snapshot")
    preview_parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    preview_parser.add_argument("--copy", action="store_true")
    preview_parser.add_argument("--limit", type=int, default=30)
    preview_parser.set_defaults(func=cmd_preview)

    serve_parser = subparsers.add_parser("serve", help="Keyrir einfalt Flask-yfirlit")
    serve_parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    serve_parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH)
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=5000)
    serve_parser.set_defaults(func=cmd_serve)

    return parser


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = build_parser()
    args = parser.parse_args(["scrape"] if len(sys.argv) == 1 else None)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
