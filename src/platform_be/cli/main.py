import argparse
import asyncio
import sys

import uvicorn

from platform_be.cli.bootstrap_admin import bootstrap_admin
from platform_be.core.errors import APIError


def main() -> None:
    parser = argparse.ArgumentParser(prog="platform-be")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Run the API server")
    run.add_argument("--host", default="0.0.0.0")
    run.add_argument("--port", type=int, default=8000)
    run.add_argument("--reload", action="store_true")
    bootstrap = commands.add_parser("bootstrap-admin", help="Grant the first Platform Admin role")
    bootstrap.add_argument("--email", required=True)
    args = parser.parse_args()

    if args.command == "run":
        uvicorn.run("platform_be.main:app", host=args.host, port=args.port, reload=args.reload)
    elif args.command == "bootstrap-admin":
        try:
            asyncio.run(bootstrap_admin(args.email))
        except APIError as exc:
            print(f"{exc.code}: {exc.message}", file=sys.stderr)
            raise SystemExit(1) from exc
        print("Platform Admin role granted.")
