"""Print the pipeline's effective configuration, with secrets redacted.

Two purposes:

1. It exercises config.py and logging_setup.py together against the real
   config/settings.yaml and the real .env - the Step 2 equivalent of
   check_infra.py.
2. It answers "what settings is this run actually using?", which is the first
   question worth asking when a pipeline behaves unexpectedly in an
   environment you are not sitting in front of.

    python scripts/show_config.py            # human-readable
    python scripts/show_config.py --json     # as a JSON log line

Exits non-zero with the full list of problems if configuration is invalid.
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
from datetime import date

from wrc_pipeline.config import ConfigError, Settings, load_settings
from wrc_pipeline.logging_setup import Event, get_logger, setup_logging


def _as_dict(settings: Settings) -> dict:
    """Flatten Settings to plain data, replacing anything secret."""
    data = dataclasses.asdict(settings)

    # Never print credentials, even to a local terminal - this output gets
    # pasted into issues and chat.
    data["mongo"]["uri"] = settings.mongo.safe_uri
    data["object_store"]["secret_key"] = "***"

    data["config_file"] = str(settings.config_file)
    data["logging"]["file"] = str(settings.logging.file) if settings.logging.file else None
    return data


def _print_human(settings: Settings) -> None:
    data = _as_dict(settings)
    print(f"Config file : {data['config_file']}")
    print(f"Log level   : {settings.logging.level}")
    print(f"Log file    : {data['logging']['file'] or '(stdout only)'}")
    print()

    for section in ("source", "partitioning", "mongo", "object_store", "scraping"):
        print(f"[{section}]")
        for key, value in data[section].items():
            print(f"  {key:<34} {value}")
        print()

    print("[bodies]")
    for name, body_id in sorted(settings.bodies.items(), key=lambda kv: kv[1]):
        print(f"  {name:<34} {body_id}")
    print()

    # Show the derived values, since these are where mistakes actually surface.
    print("[derived]")
    print(f"  {'search_url':<34} {settings.source.search_url}")
    sample = date(2025, 10, 1)
    print(
        f"  {'example date filter':<34} "
        f"{sample.isoformat()} -> {settings.source.format_date(sample)!r}"
        "   (no leading zeros: correct)"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the configuration as a JSON log line instead of a table",
    )
    args = parser.parse_args(argv)

    try:
        settings = load_settings()
    except ConfigError as exc:
        # Printed to stderr, not logged: logging is configured *from* settings,
        # so it is not necessarily usable at this point.
        print(f"\n{exc}\n", file=sys.stderr)
        return 1

    if args.json:
        setup_logging(settings)
        get_logger("wrc_pipeline.config").info(
            "configuration loaded",
            extra={"event": Event.RUN_STARTED, "config": _as_dict(settings)},
        )
    else:
        _print_human(settings)

    return 0


if __name__ == "__main__":
    sys.exit(main())
