"""Measure throughput at several concurrency settings, to choose them evidence-first.

The exercise asks for "the fastest way to scrape the URLs without getting
blocked". Both halves of that are empirical questions about a specific server,
and guessing at them produces numbers nobody can defend. This runs the same
workload under several AutoThrottle profiles and reports what actually happened
- so the settings in ``config/settings.yaml`` have a measurement behind them
rather than an opinion.

    python scripts/tune_throughput.py 2024-02-01 2024-02-29 --bodies labour_court

**AutoThrottle is the lever being tuned**, not ``DOWNLOAD_DELAY``. AutoThrottle
measures the server's actual latency and adjusts its own delay to hold roughly
``AUTOTHROTTLE_TARGET_CONCURRENCY`` requests in flight - so it adapts when the
site is busy, which a fixed sleep cannot. ``DOWNLOAD_DELAY`` stays a floor
beneath it and ``CONCURRENT_REQUESTS_PER_DOMAIN`` a hard ceiling above it.

**How to read the output.** Throughput that stops improving as concurrency
rises means the ceiling is the server, not the setting - and pushing past that
point buys nothing while costing the site something. Any non-200 status, any
retry, or a rising latency curve is a signal to stop lower.

This is deliberately polite: each profile runs one small partition, and the
whole sweep is a few hundred requests against a public government service. It
is not a load test and should never be pointed at one.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from wrc_pipeline.config import ConfigError, load_settings

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Profile:
    """One set of throughput settings to measure."""

    name: str
    target_concurrency: float
    per_domain: int
    delay: float


# Deliberately conservative and deliberately short. Doubling the target each
# time finds the plateau in four runs; going further would be a load test.
DEFAULT_PROFILES = (
    Profile("baseline  (serial-ish)", 1.0, 2, 0.5),
    Profile("target 2", 2.0, 4, 0.25),
    Profile("target 4", 4.0, 8, 0.25),
    Profile("target 8", 8.0, 16, 0.1),
)


def write_profile_config(base_config: Path, profile: Profile, dest: Path) -> Path:
    """Copy the real config with only the throughput settings changed.

    Editing a copy rather than the committed file means a sweep cannot leave
    the project configured with whatever the last profile happened to be.
    """
    text = base_config.read_text(encoding="utf-8")
    replacements = {
        "autothrottle_target_concurrency": profile.target_concurrency,
        "concurrent_requests_per_domain": profile.per_domain,
        "download_delay": profile.delay,
    }
    out: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        for key, value in replacements.items():
            if stripped.startswith(f"{key}:"):
                indent = line[: len(line) - len(line.lstrip())]
                line = f"{indent}{key}: {value}"
                break
        out.append(line)

    dest.write_text("\n".join(out) + "\n", encoding="utf-8")
    return dest


def run_profile(
    args: argparse.Namespace, profile: Profile, workdir: Path, base_config: Path
) -> dict[str, Any]:
    """Run the workload once under ``profile`` and return its stats."""
    config_path = write_profile_config(
        base_config, profile, workdir / f"{profile.name.split()[0]}.yaml"
    )
    stats_path = workdir / f"{profile.name.split()[0]}.json"

    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "run_spider.py"),
        args.start_date,
        args.end_date,
        "--stats-json",
        str(stats_path),
    ]
    if args.bodies:
        command += ["--bodies", args.bodies]

    environment = {"WRC_CONFIG_FILE": str(config_path)}
    print(f"  running {profile.name} ...", end="", flush=True)

    import os

    subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={**os.environ, **environment},
    )

    if not stats_path.exists():
        print(" FAILED (no stats produced)")
        return {}

    stats = json.loads(stats_path.read_text(encoding="utf-8"))
    print(f" {stats['scrapy'].get('elapsed_time_seconds', 0):.1f}s")
    return stats


def report(profiles: list[tuple[Profile, dict[str, Any]]]) -> None:
    print()
    print(
        f"  {'profile':<22} {'target':>6} {'per-dom':>8} {'delay':>6} "
        f"{'elapsed':>8} {'reqs':>6} {'req/min':>8} {'MB':>7} {'non-200':>8} {'retries':>8}"
    )
    print("  " + "-" * 104)

    for profile, stats in profiles:
        if not stats:
            print(f"  {profile.name:<22} {'run failed':>60}")
            continue
        scrapy_stats = stats["scrapy"]
        elapsed = scrapy_stats.get("elapsed_time_seconds", 0) or 0.001
        requests = scrapy_stats.get("downloader/request_count", 0)
        megabytes = (scrapy_stats.get("downloader/response_bytes", 0)) / 1_048_576

        non_200 = sum(
            value
            for key, value in scrapy_stats.items()
            if key.startswith("downloader/response_status_count/")
            and not key.endswith(("/200", "/304"))
        )
        retries = sum(
            value for key, value in scrapy_stats.items() if key.startswith("retry/")
        )

        print(
            f"  {profile.name:<22} {profile.target_concurrency:>6.1f} "
            f"{profile.per_domain:>8} {profile.delay:>6.2f} "
            f"{elapsed:>7.1f}s {requests:>6} {requests / elapsed * 60:>8.1f} "
            f"{megabytes:>7.1f} {non_200:>8} {retries:>8}"
        )

    print()
    print("  Read it this way:")
    print("    req/min stops rising  -> the server is the ceiling, not the setting")
    print("    non-200 or retries    -> stop below this profile")
    print("    Choose the lowest setting that reaches the plateau, not the highest")
    print("    one that still works.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("start_date", help="first day of the workload, ISO")
    parser.add_argument("end_date", help="last day, inclusive")
    parser.add_argument("--bodies", help="comma-separated body names")
    args = parser.parse_args(argv)

    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 2

    print("Throughput sweep")
    print(f"  workload: {args.start_date} to {args.end_date}, {args.bodies or 'all bodies'}")
    print(f"  base config: {settings.config_file}")
    print("  each profile runs the same workload once\n")

    results: list[tuple[Profile, dict[str, Any]]] = []
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        for profile in DEFAULT_PROFILES:
            results.append((profile, run_profile(args, profile, workdir, settings.config_file)))

    report(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
