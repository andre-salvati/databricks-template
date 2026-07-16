"""
Render the repo's star history as committed SVGs (light + dark) for the README.

Reads stargazer timestamps from the GitHub API via `gh` and plots them locally, so the
README depends on files in this repo rather than on a third-party chart service.
Usage: uv run python scripts/star_history.py [--repo OWNER/NAME] [--out-dir DIR]
"""

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from xml.sax.saxutils import escape

# Slot 1 (blue) of the validated categorical palette, stepped per surface; a single
# series needs no legend, so the title carries identity and the line carries the color.
THEMES = {
    "light": {"surface": "#fcfcfb", "text": "#0b0b0b", "muted": "#52514e", "grid": "#e6e6e3", "series": "#2a78d6"},
    "dark": {"surface": "#1a1a19", "text": "#ffffff", "muted": "#c3c2b7", "grid": "#33332f", "series": "#3987e5"},
}

WIDTH, HEIGHT = 800, 400
PAD_L, PAD_R, PAD_T, PAD_B = 64, 88, 56, 44
MAX_POINTS = 200
FONT = "-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif"


def fetch_stars(repo: str) -> list[datetime]:
    """Return every `starred_at`, oldest first. GitHub caps this endpoint at 40,000 stars."""
    cmd = [
        "gh",
        "api",
        "-H",
        "Accept: application/vnd.github.star+json",
        f"/repos/{repo}/stargazers?per_page=100",
        "--paginate",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        raise RuntimeError("`gh` not found — install the GitHub CLI or ensure it is on PATH.") from None
    if result.returncode != 0:
        raise RuntimeError(f"gh api failed for {repo}: {result.stderr.strip()}")

    records = json.loads(result.stdout)  # `gh --paginate` merges the pages into one array
    stars = sorted(
        datetime.strptime(r["starred_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        for r in records
        if r.get("starred_at")
    )
    if not stars:
        raise RuntimeError(f"{repo} has no stargazers with timestamps — nothing to plot.")
    return stars


def nice_ticks(top: int, count: int = 4) -> list[int]:
    """Round tick values (0 / 25 / 50 ...) covering 0..top."""
    raw = max(1, top / count)
    magnitude = 10 ** (len(str(int(raw))) - 1)
    step = next((m * magnitude for m in (1, 2, 2.5, 5, 10) if m * magnitude >= raw), raw)
    ticks, value = [], 0
    while value < top + step:
        ticks.append(int(value))
        value += step
    return ticks


def build_svg(repo: str, stars: list[datetime], theme: dict[str, str]) -> str:
    # Cumulative count: star N lands at index N. Anchor the series at zero on the first
    # star's day so the line starts on the baseline instead of at 1.
    points = [(stars[0], 0)] + [(when, i + 1) for i, when in enumerate(stars)]
    if len(points) > MAX_POINTS:
        stride = len(points) / MAX_POINTS
        sampled = [points[int(i * stride)] for i in range(MAX_POINTS)]
        points = sampled + [points[-1]]  # never drop the current total

    t0, t1 = points[0][0].timestamp(), points[-1][0].timestamp()
    span = max(t1 - t0, 1)
    total = points[-1][1]
    ticks = nice_ticks(total)
    y_top = ticks[-1]

    def px(when: datetime) -> float:
        return PAD_L + (when.timestamp() - t0) / span * (WIDTH - PAD_L - PAD_R)

    def py(count: int) -> float:
        return HEIGHT - PAD_B - (count / y_top) * (HEIGHT - PAD_T - PAD_B)

    coords = [(px(w), py(c)) for w, c in points]
    line = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in coords)
    area = f"{line} L{coords[-1][0]:.1f},{HEIGHT - PAD_B} L{coords[0][0]:.1f},{HEIGHT - PAD_B} Z"

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}" font-family="{FONT}">',
        f'<rect width="{WIDTH}" height="{HEIGHT}" fill="{theme["surface"]}"/>',
        f'<text x="{PAD_L}" y="30" fill="{theme["text"]}" font-size="15" font-weight="600">'
        f"{escape(repo)} — stars over time</text>",
        f'<defs><linearGradient id="fade" x1="0" x2="0" y1="0" y2="1">'
        f'<stop offset="0" stop-color="{theme["series"]}" stop-opacity="0.18"/>'
        f'<stop offset="1" stop-color="{theme["series"]}" stop-opacity="0"/></linearGradient></defs>',
    ]

    # Recessive hairline gridlines, with the y ticks carrying the values the line doesn't label.
    for tick in ticks:
        y = py(tick)
        out.append(
            f'<line x1="{PAD_L}" x2="{WIDTH - PAD_R}" y1="{y:.1f}" y2="{y:.1f}" '
            f'stroke="{theme["grid"]}" stroke-width="1"/>'
        )
        out.append(
            f'<text x="{PAD_L - 10}" y="{y + 4:.1f}" fill="{theme["muted"]}" font-size="11" '
            f'text-anchor="end">{tick:,}</text>'
        )

    for frac in (0, 0.5, 1):
        when = datetime.fromtimestamp(t0 + span * frac, tz=timezone.utc)
        anchor = "start" if frac == 0 else "middle" if frac == 0.5 else "end"
        out.append(
            f'<text x="{px(when):.1f}" y="{HEIGHT - PAD_B + 20:.1f}" fill="{theme["muted"]}" '
            f'font-size="11" text-anchor="{anchor}">{when.strftime("%b %Y")}</text>'
        )

    out.append(f'<path d="{area}" fill="url(#fade)"/>')
    out.append(
        f'<path d="{line}" fill="none" stroke="{theme["series"]}" stroke-width="2" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
    )

    # One direct label — the current total. Sparing is what makes it readable.
    end_x, end_y = coords[-1]
    out.append(f'<circle cx="{end_x:.1f}" cy="{end_y:.1f}" r="4" fill="{theme["series"]}"/>')
    out.append(
        f'<text x="{end_x + 10:.1f}" y="{end_y + 4:.1f}" fill="{theme["text"]}" font-size="13" '
        f'font-weight="600">{total:,}</text>'
    )
    out.append("</svg>")
    return "\n".join(out) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate star-history SVGs for the README.")
    parser.add_argument("--repo", default="andre-salvati/databricks-template", help="OWNER/NAME")
    parser.add_argument("--out-dir", default="assets", help="directory to write the SVGs into")
    args = parser.parse_args()

    stars = fetch_stars(args.repo)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for name, theme in THEMES.items():
        suffix = "" if name == "light" else "_dark"
        path = out_dir / f"star_history{suffix}.svg"
        path.write_text(build_svg(args.repo, stars, theme))
        print(f"wrote {path}")

    print(f"{len(stars)} stars — {stars[0]:%Y-%m-%d} to {stars[-1]:%Y-%m-%d}")


if __name__ == "__main__":
    main()
