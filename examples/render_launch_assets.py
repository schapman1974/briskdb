#!/usr/bin/env python3
"""Render the README GIF and exact-text GitHub social preview."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "docs" / "assets"
BACKGROUND = ASSETS / "social-preview-background.png"
SOCIAL_PREVIEW = ASSETS / "social-preview.png"
DEMO_GIF = ASSETS / "briskdb-demo.gif"
MINT = "#56e0ac"
PALE = "#edf8f5"
MUTED = "#8da6a0"
TERMINAL = "#08110f"


def font(size: int) -> ImageFont.FreeTypeFont:
    candidates = [
        "/System/Library/Fonts/SFNSMono.ttf",
        "/System/Library/Fonts/SFNS.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        if Path(candidate).is_file():
            return ImageFont.truetype(candidate, size=size)
    return ImageFont.load_default(size=size)


def sans_font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont:
    candidates = [
        "/System/Library/Fonts/SFNS.ttf",
        "/System/Library/Fonts/HelveticaNeue.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        if Path(candidate).is_file():
            return ImageFont.truetype(candidate, size=size)
    return font(size)


def demo_summary() -> dict[str, object]:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "examples" / "launch_demo.py"), "--json"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def terminal_frame(lines: list[tuple[str, str]]) -> Image.Image:
    image = Image.new("RGB", (1200, 675), "#06100e")
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle(
        (35, 35, 1165, 640), radius=22, fill=TERMINAL, outline="#24423b", width=2
    )
    draw.rounded_rectangle((35, 35, 1165, 92), radius=22, fill="#10201c")
    draw.rectangle((35, 70, 1165, 92), fill="#10201c")
    for x, color in ((70, "#ff6b6b"), (100, "#ffd166"), (130, MINT)):
        draw.ellipse((x - 9, 55, x + 9, 73), fill=color)
    draw.text((480, 50), "briskdb demo — zsh", font=font(20), fill=MUTED)
    y = 120
    for text, color in lines:
        draw.text((72, y), text, font=font(25), fill=color)
        y += 45
    return image


def render_demo_gif(summary: dict[str, object]) -> None:
    counts = " / ".join(str(value) for value in summary["shard_counts"])
    port = str(summary["postgres_address"]).rsplit(":", 1)[-1]
    transcript = [
        ("$ python -m pip install briskdb", PALE),
        (f"✓ native wheel ready · briskdb {summary['version']}", MINT),
        ("$ python examples/launch_demo.py", PALE),
        (f"BriskDB · {summary['shards']} independent SQLite WAL shards", "#b9fff0"),
        (
            f"✓ {summary['writes']} writes from {summary['writers']} Python threads",
            MINT,
        ),
        (f"✓ shard files: {counts} rows", MINT),
        (f"✓ routed reads: {summary['read_rows']} rows", MINT),
        (f"✓ HTTP /health → {summary['health']}", MINT),
        (f"✓ PostgreSQL → 127.0.0.1:{port}", MINT),
        ("Your data is still ordinary SQLite.", PALE),
    ]
    frames = [terminal_frame([])]
    durations = [700]
    visible: list[tuple[str, str]] = []
    for line in transcript:
        visible.append(line)
        frames.append(terminal_frame(visible))
        durations.append(650 if line[0].startswith("$") else 850)
    durations[-1] = 2600
    frames[0].save(
        DEMO_GIF,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True,
        disposal=2,
    )


def cover(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    scale = max(size[0] / image.width, size[1] / image.height)
    resized = image.resize(
        (round(image.width * scale), round(image.height * scale)),
        Image.Resampling.LANCZOS,
    )
    left = (resized.width - size[0]) // 2
    top = (resized.height - size[1]) // 2
    return resized.crop((left, top, left + size[0], top + size[1]))


def render_social_preview() -> None:
    image = cover(Image.open(BACKGROUND).convert("RGB"), (1280, 640))
    overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
    alpha = Image.new("L", image.size)
    alpha.putdata(
        [
            max(0, min(235, round(225 * (1 - max(0, x - 480) / 440))))
            for _y in range(image.height)
            for x in range(image.width)
        ]
    )
    overlay.putalpha(alpha)
    image = Image.alpha_composite(image.convert("RGBA"), overlay)
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((64, 62, 292, 104), radius=21, outline=MINT, width=2)
    draw.text((85, 70), "OPEN-SOURCE ALPHA", font=sans_font(18, bold=True), fill=MINT)
    draw.text((64, 142), "BRISKDB", font=sans_font(66, bold=True), fill=MINT)
    draw.text((64, 235), "SQLite files.", font=sans_font(52, bold=True), fill=PALE)
    draw.text(
        (64, 298), "One sharded database.", font=sans_font(52, bold=True), fill=PALE
    )
    draw.text(
        (68, 395),
        "Parallel writes  ·  PostgreSQL  ·  HTTP",
        font=sans_font(24),
        fill="#b9fff0",
    )
    draw.text((68, 442), "Embedded Rust + Python", font=sans_font(24), fill="#b9fff0")
    draw.line((68, 510, 555, 510), fill="#2c6758", width=2)
    draw.text(
        (68, 535),
        "Ordinary SQLite shards. No SQLite fork.",
        font=sans_font(21),
        fill=MUTED,
    )
    image.convert("RGB").save(SOCIAL_PREVIEW, optimize=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--social-only", action="store_true")
    args = parser.parse_args()
    render_social_preview()
    if not args.social_only:
        render_demo_gif(demo_summary())
    print(SOCIAL_PREVIEW.relative_to(ROOT))
    if not args.social_only:
        print(DEMO_GIF.relative_to(ROOT))


if __name__ == "__main__":
    main()
