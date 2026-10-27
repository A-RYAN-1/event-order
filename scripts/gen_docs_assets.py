#!/usr/bin/env python3
"""Generate the README and docs figures as SVG (light and dark) plus the social preview PNG.

Standard library only for the SVGs; the PNG needs Pillow and the Segoe UI / Consolas fonts, and is skipped when either
is missing. Run from the repository root:

    python scripts/gen_docs_assets.py

Numbers in the benchmark figure are copied from the README proof table; update both together.
"""
import os
import sys

OUT = os.path.join("docs", "assets")
SANS = "system-ui, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

THEMES = {
    "light": dict(card="#f6f8fa", border="#d0d7de", box="#ffffff", text="#1f2328", muted="#59636e",
                  accent="#0969da", good="#1a7f37", warn="#9a6700", bad="#cf222e", tint="#ddf4ff"),
    "dark": dict(card="#0d1117", border="#30363d", box="#161b22", text="#e6edf3", muted="#8b949e",
                 accent="#58a6ff", good="#3fb950", warn="#d29922", bad="#f85149", tint="#12294a"),
}


class Svg:
    def __init__(self, w, h, t, title, desc):
        self.w, self.h, self.t = w, h, t
        self.parts = []
        self.head = (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" '
            f'aria-labelledby="t d"><title id="t">{title}</title><desc id="d">{desc}</desc>'
            f'<defs><marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
            f'orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{t["muted"]}"/></marker>'
            f'<marker id="ac" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
            f'orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{t["accent"]}"/></marker></defs>'
            f'<rect x="0.5" y="0.5" width="{w - 1}" height="{h - 1}" rx="12" fill="{t["card"]}" stroke="{t["border"]}"/>'
        )

    def rect(self, x, y, w, h, fill=None, stroke=None, rx=8, dash=None, sw=1.5, op=None):
        a = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill or "none"}"'
        if stroke:
            a += f' stroke="{stroke}" stroke-width="{sw}"'
        if dash:
            a += f' stroke-dasharray="{dash}"'
        if op is not None:
            a += f' opacity="{op}"'
        self.parts.append(a + "/>")

    def text(self, x, y, s, size=14, fill=None, anchor="start", weight="400", mono=False, op=None):
        fam = MONO if mono else SANS
        a = (f'<text x="{x}" y="{y}" font-family="{fam}" font-size="{size}" font-weight="{weight}" '
             f'fill="{fill or self.t["text"]}" text-anchor="{anchor}"')
        if op is not None:
            a += f' opacity="{op}"'
        self.parts.append(a + f">{s}</text>")

    def line(self, x1, y1, x2, y2, color=None, arrow=None, dash=None, sw=1.5):
        c = color or self.t["muted"]
        a = f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{c}" stroke-width="{sw}"'
        if dash:
            a += f' stroke-dasharray="{dash}"'
        if arrow:
            a += f' marker-end="url(#{arrow})"'
        self.parts.append(a + "/>")

    def raw(self, s):
        self.parts.append(s)

    def save(self, name):
        os.makedirs(OUT, exist_ok=True)
        with open(os.path.join(OUT, name), "w", encoding="utf-8", newline="\n") as f:
            f.write(self.head + "".join(self.parts) + "</svg>\n")


def logo(s, x, y, color):
    """Three rising bars: events put back in order."""
    for i, h in enumerate((18, 32, 46)):
        s.rect(x + i * 20, y + 46 - h, 14, h, fill=color, rx=4)


def box_row(s, x, y, vals, size=44, gap=8, fill=None, stroke=None, color=None):
    for i, v in enumerate(vals):
        bx = x + i * (size + gap)
        s.rect(bx, y, size, size, fill=fill or s.t["box"], stroke=stroke or s.t["border"], rx=8)
        s.text(bx + size / 2, y + size / 2 + 6, v, 18, color or s.t["text"], "middle", "600", mono=True)


def hero(t, name):
    s = Svg(960, 150, t, "Events put back in order",
            "Events arrive shuffled and are released in event-time order.")
    box_row(s, 40, 34, ["2", "1", "4", "3", "6", "5"])
    s.text(40, 108, "events arrive shuffled", 13, t["muted"])
    s.rect(398, 42, 164, 28, fill=t["tint"], stroke=t["accent"], rx=14)
    s.text(480, 61, "watermark + bounded delay", 12, t["accent"], "middle", "600")
    s.line(366, 56, 394, 56, arrow="ah")
    s.line(566, 56, 594, 56, arrow="ah")
    box_row(s, 608, 34, ["1", "2", "3", "4", "5", "6"], stroke=t["good"], color=t["good"])
    s.text(608, 108, "released in event-time order", 13, t["muted"])
    s.save(name)


def logo_mark():
    os.makedirs(OUT, exist_ok=True)
    bars = "".join(f'<rect x="{i * 20}" y="{46 - h}" width="14" height="{h}" rx="4" fill="#388bfd"/>'
                   for i, (h) in enumerate((18, 32, 46)))
    svg = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 54 46" width="54" height="46" role="img" '
           'aria-label="event-order logo">' + bars + "</svg>")
    with open(os.path.join(OUT, "logo.svg"), "w", encoding="utf-8") as f:
        f.write(svg + chr(10))


def overview(t, name):
    s = Svg(960, 320, t, "How the pieces fit",
            "Sources go through Dedup, a keyed Buffer, your code and a Tracker to a broker commit; Aligned feeds the buffer.")
    s.text(40, 40, "How the pieces fit", 18, t["text"], weight="700")

    def node(x, y, w, title, sub, tag=None, tagc=None):
        s.rect(x, y, w, 64, fill=t["box"], stroke=t["border"])
        s.text(x + w / 2, y + 28, title, 15, t["text"], "middle", "600")
        s.text(x + w / 2, y + 48, sub, 12, t["muted"], "middle")
        if tag:
            s.rect(x + w - 34, y - 11, 34, 20, fill=tagc, rx=10)
            s.text(x + w - 17, y + 4, tag, 11, t["card"], "middle", "700")

    y = 110
    node(40, y, 100, "Sources", "Kafka, MQTT, files")
    node(176, y, 120, "Dedup", "bounded window", "A2", t["warn"])
    node(332, y, 150, "Buffer / Keyed", "event-time order")
    node(518, y, 110, "Your code", "process in order")
    node(664, y, 100, "Tracker", "safe offsets", "A1", t["good"])
    node(800, y, 130, "Commit", "lowest not done")
    for x1, x2 in ((140, 172), (296, 328), (482, 514), (628, 660), (764, 796)):
        s.line(x1, y + 32, x2, y + 32, arrow="ah")
    node(332, 230, 150, "Aligned", "min of source clocks", "A3", t["accent"])
    s.line(90, y + 64, 90, 262, dash="4 4")
    s.line(90, 262, 328, 262, dash="4 4", arrow="ah")
    s.text(100, 250, "per-source watermarks", 11, t["muted"])
    s.line(407, 230, 407, y + 68, color=t["accent"], arrow="ac")
    s.text(417, 205, "AdvanceTo", 12, t["accent"], weight="600", mono=True)
    s.save(name)


def a1(t, name):
    s = Svg(960, 290, t, "A1 safe commit offsets",
            "Eight offsets; 0, 1, 2, 4 and 5 are processed and 3, 6 and 7 are held. The safe commit is offset 3, the naive one is 8.")
    s.text(40, 40, "A1. Commit what is safe, not what was fetched", 18, t["text"], weight="700")
    x0, y0, w, step = 70, 130, 84, 100
    state = ["done", "done", "done", "held", "done", "done", "held", "held"]
    for i, st in enumerate(state):
        x = x0 + i * step
        col = t["good"] if st == "done" else t["warn"]
        s.rect(x, y0, w, 56, fill=t["box"], stroke=col, sw=2)
        s.text(x + w / 2, y0 + 25, f"offset {i}", 13, t["text"], "middle", "600", mono=True)
        s.text(x + w / 2, y0 + 44, "processed" if st == "done" else "held", 12, col, "middle")
    sx = x0 + 3 * step - 8
    s.line(sx, 88, sx, y0 - 4, color=t["accent"], arrow="ac", sw=2)
    s.text(sx, 76, "safe commit = 3", 14, t["accent"], "middle", "700")
    s.text(sx, 62, "(replay starts here)", 11, t["muted"], "middle")
    nx = x0 + 8 * step - 16
    s.line(nx, 88, nx, y0 - 4, color=t["bad"], arrow="ah", sw=2)
    s.text(nx, 76, "naive commit = 8", 14, t["bad"], "middle", "700")
    s.text(x0, 232, "Tracker commits the lowest offset not yet Done. A crash replays 3 to 7; nothing held is lost.", 14, t["text"])
    s.text(x0, 254, "Committing the last fetched offset would skip the held events 3, 6 and 7 for good.", 14, t["muted"])
    s.save(name)


def a2(t, name):
    s = Svg(960, 270, t, "A2 duplicate window",
            "A repeated id inside the window is dropped; a repeat after the window has passed is not caught.")
    s.text(40, 40, "A2. Drop repeats inside a bounded window", 18, t["text"], weight="700")
    s.line(50, 170, 920, 170, arrow="ah")
    s.text(920, 196, "event time", 12, t["muted"], "end")
    s.rect(120, 92, 440, 34, fill=t["tint"], stroke=t["accent"], rx=17)
    s.text(340, 114, "window: ids remembered (bounded by time and by count)", 12, t["accent"], "middle", "600")
    items = [(120, "A", "ok"), (220, "B", "ok"), (320, "C", "ok"), (440, "A", "dup"), (700, "A", "late")]
    for x, ident, kind in items:
        col = {"ok": t["good"], "dup": t["bad"], "late": t["muted"]}[kind]
        s.rect(x - 24, 138, 48, 44, fill=t["box"], stroke=col, sw=2, dash="5 4" if kind != "ok" else None)
        s.text(x, 166, ident, 18, col, "middle", "700", mono=True)
    s.text(440, 214, "repeat inside the window", 12, t["bad"], "middle", "600")
    s.text(440, 230, "dropped, never reaches the buffer", 12, t["muted"], "middle")
    s.text(700, 214, "repeat after the window", 12, t["muted"], "middle", "600")
    s.text(700, 230, "not caught: keep processing idempotent", 12, t["muted"], "middle")
    s.save(name)


def a3(t, name):
    s = Svg(960, 310, t, "A3 aligned watermark",
            "Three sources: p0 is far ahead, p1 is slowest, p2 is idle. The aligned watermark sits at p1.")
    s.text(40, 40, "A3. One watermark over several sources", 18, t["text"], weight="700")
    lanes = [("p0", 110, 800, t["good"], False), ("p1", 160, 560, t["warn"], False), ("p2", 210, 250, t["muted"], True)]
    for name_, y, wx, col, idle in lanes:
        s.text(50, y + 5, name_, 15, t["text"], weight="700", mono=True)
        s.line(100, y, 900, y, color=t["border"], sw=3, dash="2 6" if idle else None)
        s.raw(f'<circle cx="{wx}" cy="{y}" r="9" fill="{col}"' + (' opacity="0.55"' if idle else "") + "/>")
        label = "idle, excluded" if idle else ("watermark" if name_ == "p0" else "slowest active source")
        s.text(wx + 16, y + 5, label, 12, col)
    s.line(560, 84, 560, 236, color=t["accent"], arrow="ac", dash="5 4", sw=2)
    s.text(560, 74, "aligned watermark = minimum of the active sources", 13, t["accent"], "middle", "700")
    s.text(100, 270, "Release by the fastest source and p1's events become late. Aligned holds at p1, skips p2 while it is quiet,", 13, t["text"])
    s.text(100, 290, "and Lag names the source holding it back.", 13, t["muted"])
    s.save(name)


def bench(t, name):
    s = Svg(960, 230, t, "Cost per event against sort-every-add",
            "Buffer.Add costs 131 ns per event; sorting the buffer on every add costs 1493 ns, at a disorder window of 256.")
    s.text(40, 40, "Cost per event at a disorder window of 256 (ns, lower is better)", 18, t["text"], weight="700")
    top = 1493.0
    rows = [("Buffer.Add (0 allocations)", 131, t["accent"]), ("sort on every add (3 allocations)", 1493, t["muted"])]
    for i, (label, v, col) in enumerate(rows):
        y = 78 + i * 52
        s.text(40, y + 22, label, 14, t["text"])
        bw = max(4, int(v / top * 470))
        s.rect(300, y, bw, 32, fill=col, rx=6)
        s.text(300 + bw + 12, y + 22, f"{v:,} ns", 15, t["text"], weight="700", mono=True)
    s.text(40, 200, "Median of 10 runs, Windows amd64, Go 1.27.0, i5-13450HX, 2026-10-24. Reproduce with scripts/bench.sh.", 12, t["muted"])
    s.save(name)


def cli(name):
    t = THEMES["dark"]
    s = Svg(900, 300, t, "Terminal output of the sensors example",
            "go run ./examples/sensors prints five ordered readings, one late retry and a counter summary.")
    s.rect(0.5, 0.5, 899, 38, fill="#161b22", stroke=t["border"], rx=12)
    for i, c in enumerate(("#ff5f56", "#ffbd2e", "#27c93f")):
        s.raw(f'<circle cx="{26 + i * 22}" cy="20" r="6" fill="{c}"/>')
    s.text(450, 25, "event-order", 12, t["muted"], "middle")
    lines = [
        ("$ go run ./examples/sensors", t["accent"]),
        ("t+1s  21.3C", t["text"]), ("t+2s  21.4C", t["text"]), ("t+3s  21.5C", t["text"]), ("t+4s  21.7C", t["text"]),
        ("t+2s  21.4C (retry)  <- late", t["warn"]), ("t+9s  22.1C  (flushed)", t["muted"]),
        ("{Buffered:0 Released:5 Late:1 Peak:5 Watermark:2026-01-01 12:00:09 +0000 UTC}", t["muted"]),
    ]
    for i, (txt, col) in enumerate(lines):
        s.text(28, 70 + i * 28, txt.replace("<-", "&lt;-"), 15, col, mono=True)
    s.save(name)


def social_png():
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        print("Pillow missing: social preview skipped")
        return
    fonts = os.path.join(os.environ.get("WINDIR", "C:\\Windows"), "Fonts")
    try:
        f_big = ImageFont.truetype(os.path.join(fonts, "segoeuib.ttf"), 96)
        f_mid = ImageFont.truetype(os.path.join(fonts, "segoeui.ttf"), 38)
        f_mono = ImageFont.truetype(os.path.join(fonts, "consolab.ttf"), 44)
    except OSError:
        print("fonts missing: social preview skipped")
        return
    t = THEMES["dark"]
    img = Image.new("RGB", (1280, 640), t["card"])
    d = ImageDraw.Draw(img)
    d.rounded_rectangle((24, 24, 1255, 615), 28, outline=t["border"], width=2)
    for i, h in enumerate((40, 70, 100)):
        d.rounded_rectangle((90 + i * 44, 110 + 100 - h, 90 + i * 44 + 32, 210), 8, fill=t["accent"])
    d.text((250, 78), "event-order", font=f_big, fill=t["text"])
    d.text((92, 250), "Event-time ordering, safe commit offsets,", font=f_mid, fill=t["muted"])
    d.text((92, 298), "duplicate windows and aligned watermarks", font=f_mid, fill=t["muted"])
    d.text((92, 346), "for Go streams. Zero dependencies.", font=f_mid, fill=t["muted"])
    for row, (y, vals, col) in enumerate(((440, "2 1 4 3 6 5", t["muted"]), (510, "1 2 3 4 5 6", t["good"]))):
        for i, v in enumerate(vals.split()):
            x = 92 + i * 70
            d.rounded_rectangle((x, y, x + 58, y + 46), 10, outline=col, width=3, fill=t["box"])
            d.text((x + 29, y + 23), v, font=f_mono, fill=col, anchor="mm")
    d.text((560, 497), "->  watermark + bounded delay  ->", font=f_mid, fill=t["accent"], anchor="lm")
    os.makedirs(OUT, exist_ok=True)
    img.save(os.path.join(OUT, "social-preview.png"))


def main():
    for tn, t in THEMES.items():
        hero(t, f"hero-{tn}.svg")
        overview(t, f"overview-{tn}.svg")
        a1(t, f"a1-commit-{tn}.svg")
        a2(t, f"a2-dedup-{tn}.svg")
        a3(t, f"a3-aligned-{tn}.svg")
        bench(t, f"bench-{tn}.svg")
    logo_mark()
    cli("cli.svg")
    social_png()
    print("wrote", OUT)


if __name__ == "__main__":
    sys.exit(main())
