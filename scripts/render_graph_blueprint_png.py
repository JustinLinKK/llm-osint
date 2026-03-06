#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import struct
import zlib
from typing import Dict, List, Tuple

Color = Tuple[int, int, int]

PALETTE: Dict[str, Color] = {
    "subject": (215, 38, 61),
    "anchor": (31, 119, 180),
    "context": (44, 160, 44),
    "evidence": (255, 127, 14),
    "risk": (108, 117, 125),
    "text": (22, 28, 36),
    "line": (78, 94, 112),
    "white": (255, 255, 255),
    "black": (0, 0, 0),
    "bg": (247, 248, 251),
    "legend_border": (210, 216, 226),
}

# 5x7 bitmap font for uppercase letters, digits, and a few symbols.
FONT: Dict[str, List[str]] = {
    "A": ["01110", "10001", "10001", "11111", "10001", "10001", "10001"],
    "B": ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
    "C": ["01110", "10001", "10000", "10000", "10000", "10001", "01110"],
    "D": ["11110", "10001", "10001", "10001", "10001", "10001", "11110"],
    "E": ["11111", "10000", "10000", "11110", "10000", "10000", "11111"],
    "F": ["11111", "10000", "10000", "11110", "10000", "10000", "10000"],
    "G": ["01110", "10001", "10000", "10111", "10001", "10001", "01110"],
    "H": ["10001", "10001", "10001", "11111", "10001", "10001", "10001"],
    "I": ["01110", "00100", "00100", "00100", "00100", "00100", "01110"],
    "J": ["00001", "00001", "00001", "00001", "10001", "10001", "01110"],
    "K": ["10001", "10010", "10100", "11000", "10100", "10010", "10001"],
    "L": ["10000", "10000", "10000", "10000", "10000", "10000", "11111"],
    "M": ["10001", "11011", "10101", "10101", "10001", "10001", "10001"],
    "N": ["10001", "10001", "11001", "10101", "10011", "10001", "10001"],
    "O": ["01110", "10001", "10001", "10001", "10001", "10001", "01110"],
    "P": ["11110", "10001", "10001", "11110", "10000", "10000", "10000"],
    "Q": ["01110", "10001", "10001", "10001", "10101", "10010", "01101"],
    "R": ["11110", "10001", "10001", "11110", "10100", "10010", "10001"],
    "S": ["01111", "10000", "10000", "01110", "00001", "00001", "11110"],
    "T": ["11111", "00100", "00100", "00100", "00100", "00100", "00100"],
    "U": ["10001", "10001", "10001", "10001", "10001", "10001", "01110"],
    "V": ["10001", "10001", "10001", "10001", "10001", "01010", "00100"],
    "W": ["10001", "10001", "10001", "10101", "10101", "10101", "01010"],
    "X": ["10001", "10001", "01010", "00100", "01010", "10001", "10001"],
    "Y": ["10001", "10001", "01010", "00100", "00100", "00100", "00100"],
    "Z": ["11111", "00001", "00010", "00100", "01000", "10000", "11111"],
    "0": ["01110", "10001", "10011", "10101", "11001", "10001", "01110"],
    "1": ["00100", "01100", "00100", "00100", "00100", "00100", "01110"],
    "2": ["01110", "10001", "00001", "00010", "00100", "01000", "11111"],
    "3": ["11110", "00001", "00001", "00110", "00001", "00001", "11110"],
    "4": ["00010", "00110", "01010", "10010", "11111", "00010", "00010"],
    "5": ["11111", "10000", "10000", "11110", "00001", "00001", "11110"],
    "6": ["01110", "10000", "10000", "11110", "10001", "10001", "01110"],
    "7": ["11111", "00001", "00010", "00100", "01000", "01000", "01000"],
    "8": ["01110", "10001", "10001", "01110", "10001", "10001", "01110"],
    "9": ["01110", "10001", "10001", "01111", "00001", "00001", "01110"],
    "&": ["01100", "10010", "10100", "01000", "10101", "10010", "01101"],
    "-": ["00000", "00000", "00000", "11111", "00000", "00000", "00000"],
    "_": ["00000", "00000", "00000", "00000", "00000", "00000", "11111"],
    "/": ["00001", "00010", "00100", "01000", "10000", "00000", "00000"],
    ":": ["00000", "00100", "00100", "00000", "00100", "00100", "00000"],
    "(": ["00010", "00100", "01000", "01000", "01000", "00100", "00010"],
    ")": ["01000", "00100", "00010", "00010", "00010", "00100", "01000"],
    " ": ["00000", "00000", "00000", "00000", "00000", "00000", "00000"],
}


def hex_to_rgb(value: str) -> Color:
    value = value.lstrip("#")
    return (int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16))


class Canvas:
    def __init__(self, width: int, height: int, bg: Color):
        self.width = width
        self.height = height
        self.rows = [bytearray(bg * width) for _ in range(height)]

    def set_px(self, x: int, y: int, color: Color):
        if x < 0 or y < 0 or x >= self.width or y >= self.height:
            return
        row = self.rows[y]
        i = x * 3
        row[i:i + 3] = bytes(color)

    def draw_line(self, x1: int, y1: int, x2: int, y2: int, color: Color, thickness: int = 1):
        dx = abs(x2 - x1)
        sx = 1 if x1 < x2 else -1
        dy = -abs(y2 - y1)
        sy = 1 if y1 < y2 else -1
        err = dx + dy
        while True:
            r = max(0, thickness // 2)
            for oy in range(-r, r + 1):
                for ox in range(-r, r + 1):
                    self.set_px(x1 + ox, y1 + oy, color)
            if x1 == x2 and y1 == y2:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy
                x1 += sx
            if e2 <= dx:
                err += dx
                y1 += sy

    def draw_rect(self, x: int, y: int, w: int, h: int, fill: Color, border: Color, border_w: int = 2):
        for yy in range(y, y + h):
            for xx in range(x, x + w):
                self.set_px(xx, yy, fill)
        for i in range(border_w):
            self.draw_line(x + i, y + i, x + w - 1 - i, y + i, border)
            self.draw_line(x + i, y + i, x + i, y + h - 1 - i, border)
            self.draw_line(x + w - 1 - i, y + i, x + w - 1 - i, y + h - 1 - i, border)
            self.draw_line(x + i, y + h - 1 - i, x + w - 1 - i, y + h - 1 - i, border)

    def draw_char(self, x: int, y: int, ch: str, color: Color, scale: int = 2):
        pattern = FONT.get(ch, FONT[" "])
        for row_idx, row in enumerate(pattern):
            for col_idx, bit in enumerate(row):
                if bit == "1":
                    for sy in range(scale):
                        for sx in range(scale):
                            self.set_px(x + col_idx * scale + sx, y + row_idx * scale + sy, color)

    def draw_text(self, x: int, y: int, text: str, color: Color, scale: int = 2, center: bool = False, max_width: int | None = None):
        text = text.upper()
        if max_width and max_width > 0:
            wrapped: List[str] = []
            current = ""
            for token in text.split(" "):
                candidate = token if not current else current + " " + token
                if self.measure_text(candidate, scale) <= max_width:
                    current = candidate
                else:
                    if current:
                        wrapped.append(current)
                    current = token
            if current:
                wrapped.append(current)
        else:
            wrapped = [text]

        line_h = 7 * scale + scale
        for idx, line in enumerate(wrapped):
            tx = x
            if center:
                tx = x - self.measure_text(line, scale) // 2
            ty = y + idx * line_h
            cursor = tx
            for ch in line:
                self.draw_char(cursor, ty, ch, color, scale)
                cursor += 6 * scale

    @staticmethod
    def measure_text(text: str, scale: int = 2) -> int:
        return max(0, len(text) * 6 * scale - scale)

    def save_png(self, path: str):
        raw = bytearray()
        for row in self.rows:
            raw.append(0)
            raw.extend(row)

        def chunk(tag: bytes, data: bytes) -> bytes:
            return (
                struct.pack(">I", len(data))
                + tag
                + data
                + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
            )

        ihdr = struct.pack(">IIBBBBB", self.width, self.height, 8, 2, 0, 0, 0)
        idat = zlib.compress(bytes(raw), 9)

        png = bytearray(b"\x89PNG\r\n\x1a\n")
        png.extend(chunk(b"IHDR", ihdr))
        png.extend(chunk(b"IDAT", idat))
        png.extend(chunk(b"IEND", b""))

        with open(path, "wb") as f:
            f.write(png)


def node_center(node: Dict[str, int]) -> Tuple[int, int]:
    return (int(node["x"] + node["w"] // 2), int(node["y"] + node["h"] // 2))


def edge_points(src: Dict[str, int], dst: Dict[str, int]) -> Tuple[int, int, int, int]:
    sx, sy = node_center(src)
    dx, dy = node_center(dst)
    vx = dx - sx
    vy = dy - sy
    if vx == 0 and vy == 0:
        return sx, sy, dx, dy
    dist = math.hypot(vx, vy)
    ux, uy = vx / dist, vy / dist
    s_half = min(src["w"], src["h"]) / 2.4
    d_half = min(dst["w"], dst["h"]) / 2.4
    x1 = int(round(sx + ux * s_half))
    y1 = int(round(sy + uy * s_half))
    x2 = int(round(dx - ux * d_half))
    y2 = int(round(dy - uy * d_half))
    return x1, y1, x2, y2


def draw_arrow(canvas: Canvas, x1: int, y1: int, x2: int, y2: int, color: Color):
    canvas.draw_line(x1, y1, x2, y2, color, thickness=2)
    angle = math.atan2(y2 - y1, x2 - x1)
    ah = 12
    aw = 7
    x3 = int(x2 - ah * math.cos(angle) + aw * math.sin(angle))
    y3 = int(y2 - ah * math.sin(angle) - aw * math.cos(angle))
    x4 = int(x2 - ah * math.cos(angle) - aw * math.sin(angle))
    y4 = int(y2 - ah * math.sin(angle) + aw * math.cos(angle))
    canvas.draw_line(x2, y2, x3, y3, color, thickness=2)
    canvas.draw_line(x2, y2, x4, y4, color, thickness=2)
    canvas.draw_line(x3, y3, x4, y4, color, thickness=2)


def edge_color(label: str) -> Color:
    label_u = (label or "").upper()
    if "SUPPORTED" in label_u or "DERIVES" in label_u:
        return (205, 111, 18)
    if "RISK" in label_u or "UNCERTAINTY" in label_u:
        return (104, 110, 120)
    if "AFFILIATED" in label_u or "COAUTHOR" in label_u or "CONNECTED" in label_u:
        return (43, 135, 67)
    return PALETTE["line"]


def render(blueprint_path: str, output_path: str):
    with open(blueprint_path, "r", encoding="utf-8") as f:
        bp = json.load(f)

    canvas_cfg = bp.get("canvas", {})
    width = int(canvas_cfg.get("width", 2400))
    height = int(canvas_cfg.get("height", 1500))
    bg = hex_to_rgb(canvas_cfg.get("background", "#f7f8fb"))
    c = Canvas(width, height, bg)

    title = str(bp.get("title", "GRAPH BLUEPRINT"))
    c.draw_text(width // 2, 28, title, PALETTE["text"], scale=3, center=True)

    node_map: Dict[str, Dict[str, int]] = {n["id"]: n for n in bp.get("nodes", [])}

    for e in bp.get("edges", []):
        src = node_map.get(e.get("from"))
        dst = node_map.get(e.get("to"))
        if not src or not dst:
            continue
        x1, y1, x2, y2 = edge_points(src, dst)
        col = edge_color(str(e.get("label", "")))
        draw_arrow(c, x1, y1, x2, y2, col)

        mx = (x1 + x2) // 2
        my = (y1 + y2) // 2
        lbl = str(e.get("label", "")).upper()
        tw = c.measure_text(lbl, scale=1)
        c.draw_rect(mx - tw // 2 - 6, my - 9, tw + 12, 18, PALETTE["white"], (220, 225, 233), border_w=1)
        c.draw_text(mx, my - 6, lbl, PALETTE["text"], scale=1, center=True)

    for n in bp.get("nodes", []):
        kind = str(n.get("kind", "context")).lower()
        fill = PALETTE.get(kind, PALETTE["context"])
        border = tuple(max(0, x - 35) for x in fill)
        x = int(n["x"])
        y = int(n["y"])
        w = int(n["w"])
        h = int(n["h"])
        c.draw_rect(x, y, w, h, fill, border, border_w=3)
        label = str(n.get("label", "")).upper()
        c.draw_text(x + w // 2, y + 18, label, PALETTE["white"], scale=2, center=True, max_width=w - 20)

    # Legend
    legend = bp.get("legend", [])
    lx = 70
    ly = 1110
    lw = 560
    lh = 300
    c.draw_rect(lx, ly, lw, lh, PALETTE["white"], PALETTE["legend_border"], border_w=2)
    c.draw_text(lx + 20, ly + 18, "LEGEND", PALETTE["text"], scale=2)
    yy = ly + 62
    for item in legend[:8]:
        color = hex_to_rgb(item.get("color", "#999999"))
        name = str(item.get("name", "")).upper()
        c.draw_rect(lx + 20, yy, 28, 28, color, (80, 80, 80), border_w=1)
        c.draw_text(lx + 64, yy + 6, name, PALETTE["text"], scale=2)
        yy += 44

    c.save_png(output_path)


def main():
    p = argparse.ArgumentParser(description="Render graph blueprint JSON into PNG without external image libs.")
    p.add_argument("--blueprint", required=True, help="Path to blueprint JSON")
    p.add_argument("--out", required=True, help="Path to output PNG")
    args = p.parse_args()
    render(args.blueprint, args.out)


if __name__ == "__main__":
    main()
