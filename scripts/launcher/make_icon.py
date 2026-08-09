"""Generate assets/perplab.ico — the desktop icon, from nothing but the stdlib.

A 32x32 and a 16x16 image packed into one .ico: a near-black rounded square carrying a
white "P", matching the in-app brand mark (Chrome.tsx renders the same shape as a div).
Drawn as a pixel map rather than with PIL so the repo needs no imaging dependency for
one static asset. Deterministic: same script, same bytes.
"""

from __future__ import annotations

import struct
from pathlib import Path

BG = (11, 9, 9, 255)  # #09090b in BGRA order (B, G, R, A)
FG = (250, 250, 250, 255)  # #fafafa


def rounded_mask(size: int, radius: int) -> list[list[bool]]:
    """True where the rounded square is opaque."""
    mask = [[True] * size for _ in range(size)]
    corners = [
        (radius - 1, radius - 1, -1, -1),
        (size - radius, radius - 1, +1, -1),
        (radius - 1, size - radius, -1, +1),
        (size - radius, size - radius, +1, +1),
    ]
    for cx, cy, _, _ in corners:
        for y in range(size):
            for x in range(size):
                in_corner_x = (x < radius and cx < radius) or (x >= size - radius and cx >= size - radius)
                in_corner_y = (y < radius and cy < radius) or (y >= size - radius and cy >= size - radius)
                if in_corner_x and in_corner_y:
                    if (x - cx) ** 2 + (y - cy) ** 2 > radius * radius:
                        mask[y][x] = False
    return mask


def glyph_p(size: int) -> set[tuple[int, int]]:
    """The pixels of a blocky 'P', scaled to the canvas."""
    # Layout on a 32-grid, scaled down for 16: stem plus a loop.
    s = size / 32
    px: set[tuple[int, int]] = set()

    def rect(x0: float, y0: float, x1: float, y1: float) -> None:
        for y in range(round(y0 * s), round(y1 * s)):
            for x in range(round(x0 * s), round(x1 * s)):
                if 0 <= x < size and 0 <= y < size:
                    px.add((x, y))

    rect(10, 7, 14, 25)   # stem
    rect(10, 7, 21, 10)   # loop top
    rect(18, 7, 22, 17)   # loop right
    rect(10, 14, 21, 17)  # loop bottom
    return px


def bmp_for(size: int) -> bytes:
    radius = max(3, size // 5)
    mask = rounded_mask(size, radius)
    glyph = glyph_p(size)

    # BITMAPINFOHEADER: height is doubled (XOR + AND masks), rows bottom-up.
    header = struct.pack(
        "<IiiHHIIiiII", 40, size, size * 2, 1, 32, 0, size * size * 4, 0, 0, 0, 0
    )
    xor_rows = []
    for y in range(size - 1, -1, -1):
        row = bytearray()
        for x in range(size):
            if not mask[y][x]:
                row += bytes((0, 0, 0, 0))
            elif (x, y) in glyph:
                row += bytes(FG)
            else:
                row += bytes(BG)
        xor_rows.append(bytes(row))
    # AND mask: all zero (alpha channel carries transparency), padded to 32-bit rows.
    and_row = b"\x00" * (((size + 31) // 32) * 4)
    return header + b"".join(xor_rows) + and_row * size


def main() -> None:
    images = [(32, bmp_for(32)), (16, bmp_for(16))]
    out = Path(__file__).resolve().parents[2] / "assets" / "perplab.ico"
    out.parent.mkdir(parents=True, exist_ok=True)

    header = struct.pack("<HHH", 0, 1, len(images))
    entries = b""
    offset = 6 + 16 * len(images)
    blobs = b""
    for size, blob in images:
        entries += struct.pack(
            "<BBBBHHII", size % 256, size % 256, 0, 0, 1, 32, len(blob), offset
        )
        offset += len(blob)
        blobs += blob
    out.write_bytes(header + entries + blobs)
    print(f"wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
