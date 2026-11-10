"""
Pure-Python procedural vector rasterizer for the MineRelay brand logo & Windows .ico icon.
Generates:
  - public/logo.png        (256x256 high-DPI RGBA PNG)
  - public/server-icon.png (64x64 Minecraft server list icon)
  - public/logo.ico        (Multi-resolution 16, 24, 32, 48, 64, 128, 256 Windows .ico for PyInstaller)
  - public/favicon.ico     (Browser fallback favicon)
"""

import math
import os
import struct
import zlib

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PUBLIC_DIR = os.path.join(ROOT_DIR, "public")


def clamp(v, lo=0.0, hi=1.0):
    return lo if v < lo else (hi if v > hi else v)


def smoothstep(edge0, edge1, x):
    t = clamp((x - edge0) / (edge1 - edge0) if edge1 != edge0 else 0.0)
    return t * t * (3.0 - 2.0 * t)


def lerp(a, b, t):
    return a + (b - a) * t


def lerp_color(c1, c2, t):
    t = clamp(t)
    return (
        lerp(c1[0], c2[0], t),
        lerp(c1[1], c2[1], t),
        lerp(c1[2], c2[2], t),
    )


def blend_over(dst_rgba, src_rgb, src_a):
    """Alpha blend src over dst in normalized [0..1] space."""
    if src_a <= 0.0:
        return dst_rgba
    dr, dg, db, da = dst_rgba
    sr, sg, sb = src_rgb
    out_a = src_a + da * (1.0 - src_a)
    if out_a <= 1e-6:
        return (0.0, 0.0, 0.0, 0.0)
    out_r = (sr * src_a + dr * da * (1.0 - src_a)) / out_a
    out_g = (sg * src_a + dg * da * (1.0 - src_a)) / out_a
    out_b = (sb * src_a + db * da * (1.0 - src_a)) / out_a
    return (out_r, out_g, out_b, out_a)


def sd_rounded_box(px, py, cx, cy, hw, hh, r):
    dx = abs(px - cx) - (hw - r)
    dy = abs(py - cy) - (hh - r)
    outside = math.hypot(max(dx, 0.0), max(dy, 0.0))
    inside = min(max(dx, dy), 0.0)
    return outside + inside - r


def sd_segment(px, py, ax, ay, bx, by):
    pax, pay = px - ax, py - ay
    bax, bay = bx - ax, by - ay
    denom = bax * bax + bay * bay
    h = clamp((pax * bax + pay * bay) / denom) if denom > 1e-8 else 0.0
    return math.hypot(pax - bax * h, pay - bay * h)


def sd_polygon(px, py, verts):
    """Signed distance to a 2D closed polygon (negative inside, positive outside)."""
    n = len(verts)
    d = float("inf")
    inside = False
    j = n - 1
    for i in range(n):
        vx, vy = verts[i]
        wx, wy = verts[j]
        seg_d = sd_segment(px, py, vx, vy, wx, wy)
        if seg_d < d:
            d = seg_d
        if ((vy > py) != (wy > py)) and (px < (wx - vx) * (py - vy) / (wy - vy + 1e-12) + vx):
            inside = not inside
        j = i
    return -d if inside else d


def sd_tilted_ellipse_arc(px, py, cx, cy, rx, ry, angle_deg, front_half=True):
    """Distance to the front or back half of a tilted ellipse in 128x128 coordinates."""
    ang = math.radians(angle_deg)
    ca, sa = math.cos(ang), math.sin(ang)
    dx, dy = px - cx, py - cy
    # Rotate into ellipse local frame
    lx = dx * ca + dy * sa
    ly = -dx * sa + dy * ca
    # Check if we are in front half (ly >= 0) or back half (ly <= 0)
    if front_half and ly < -2.0:
        # Distance to endpoints
        d1 = math.hypot(lx - rx, ly)
        d2 = math.hypot(lx + rx, ly)
        return min(d1, d2)
    if (not front_half) and ly > 2.0:
        d1 = math.hypot(lx - rx, ly)
        d2 = math.hypot(lx + rx, ly)
        return min(d1, d2)
    # Approximate radial distance to ellipse boundary
    theta = math.atan2(ly / ry, lx / rx)
    if front_half:
        theta = clamp(theta, 0.0, math.pi)
    else:
        if theta > 0:
            theta = math.pi if lx < 0 else 0.0
    ex = rx * math.cos(theta)
    ey = ry * math.sin(theta)
    return math.hypot(lx - ex, ly - ey)


# Predefine polygon vertices in 128x128 space matching public/logo.svg
TOP_FACE = [(64.0, 24.0), (98.0, 44.0), (64.0, 64.0), (30.0, 44.0)]
TOP_INLAY = [(64.0, 32.0), (85.0, 44.0), (64.0, 56.0), (43.0, 44.0)]
TOP_CORE = [(64.0, 37.0), (76.0, 44.0), (64.0, 51.0), (52.0, 44.0)]

LEFT_FACE = [(30.0, 44.0), (64.0, 64.0), (64.0, 103.0), (30.0, 83.0)]
LEFT_SLOT1 = [(38.0, 56.0), (56.0, 66.5), (56.0, 73.5), (38.0, 63.0)]
LEFT_SLOT2 = [(38.0, 69.0), (50.0, 76.0), (50.0, 83.0), (38.0, 76.0)]

RIGHT_FACE = [(64.0, 64.0), (98.0, 44.0), (98.0, 83.0), (64.0, 103.0)]
RIGHT_SLOT1 = [(90.0, 56.0), (72.0, 66.5), (72.0, 73.5), (90.0, 63.0)]
RIGHT_SLOT2 = [(90.0, 69.0), (78.0, 76.0), (78.0, 83.0), (90.0, 76.0)]

CUBE_OUTLINE = [(64.0, 24.0), (98.0, 44.0), (98.0, 83.0), (64.0, 103.0), (30.0, 83.0), (30.0, 44.0)]

# Colors in normalized RGB
C_BG_TOP = (0x11 / 255.0, 0x1A / 255.0, 0x2E / 255.0)
C_BG_BOT = (0x06 / 255.0, 0x09 / 255.0, 0x11 / 255.0)
C_EMERALD_LIGHT = (0xA7 / 255.0, 0xF3 / 255.0, 0xD0 / 255.0)
C_EMERALD_BRIGHT = (0x34 / 255.0, 0xD3 / 255.0, 0x99 / 255.0)
C_EMERALD_MID = (0x10 / 255.0, 0xB9 / 255.0, 0x81 / 255.0)
C_EMERALD_DEEP = (0x05 / 255.0, 0x96 / 255.0, 0x69 / 255.0)
C_EMERALD_DARK = (0x06 / 255.0, 0x4E / 255.0, 0x3B / 255.0)
C_CYAN = (0x38 / 255.0, 0xBD / 255.0, 0xF8 / 255.0)
C_WHITE_MINT = (0xEC / 255.0, 0xFD / 255.0, 0xF5 / 255.0)
C_OBSIDIAN = (0x07 / 255.0, 0x0A / 255.0, 0x12 / 255.0)


def sample_logo_128(x, y, aa_width=1.0):
    """Evaluate the MineRelay vector logo at coordinate (x, y) in [0..128] space."""
    rgba = (0.0, 0.0, 0.0, 0.0)

    # 1. Squircle background badge (center 64,64, half-size 58, corner radius 28)
    d_bg = sd_rounded_box(x, y, 64.0, 64.0, 58.0, 58.0, 28.0)
    bg_alpha = 1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_bg)
    if bg_alpha <= 0.0:
        return (0, 0, 0, 0)

    t_bg = clamp(((x - 12.0) + (y - 8.0)) / 216.0)
    bg_col = lerp_color(C_BG_TOP, C_BG_BOT, t_bg)

    # Ambient radial core glow
    dist_center = math.hypot(x - 64.0, y - 64.0)
    glow_t = clamp(1.0 - dist_center / 50.0)
    bg_col = lerp_color(bg_col, C_EMERALD_MID, (glow_t * glow_t) * 0.34)
    rgba = (bg_col[0], bg_col[1], bg_col[2], bg_alpha)

    # Rim highlight (stroke at d_bg == -1.5, thickness 2.0)
    d_rim = abs(d_bg + 1.5) - 1.0
    rim_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_rim)) * 0.55 * bg_alpha
    rim_col = lerp_color(C_EMERALD_BRIGHT, C_CYAN, clamp((x - 16.0) / 96.0))
    rgba = blend_over(rgba, rim_col, rim_a)

    # 2. Back half of Relay Orbit Ring
    d_back_orbit = sd_tilted_ellipse_arc(x, y, 64.0, 62.0, 48.0, 17.5, -16.0, front_half=False) - 1.4
    back_orbit_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_back_orbit)) * 0.42 * bg_alpha
    if back_orbit_a > 0.0:
        orbit_col = lerp_color(C_CYAN, C_EMERALD_BRIGHT, clamp((x - 16.0) / 96.0))
        rgba = blend_over(rgba, orbit_col, back_orbit_a)

    # 3. Soft drop shadow under Isometric Cube
    d_shadow = sd_polygon(x, y - 4.5, CUBE_OUTLINE)
    if d_shadow < 10.0:
        sh_a = (1.0 - smoothstep(-2.0, 9.0, d_shadow)) * 0.55 * bg_alpha
        rgba = blend_over(rgba, (0.01, 0.02, 0.05), sh_a)

    # 4. Isometric Cube Faces
    # Top Face
    d_top = sd_polygon(x, y, TOP_FACE)
    top_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_top)) * bg_alpha
    if top_a > 0.0:
        t_top = clamp((y - 24.0) / 40.0)
        c_top = lerp_color(C_EMERALD_LIGHT, C_EMERALD_BRIGHT, clamp(t_top * 1.4))
        c_top = lerp_color(c_top, C_EMERALD_MID, clamp((t_top - 0.35) / 0.65))
        rgba = blend_over(rgba, c_top, top_a)

        # Top Inlay
        d_inlay = sd_polygon(x, y, TOP_INLAY)
        inlay_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_inlay)) * 0.36 * top_a
        rgba = blend_over(rgba, C_EMERALD_DARK, inlay_a)

        # Top Crystalline Core
        d_core = sd_polygon(x, y, TOP_CORE)
        core_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_core)) * 0.88 * top_a
        rgba = blend_over(rgba, C_WHITE_MINT, core_a)

    # Left Face
    d_left = sd_polygon(x, y, LEFT_FACE)
    left_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_left)) * bg_alpha
    if left_a > 0.0:
        t_left = clamp((y - 44.0) / 59.0)
        c_left = lerp_color(C_EMERALD_BRIGHT, C_EMERALD_MID, clamp(t_left * 1.6))
        c_left = lerp_color(c_left, C_EMERALD_DEEP, clamp((t_left - 0.3) / 0.7))
        rgba = blend_over(rgba, c_left, left_a)

        d_ls1 = sd_polygon(x, y, LEFT_SLOT1)
        ls1_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_ls1)) * 0.45 * left_a
        rgba = blend_over(rgba, C_EMERALD_DARK, ls1_a)

        d_ls2 = sd_polygon(x, y, LEFT_SLOT2)
        ls2_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_ls2)) * 0.78 * left_a
        rgba = blend_over(rgba, C_EMERALD_LIGHT, ls2_a)

    # Right Face
    d_right = sd_polygon(x, y, RIGHT_FACE)
    right_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_right)) * bg_alpha
    if right_a > 0.0:
        t_right = clamp((y - 44.0) / 59.0)
        c_right = lerp_color(C_EMERALD_MID, C_EMERALD_DEEP, clamp(t_right * 1.4))
        c_right = lerp_color(c_right, C_EMERALD_DARK, clamp((t_right - 0.35) / 0.65))
        rgba = blend_over(rgba, c_right, right_a)

        d_rs1 = sd_polygon(x, y, RIGHT_SLOT1)
        rs1_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_rs1)) * 0.55 * right_a
        rgba = blend_over(rgba, (0.01, 0.17, 0.13), rs1_a)

        d_rs2 = sd_polygon(x, y, RIGHT_SLOT2)
        rs2_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_rs2)) * 0.80 * right_a
        rgba = blend_over(rgba, C_CYAN, rs2_a)

    # Specular Y-Edges on Cube
    d_y1 = sd_segment(x, y, 30.0, 44.0, 64.0, 64.0)
    d_y2 = sd_segment(x, y, 64.0, 64.0, 98.0, 44.0)
    d_y3 = sd_segment(x, y, 64.0, 64.0, 64.0, 103.0)
    d_y = min(d_y1, d_y2, d_y3) - 0.95
    y_edge_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_y)) * 0.85 * bg_alpha
    rgba = blend_over(rgba, C_WHITE_MINT, y_edge_a)

    # 5. Front Half of Relay Orbit Ring
    d_front_orbit = sd_tilted_ellipse_arc(x, y, 64.0, 62.0, 48.0, 17.5, -16.0, front_half=True)
    orbit_stroke = d_front_orbit - 2.1
    front_orbit_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, orbit_stroke)) * bg_alpha
    if front_orbit_a > 0.0:
        orbit_col = lerp_color(C_CYAN, C_EMERALD_BRIGHT, clamp((x - 16.0) / 96.0))
        rgba = blend_over(rgba, orbit_col, front_orbit_a)
        # Inner bright core of front orbit
        core_stroke = d_front_orbit - 0.75
        core_orbit_a = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, core_stroke)) * 0.72 * bg_alpha
        rgba = blend_over(rgba, C_WHITE_MINT, core_orbit_a)

    # 6. Relay Signal Satellite Nodes (Left: 18, 75; Right: 110, 49; Center: 64, 79)
    for nx, ny, outer_r, inner_r, ring_col in (
        (18.0, 75.0, 6.8, 3.2, C_CYAN),
        (110.0, 49.0, 6.8, 3.2, C_EMERALD_BRIGHT),
        (64.0, 79.0, 4.6, 2.3, C_EMERALD_MID),
    ):
        dn = math.hypot(x - nx, y - ny)
        # Outer dark disc + colored ring
        a_outer = (1.0 - smoothstep(outer_r - aa_width * 0.5, outer_r + aa_width * 0.5, dn)) * bg_alpha
        if a_outer > 0.0:
            rgba = blend_over(rgba, C_OBSIDIAN, a_outer)
            d_ring = abs(dn - (outer_r - 1.2)) - 1.25
            a_ring = (1.0 - smoothstep(-aa_width * 0.5, aa_width * 0.5, d_ring)) * bg_alpha
            rgba = blend_over(rgba, ring_col, a_ring)
            a_dot = (1.0 - smoothstep(inner_r - aa_width * 0.5, inner_r + aa_width * 0.5, dn)) * bg_alpha
            rgba = blend_over(rgba, C_WHITE_MINT, a_dot)

    return (
        int(clamp(rgba[0]) * 255.0 + 0.5),
        int(clamp(rgba[1]) * 255.0 + 0.5),
        int(clamp(rgba[2]) * 255.0 + 0.5),
        int(clamp(rgba[3]) * 255.0 + 0.5),
    )


def render_master_buffer(size=256):
    """Render the anti-aliased master image at `size x size`."""
    scale = 128.0 / float(size)
    aa_width = scale * 1.15
    pixels = bytearray(size * size * 4)
    idx = 0
    for py in range(size):
        cy = (py + 0.5) * scale
        for px in range(size):
            cx = (px + 0.5) * scale
            r, g, b, a = sample_logo_128(cx, cy, aa_width)
            pixels[idx] = r
            pixels[idx + 1] = g
            pixels[idx + 2] = b
            pixels[idx + 3] = a
            idx += 4
    return bytes(pixels)


def downsample_box(src_bytes, src_size, dst_size):
    """High-quality area-average box downsampler with premultiplied alpha."""
    if dst_size == src_size:
        return src_bytes
    ratio = src_size // dst_size
    dst = bytearray(dst_size * dst_size * 4)
    d_idx = 0
    for dy in range(dst_size):
        y0 = dy * ratio
        for dx in range(dst_size):
            x0 = dx * ratio
            sum_r = 0.0
            sum_g = 0.0
            sum_b = 0.0
            sum_a = 0.0
            for sy in range(y0, y0 + ratio):
                row_off = (sy * src_size + x0) * 4
                for sx in range(ratio):
                    off = row_off + sx * 4
                    a = src_bytes[off + 3]
                    sum_r += src_bytes[off] * a
                    sum_g += src_bytes[off + 1] * a
                    sum_b += src_bytes[off + 2] * a
                    sum_a += a
            count = ratio * ratio
            avg_a = sum_a / count
            if avg_a > 0.5:
                r = int(clamp(sum_r / sum_a, 0.0, 255.0) + 0.5)
                g = int(clamp(sum_g / sum_a, 0.0, 255.0) + 0.5)
                b = int(clamp(sum_b / sum_a, 0.0, 255.0) + 0.5)
                a = int(clamp(avg_a, 0.0, 255.0) + 0.5)
            else:
                r = g = b = a = 0
            dst[d_idx] = r
            dst[d_idx + 1] = g
            dst[d_idx + 2] = b
            dst[d_idx + 3] = a
            d_idx += 4
    return bytes(dst)


def encode_png(rgba_bytes, width, height):
    """Encode raw RGBA bytes into a standard PNG byte stream."""
    raw_rows = bytearray()
    stride = width * 4
    for y in range(height):
        raw_rows.append(0)  # Filter type 0 (None)
        raw_rows.extend(rgba_bytes[y * stride : (y + 1) * stride])

    def make_chunk(chunk_type, data):
        chunk = struct.pack(">I", len(data)) + chunk_type + data
        crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
        return chunk + struct.pack(">I", crc)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    compressed = zlib.compress(bytes(raw_rows), level=9)
    return (
        b"\x89PNG\r\n\x1a\n"
        + make_chunk(b"IHDR", ihdr)
        + make_chunk(b"IDAT", compressed)
        + make_chunk(b"IEND", b"")
    )


def encode_dib_frame(rgba_bytes, size):
    """Encode raw RGBA bytes into a Windows ICO 32-bit BGRA BITMAPINFOHEADER + XOR + AND mask."""
    # BITMAPINFOHEADER (40 bytes): height is 2 * size (includes XOR + AND mask)
    bmi = struct.pack(
        "<IIIHHIIIIII",
        40,           # biSize
        size,         # biWidth
        size * 2,     # biHeight (bottom-up XOR + AND)
        1,            # biPlanes
        32,           # biBitCount
        0,            # biCompression (BI_RGB)
        size * size * 4,
        0,
        0,
        0,
        0,
    )
    xor_mask = bytearray(size * size * 4)
    # Bottom-up row order, BGRA channel order
    dst_idx = 0
    for y in range(size - 1, -1, -1):
        row_off = y * size * 4
        for x in range(size):
            off = row_off + x * 4
            xor_mask[dst_idx] = rgba_bytes[off + 2]      # B
            xor_mask[dst_idx + 1] = rgba_bytes[off + 1]  # G
            xor_mask[dst_idx + 2] = rgba_bytes[off]      # R
            xor_mask[dst_idx + 3] = rgba_bytes[off + 3]  # A
            dst_idx += 4

    # 1-bit AND mask: rows padded to 32-bit (4-byte) boundary
    and_row_bytes = ((size + 31) // 32) * 4
    and_mask = bytearray(and_row_bytes * size)
    for y in range(size - 1, -1, -1):
        out_row = (size - 1 - y) * and_row_bytes
        row_off = y * size * 4
        for x in range(size):
            alpha = rgba_bytes[row_off + x * 4 + 3]
            if alpha < 8:
                byte_idx = out_row + (x // 8)
                bit_idx = 7 - (x % 8)
                and_mask[byte_idx] |= (1 << bit_idx)

    return bmi + bytes(xor_mask) + bytes(and_mask)


def build_ico(frames_by_size):
    """
    Build a multi-resolution Windows .ico file.
    Sizes <= 64 use 32-bit BGRA DIB for universal Win32/GDI compatibility;
    Sizes >= 128 use PNG payload for compact high-DPI Vista+/Win11 icons.
    """
    sizes = sorted(frames_by_size.keys())
    header = struct.pack("<HHH", 0, 1, len(sizes))
    entries = []
    payloads = []
    offset = 6 + 16 * len(sizes)

    for sz in sizes:
        rgba = frames_by_size[sz]
        if sz >= 256:
            data = encode_png(rgba, sz, sz)
        else:
            data = encode_dib_frame(rgba, sz)
        w_byte = 0 if sz >= 256 else sz
        h_byte = 0 if sz >= 256 else sz
        entry = struct.pack(
            "<BBBBHHII",
            w_byte,
            h_byte,
            0,        # bColorCount
            0,        # bReserved
            1,        # wPlanes
            32,       # wBitCount
            len(data),
            offset,
        )
        entries.append(entry)
        payloads.append(data)
        offset += len(data)

    return header + b"".join(entries) + b"".join(payloads)


def main():
    os.makedirs(PUBLIC_DIR, exist_ok=True)
    print("Rendering 512x512 supersampled master buffer for MineRelay logo...")
    master_512 = render_master_buffer(512)

    sizes = [16, 24, 32, 48, 64, 128, 256]
    frames = {sz: downsample_box(master_512, 512, sz) for sz in sizes}

    # Save 256x256 public/logo.png
    logo_png_path = os.path.join(PUBLIC_DIR, "logo.png")
    with open(logo_png_path, "wb") as f:
        f.write(encode_png(frames[256], 256, 256))
    print(f"  [OK] Wrote {logo_png_path} (256x256 PNG)")

    # Save 64x64 public/server-icon.png (Minecraft Server List Icon)
    server_icon_path = os.path.join(PUBLIC_DIR, "server-icon.png")
    with open(server_icon_path, "wb") as f:
        f.write(encode_png(frames[64], 64, 64))
    print(f"  [OK] Wrote {server_icon_path} (64x64 Minecraft server-icon.png)")

    # Save multi-resolution public/logo.ico & public/favicon.ico
    ico_bytes = build_ico(frames)
    for ico_name in ("logo.ico", "favicon.ico"):
        ico_path = os.path.join(PUBLIC_DIR, ico_name)
        with open(ico_path, "wb") as f:
            f.write(ico_bytes)
        print(f"  [OK] Wrote {ico_path} ({len(sizes)} resolutions: {sizes})")


if __name__ == "__main__":
    main()
