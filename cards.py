"""Branded news cards for xqueue: render an Arabic headline card and host it publicly.

Rendering needs Pillow built with libraqm (for Arabic shaping and right-to-left
layout). Hosting pushes the PNG to a public GitHub repo through the contents API,
because Buffer can only attach images it can download from a public URL.
"""
import base64
import datetime
import hashlib
import json
import pathlib
import re
import time
import urllib.error
import urllib.request

from PIL import Image, ImageDraw, ImageFont, features

HERE = pathlib.Path(__file__).resolve().parent
FONTS = HERE / "assets" / "fonts"
LOGO = HERE / "assets" / "logo.png"
WIDTH, HEIGHT = 1200, 675  # 16:9, shown uncropped in the X timeline
MARGIN = 80

BACKGROUND = (12, 18, 32)
TEXT = (245, 247, 250)
MUTED = (148, 163, 184)
ACCENTS = {  # category -> (accent colour, label)
    "tech": ((56, 189, 248), "تقنية"),
    "football": ((34, 197, 94), "كرة القدم"),
}
DEFAULT_ACCENT = ((245, 158, 11), "")
MONTHS = ["يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو",
          "يوليو", "أغسطس", "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر"]


ARABIC = re.compile(r"[\u0600-\u06FF]")
# Scores, times and ranges such as 2-1 or 3:0: without a left-to-right mark the shaper reverses
# them inside Arabic text ("1-2"), unlike X, which shows them as written.
NUMBER_RUN = re.compile(r"\d+(?:\s?[-–—:/.,]\s?\d+)+")


def protect_numbers(text):
    return NUMBER_RUN.sub(lambda m: f"\u200e{m.group(0)}\u200e", text)


def available():
    """True when Pillow can shape Arabic; without libraqm the letters render unjoined and reversed."""
    return features.check("raqm") and (FONTS / "IBMPlexSansArabic-Bold.ttf").exists()


def font(weight, size):
    return ImageFont.truetype(str(FONTS / f"IBMPlexSansArabic-{weight}.ttf"), size,
                              layout_engine=ImageFont.Layout.RAQM)


def width_of(draw, text, fnt):
    return draw.textlength(text, font=fnt, direction="rtl", language="ar")


def wrap(draw, text, fnt, max_width):
    lines, line = [], ""
    for word in text.split():
        candidate = f"{line} {word}".strip()
        if line and width_of(draw, candidate, fnt) > max_width:
            lines.append(line)
            line = word
        else:
            line = candidate
    return lines + [line] if line else lines


def fit_headline(draw, text, max_width, max_height):
    """Largest font size at which the headline fits the text area in at most 4 lines."""
    for size in range(76, 39, -4):
        fnt = font("Bold", size)
        lines = wrap(draw, text, fnt, max_width)
        line_height = int(size * 1.55)
        if len(lines) <= 4 and len(lines) * line_height <= max_height:
            return fnt, lines, line_height
    fnt = font("Bold", 40)
    lines = wrap(draw, text, fnt, max_width)[:4]
    lines[-1] = lines[-1].rstrip("،.") + "…"
    return fnt, lines, int(40 * 1.55)


def render(headline, category, brand, path, day=None):
    """Draw the card to `path` (PNG) and return the path."""
    accent, label = ACCENTS.get((category or "").lower(), DEFAULT_ACCENT)
    headline = protect_numbers(" ".join(headline.split()))
    img = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND)
    draw = ImageDraw.Draw(img)
    right = WIDTH - MARGIN

    # Accent stripe down the right edge, where Arabic reading starts.
    draw.rectangle([WIDTH - 14, 0, WIDTH, HEIGHT], fill=accent)

    # Category pill, top right.
    top = 64
    if label:
        pill_font = font("Medium", 30)
        pill_w = width_of(draw, label, pill_font) + 44
        draw.rounded_rectangle([right - pill_w, top, right, top + 56], radius=28, fill=accent)
        draw.text((right - 22, top + 28), label, font=pill_font, fill=BACKGROUND,
                  anchor="rm", direction="rtl", language="ar")

    # Headline, right-aligned and vertically centred in the middle band.
    area_top, area_bottom = top + 100, HEIGHT - 140
    fnt, lines, line_height = fit_headline(draw, headline, WIDTH - 2 * MARGIN, area_bottom - area_top)
    y = area_top + (area_bottom - area_top - len(lines) * line_height) // 2
    for line in lines:
        draw.text((right, y + line_height // 2), line, font=fnt, fill=TEXT,
                  anchor="rm", direction="rtl", language="ar")
        y += line_height

    # Footer: rule, brand on the right, date on the left.
    draw.line([MARGIN, HEIGHT - 110, right, HEIGHT - 110], fill=(36, 46, 66), width=2)
    small = font("Medium", 28)
    footer_y = HEIGHT - 62
    brand_right = right
    if LOGO.exists():
        logo = Image.open(LOGO).convert("RGBA")
        logo.thumbnail((56, 56))
        img.paste(logo, (right - logo.width, footer_y - logo.height // 2), logo)
        brand_right = right - logo.width - 16
    if brand:
        direction = "rtl" if ARABIC.search(brand) else "ltr"  # keeps "@handle" from becoming "handle@"
        draw.text((brand_right, footer_y), brand, font=small, fill=TEXT, anchor="rm",
                  direction=direction, language="ar" if direction == "rtl" else "en")
    day = day or datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=3))).date()
    draw.text((MARGIN, footer_y), f"{day.day} {MONTHS[day.month - 1]} {day.year}", font=small,
              fill=MUTED, anchor="lm", direction="rtl", language="ar")

    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, optimize=True)
    return path


def upload(path, repo, token, branch="main", user_agent="xqueue",
           api="https://api.github.com", raw="https://raw.githubusercontent.com"):
    """Commit the card to a public GitHub repo and return its raw URL once it is downloadable."""
    data = pathlib.Path(path).read_bytes()
    digest = hashlib.sha1(data).hexdigest()[:12]
    remote = f"cards/{datetime.date.today():%Y-%m}/{datetime.datetime.now():%d%H%M%S}-{digest}.png"
    req = urllib.request.Request(
        f"{api}/repos/{repo}/contents/{remote}",
        method="PUT",
        data=json.dumps({"message": f"Add {remote}", "branch": branch,
                         "content": base64.b64encode(data).decode()}).encode(),
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": user_agent},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        resp.read()
    url = f"{raw}/{repo}/{branch}/{remote}"
    # Buffer reads the image as soon as the post is created, so wait until the file is served.
    for _ in range(10):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, method="HEAD",
                                                               headers={"User-Agent": user_agent}), timeout=20):
                return url
        except (urllib.error.URLError, TimeoutError):
            time.sleep(3)
    raise RuntimeError(f"uploaded card is not reachable yet at {url}")
