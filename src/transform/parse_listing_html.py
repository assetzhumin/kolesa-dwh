"""Parse listing HTML into structured data."""
import re
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List

PRICE_RE = re.compile(r"([\d\s]+)\s*₸")
MILEAGE_RE = re.compile(r"(\d+(?:\s*\d+)*)\s*(?:км|km)", re.IGNORECASE)


def _clean(s: Optional[str]) -> Optional[str]:
    """
    Clean whitespace from string.
    
    Args:
        s: Input string (may be None)
        
    Returns:
        Cleaned string or None if input was None or empty after cleaning
    """
    if s is None:
        return None
    s = " ".join(s.split())
    return s if s else None


def parse_listing(html: str, url: str, listing_id: int) -> Dict[str, Any]:
    """
    Parse a listing HTML page into structured data.
    Returns a dict with all extracted fields (tolerant to missing fields).
    """
    soup = BeautifulSoup(html, "lxml")
    text_lines = [ln.strip() for ln in soup.get_text("\n").split("\n") if ln.strip()]
    text = "\n".join(text_lines)

    # Title: first H1 typically includes make/model/trim/year
    h1 = soup.find("h1")
    title = _clean(h1.get_text(" ", strip=True)) if h1 else None

    # Price: search for "... ₸"
    price_kzt = None
    m = PRICE_RE.search(text)
    if m:
        try:
            price_kzt = int(m.group(1).replace(" ", "").replace("\u00a0", ""))
        except (ValueError, AttributeError):
            pass

    # Extract key-value specs by label matching (Russian labels observed on page)
    def find_value_after(label: str) -> Optional[str]:
        """Find value after a label in text_lines."""
        try:
            i = text_lines.index(label)
            return _clean(text_lines[i + 1]) if i + 1 < len(text_lines) else None
        except (ValueError, IndexError):
            return None

    city = find_value_after("Город")
    region = find_value_after("Регион") or find_value_after("Область")
    generation = find_value_after("Поколение")
    body_type = find_value_after("Кузов")
    transmission = find_value_after("Коробка передач")
    drivetrain = find_value_after("Привод")
    steering = find_value_after("Руль")
    color = find_value_after("Цвет")
    customs = find_value_after("Растаможен в Казахстане")
    customs_cleared = True if customs == "Да" else False if customs == "Нет" else None

    # Mileage: look for patterns like "50 000 км" or "50000 км"
    mileage_km = None
    mileage_match = MILEAGE_RE.search(text)
    if mileage_match:
        try:
            mileage_str = mileage_match.group(1).replace(" ", "").replace("\u00a0", "")
            mileage_km = int(mileage_str)
        except (ValueError, AttributeError):
            pass
    
    # Also try label-based extraction
    if mileage_km is None:
        mileage_label = find_value_after("Пробег") or find_value_after("Пробег, км")
        if mileage_label:
            mileage_match = MILEAGE_RE.search(mileage_label)
            if mileage_match:
                try:
                    mileage_str = mileage_match.group(1).replace(" ", "").replace("\u00a0", "")
                    mileage_km = int(mileage_str)
                except (ValueError, AttributeError):
                    pass

    engine_raw = find_value_after("Объем двигателя, л")  # e.g. "1.5 (бензин)"
    engine_volume_l = None
    engine_type = None
    if engine_raw:
        # 1.5 (бензин)
        vol_m = re.search(r"(\d+(?:\.\d+)?)", engine_raw)
        if vol_m:
            try:
                engine_volume_l = float(vol_m.group(1))
            except (ValueError, AttributeError):
                pass
        typ_m = re.search(r"\(([^)]+)\)", engine_raw)
        if typ_m:
            engine_type = _clean(typ_m.group(1))

    # Seller name (dealer block)
    seller_name = None
    # on the shared page, dealer name appears as a clickable title line
    # fallback: pick the line after "Проверенный продавец" if present
    if "Проверенный продавец" in text_lines:
        try:
            i = text_lines.index("Проверенный продавец")
            seller_name = _clean(text_lines[i + 1]) if i + 1 < len(text_lines) else None
        except (ValueError, IndexError):
            pass

    # Options/Features: look for "Опции и характеристики" or similar
    options_text = None
    options_list = []
    if "Опции и характеристики" in text_lines:
        try:
            i = text_lines.index("Опции и характеристики")
            # Collect following lines until next section
            opts = []
            for j in range(i + 1, min(i + 20, len(text_lines))):
                line = text_lines[j]
                # Stop if we hit another section header (all caps or common headers)
                if line and (line.isupper() or line in ["Город", "Поколение", "Кузов"]):
                    break
                if line:
                    opts.append(line)
            options_text = _clean("; ".join(opts)) if opts else None
            options_list = opts
        except (ValueError, IndexError):
            pass

    # Photos: collect img srcs (prioritize data-src for lazy loading)
    photos = []
    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src") or img.get("data-lazy-src")
        if src and src.startswith(("http://", "https://", "/")):
            # Normalize relative URLs
            if src.startswith("/"):
                src = f"https://kolesa.kz{src}"
            photos.append(src)
    # Dedupe while preserving order
    seen = set()
    unique_photos = []
    for photo in photos:
        if photo not in seen:
            seen.add(photo)
            unique_photos.append(photo)
    photos = unique_photos

    # Try infer make/model/year from title (best-effort)
    # Example title: "Haval Jolion Tech Plus ... 2025 г."
    make = model = trim = None
    car_year = None
    if title:
        parts = title.split()
        if len(parts) >= 2:
            make = parts[0]
            model = parts[1]
            # Try to extract trim (usually 3rd or 4th word if present)
            if len(parts) >= 3:
                # Skip common words
                skip_words = {"г.", "год", "year"}
                for part in parts[2:]:
                    if part.lower() not in skip_words and not re.match(r"^\d{4}", part):
                        trim = part
                        break
        # Extract year
        y = re.search(r"\b(19\d{2}|20\d{2})\b", title)
        if y:
            try:
                car_year = int(y.group(1))
            except (ValueError, AttributeError):
                pass

    return {
        "listing_id": listing_id,
        "url": url,
        "title": title,
        "price_kzt": price_kzt,
        "city": city,
        "region": region,
        "make": make,
        "model": model,
        "generation": generation,
        "trim": trim,
        "car_year": car_year,
        "mileage_km": mileage_km,
        "body_type": body_type,
        "engine_volume_l": engine_volume_l,
        "engine_type": engine_type,
        "transmission": transmission,
        "drivetrain": drivetrain,
        "steering": steering,
        "color": color,
        "customs_cleared": customs_cleared,
        "seller_name": seller_name,
        "seller_type": "dealer" if seller_name else "private",
        "options_text": options_text,
        "options_list": options_list,
        "photos": photos,
        "photo_count": len(photos),
    }
