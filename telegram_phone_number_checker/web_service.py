import re


PHONE_PATTERN = re.compile(r"^\+[1-9]\d{6,14}$")


def parse_phone_batch(raw: str) -> list[str]:
    """Return unique, lightly formatted international phone numbers in input order."""
    phones: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if any(character.isalpha() for character in stripped):
            phone = stripped
        else:
            digits = "".join(character for character in stripped if character.isdigit())
            phone = f"+{digits}" if stripped.startswith("+") else stripped
        if phone not in phones:
            phones.append(phone)
    return phones


def is_valid_phone(phone: str) -> bool:
    return bool(PHONE_PATTERN.fullmatch(phone))


def map_lookup_result(phone: str, result: dict) -> dict[str, str]:
    """Convert the upstream response into a stable row for display and CSV export."""
    error = result.get("error")
    if error:
        status = (
            "NOT_FOUND_OR_PRIVACY_RESTRICTED"
            if error.startswith("No response")
            else "ERROR"
        )
        return {
            "phone": phone,
            "status": status,
            "telegram_id": "",
            "username": "",
            "name": "",
            "telegram_link": "",
            "detail": error,
        }

    username = result.get("username") or ""
    name = " ".join(
        value.strip()
        for value in (result.get("first_name") or "", result.get("last_name") or "")
        if value.strip()
    )
    return {
        "phone": phone,
        "status": "FOUND" if username else "FOUND_NO_USERNAME",
        "telegram_id": str(result.get("id") or ""),
        "username": username,
        "name": name,
        "telegram_link": (
            f"https://t.me/{username}"
            if username
            else f"https://t.me/{phone}?profile"
        ),
        "detail": "",
    }
