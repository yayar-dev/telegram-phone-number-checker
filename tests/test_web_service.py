from telegram_phone_number_checker.web_service import (
    is_valid_phone,
    map_lookup_result,
    parse_phone_batch,
)


def test_parses_unique_normalized_phone_lines():
    assert parse_phone_batch(
        "+7 (999) 123-45-67\n+ +79991234567\n\n+44 7911 123456"
    ) == ["+79991234567", "+447911123456"]


def test_rejects_phone_without_international_prefix():
    assert is_valid_phone("79991234567") is False


def test_maps_username_less_match_without_inventing_link():
    assert map_lookup_result(
        "+79991234567",
        {"id": 123, "username": None, "first_name": "Иван", "last_name": "Иванов"},
    ) == {
        "phone": "+79991234567",
        "status": "FOUND_NO_USERNAME",
        "telegram_id": "123",
        "username": "",
        "name": "Иван Иванов",
        "telegram_link": "",
        "detail": "",
    }


def test_maps_no_contact_response_as_privacy_limited():
    assert map_lookup_result(
        "+79991234567",
        {"error": "No response, the phone number is not on Telegram or has blocked contact adding."},
    )["status"] == "NOT_FOUND_OR_PRIVACY_RESTRICTED"
