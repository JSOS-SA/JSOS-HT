"""دوال مساعدة مشتركة لطبقة قاعدة البيانات.

تجمع الدوال المكررة بين ملفات db/ في مكان واحد.
"""

import re

from db.constants import MAX_TRIPS, TRIP_FIELDS


def safe_int(val):
    """تحويل آمن لعدد صحيح — يستخرج الأرقام فقط."""
    if val is None:
        return 0
    s = str(val).strip()
    if not s:
        return 0
    nums = re.sub(r"[^\d]", "", s)
    return int(nums) if nums else 0


def safe_str(val):
    """تحويل آمن لنص."""
    if val is None:
        return ""
    return str(val).strip()


def build_records_insert_sql():
    """بناء جملة الإدراج لجدول records بأعمدة الرحلات المرقّمة."""
    fixed = [
        "date",
        "shift_code",
        "sender_name",
        "msg_time",
        "row_num",
        "trip_count",
        "file_id",
    ]
    trip_cols = []
    for n in range(1, MAX_TRIPS + 1):
        for field in TRIP_FIELDS:
            trip_cols.append(f"{field}_{n}")
    all_cols = fixed + trip_cols
    placeholders = ", ".join(["?"] * len(all_cols))
    cols_str = ", ".join(all_cols)
    return f"INSERT INTO records ({cols_str}) VALUES ({placeholders})"


def build_record_2026_insert_sql():
    """بناء جملة الإدراج لجدول record_2026 بأعمدة الرحلات المرقّمة."""
    fixed = [
        "folder_date",
        "filename",
        "shift_code",
        "row_num",
        "sender_name",
        "msg_time",
        "trip_count",
    ]
    trip_cols = []
    for n in range(1, MAX_TRIPS + 1):
        for field in TRIP_FIELDS:
            trip_cols.append(f"{field}_{n}")
    all_cols = fixed + trip_cols
    placeholders = ", ".join(["?"] * len(all_cols))
    cols_str = ", ".join(all_cols)
    return (
        f"INSERT OR IGNORE INTO record_2026 "
        f"({cols_str}) VALUES ({placeholders})"
    )
