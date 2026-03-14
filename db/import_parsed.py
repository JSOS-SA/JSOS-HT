"""
استيراد ملف المعالجة (parsed xlsx) إلى قاعدة البيانات
يقرأ ملف الإكسل الناتج عن process.py ويحفظ الرحلات في جدول trips بمصدر 'parsed'

الاستخدام:
    from db.import_parsed import import_parsed
    stats = import_parsed("path/to/2026-01-01_parsed.xlsx", shift="A")
"""

import os
import sys
import re

_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)
_SCRIPTS_DIR = os.path.join(_PROJECT_DIR, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from common import read_excel_data, FIXED_COLS, TRIP_COLS
from logger_config import log_db_op, log_exception
from db.schema import get_connection, DB_PATH


def _safe_int(val):
    """تحويل آمن لعدد صحيح"""
    if val is None:
        return 0
    s = str(val).strip()
    nums = re.sub(r'[^\d]', '', s)
    return int(nums) if nums else 0


def _safe_str(val):
    """تحويل آمن لنص"""
    if val is None:
        return ""
    return str(val).strip()


def _find_col_index(headers, *targets):
    """البحث عن فهرس عمود بالاسم — يدعم عدة أسماء بديلة
    يبحث بالترتيب: أول تطابق يُعتمد"""
    for target in targets:
        for i, h in enumerate(headers):
            if target.lower() in str(h).lower():
                return i
    return None


def import_parsed(parsed_path, shift=None, db_path=None):
    """
    استيراد ملف إكسل معالج إلى جدول trips بمصدر 'parsed'

    المدخلات:
        parsed_path: مسار ملف الإكسل المعالج
        shift: رمز النوبة (اختياري)
        db_path: مسار القاعدة (اختياري)

    المخرجات:
        قاموس: rows, trips, skipped
    """
    if not os.path.exists(parsed_path):
        raise FileNotFoundError(f"ملف المعالجة غير موجود: {parsed_path}")

    headers, rows = read_excel_data(parsed_path)
    if headers is None or not rows:
        return {"rows": 0, "trips": 0, "skipped": 0}

    # بناء خريطة الأعمدة — الأعمدة الثابتة أولاً
    # يدعم رؤوس عربية (من process.py) وإنجليزية (من preprocess)
    date_idx = _find_col_index(headers, "التاريخ", "Date")
    time_idx = _find_col_index(headers, "الوقت", "Time")
    sender_idx = _find_col_index(headers, "المرسل", "Sender")
    photos_idx = _find_col_index(headers, "الصور", "Images")
    # عمود النوبة — موجود في ملفات preprocess (العمود الثالث عادةً: A/B/C)
    shift_idx = _find_col_index(headers, "النوبة", "Shift")

    # خريطة أسماء الحقول — عربي وإنجليزي
    # المفتاح = اسم الحقل في TRIP_COLS، القيمة = أسماء بديلة للبحث
    _FIELD_ALIASES = {
        "رقم الرحلة":  ["رقم الرحلة"],
        "وقت الاقلاع": ["وقت الاقلاع"],
        "عدد الركاب":  ["عدد الركاب"],
        "الوجهة":      ["الوجهة"],
        "الفيزا":      ["الفيزا"],
        "اسم الحملة":  ["اسم الحملة"],
        "الحالة":      ["الحالة"],
        "التفويج":     ["التفويج"],
        "الكشف":       ["الكشف"],
    }

    # أعمدة الرحلات — قد تتكرر (_2, _3, _4 أو "رحلة 1" "رحلة 2")
    trip_groups = []
    # البحث عن كل عمود يحتوي "رقم الرحلة" كنقطة بداية لمجموعة
    for start_col in range(len(headers)):
        h = str(headers[start_col])
        if "رقم الرحلة" in h:
            group = {}
            # نجمع الأعمدة بالترتيب — كل عمود بعد رقم الرحلة
            for offset, col_name in enumerate(TRIP_COLS):
                check_idx = start_col + offset
                if check_idx < len(headers):
                    check_h = str(headers[check_idx])
                    # تحقق: العمود يحتوي اسم الحقل (مع أو بدون لاحقة _2 _3)
                    for alias in _FIELD_ALIASES.get(col_name, [col_name]):
                        if alias in check_h:
                            group[col_name] = check_idx
                            break
            if group:
                trip_groups.append(group)

    # إذا لم نجد مجموعات، نبحث بشكل مسطح
    if not trip_groups:
        flat = {}
        for col_name in TRIP_COLS:
            aliases = _FIELD_ALIASES.get(col_name, [col_name])
            idx = _find_col_index(headers, *aliases)
            if idx is not None:
                flat[col_name] = idx
        if flat:
            trip_groups = [flat]

    conn = get_connection(db_path)
    cur = conn.cursor()
    stats = {"rows": len(rows), "trips": 0, "skipped": 0}

    try:
        for row in rows:
            # القيم الثابتة
            date_val = _safe_str(row[date_idx]) if date_idx is not None and date_idx < len(row) else ""
            time_val = _safe_str(row[time_idx]) if time_idx is not None and time_idx < len(row) else ""

            # النوبة: من المعامل أو من عمود النوبة في الملف
            row_shift = shift
            if not row_shift and shift_idx is not None and shift_idx < len(row):
                val = _safe_str(row[shift_idx]).upper()
                if val in ("A", "B", "C"):
                    row_shift = val

            # كل مجموعة رحلة في الصف
            row_has_trip = False
            for group in trip_groups:
                flight_idx = group.get("رقم الرحلة")
                if flight_idx is None or flight_idx >= len(row):
                    continue

                flight = _safe_str(row[flight_idx])
                if not flight:
                    continue

                row_has_trip = True

                # استخراج باقي الحقول من المجموعة
                dep_time = _safe_str(row[group["وقت الاقلاع"]]) if "وقت الاقلاع" in group and group["وقت الاقلاع"] < len(row) else ""
                pax = _safe_int(row[group["عدد الركاب"]]) if "عدد الركاب" in group and group["عدد الركاب"] < len(row) else 0
                dest = _safe_str(row[group["الوجهة"]]) if "الوجهة" in group and group["الوجهة"] < len(row) else ""
                visa = _safe_str(row[group["الفيزا"]]) if "الفيزا" in group and group["الفيزا"] < len(row) else ""
                campaign = _safe_str(row[group["اسم الحملة"]]) if "اسم الحملة" in group and group["اسم الحملة"] < len(row) else ""
                status_val = _safe_str(row[group["الحالة"]]) if "الحالة" in group and group["الحالة"] < len(row) else ""
                dispatch_val = _safe_str(row[group["التفويج"]]) if "التفويج" in group and group["التفويج"] < len(row) else ""
                inspection_val = _safe_str(row[group["الكشف"]]) if "الكشف" in group and group["الكشف"] < len(row) else ""

                cur.execute("""
                    INSERT INTO trips (
                        date, shift_code,
                        flight_number, departure_time, passenger_count,
                        destination, visa_type, campaign_name,
                        status, dispatch, inspection, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'parsed')
                """, (
                    date_val, row_shift,
                    flight, dep_time, pax,
                    dest, visa, campaign,
                    status_val, dispatch_val, inspection_val,
                ))
                stats["trips"] += 1

            if not row_has_trip:
                stats["skipped"] += 1

        conn.commit()
        log_db_op(
            f"استيراد معالج: {stats['trips']} رحلة من {stats['rows']} صف",
            operation="insert", query_summary="trips",
            rows_affected=stats["trips"],
            script_name="import_parsed", func_name="import_parsed",
        )

    except Exception as e:
        conn.rollback()
        log_exception("فشل استيراد المعالج", exc=e,
                      script_name="import_parsed", func_name="import_parsed")
        raise
    finally:
        conn.close()

    return stats


if __name__ == "__main__":
    print("هذا الملف يُستورد من السكربتات — للاختبار:")
    print('  from db.import_parsed import import_parsed')
    print('  stats = import_parsed("2026-01-01_parsed.xlsx", shift="A")')
