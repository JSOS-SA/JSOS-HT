"""
استيراد رسائل الواتساب إلى قاعدة البيانات
يقرأ ملف .txt ويستخرج الرسائل والرحلات والصور ويحفظها في SQLite

الاستخدام:
    from db.import_whatsapp import import_chat
    stats = import_chat("path/to/chat.txt", "1/1/26", "00:00:00", "12/31/26", "23:59:59")
"""

import os
import sys

# إضافة مجلد المشروع للمسار حتى تعمل الاستيرادات
_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)
_SCRIPTS_DIR = os.path.join(_PROJECT_DIR, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from common import (
    parse_messages, extract_trips, extract_photos,
    time_to_sec, HEADER_RE, clean,
)
from logger_config import log_db_op, log_exception
from db.schema import get_connection, DB_PATH


def _detect_shift(time_str):
    """تحديد النوبة من الوقت — نفس منطق get_shift_name في common.py
    لكن يعمل على وقت نصي بدل الوقت الحالي"""
    try:
        secs = time_to_sec(time_str)
    except (ValueError, IndexError):
        return None
    # A صباح 5:45-13:44 / B ظهر 13:45-21:44 / C مساء 21:45-5:44
    morning_start = time_to_sec("05:45:00")    # 20700
    afternoon_start = time_to_sec("13:45:00")  # 49500
    night_start = time_to_sec("21:45:00")      # 78300
    if morning_start <= secs < afternoon_start:
        return "A"
    elif afternoon_start <= secs < night_start:
        return "B"
    else:
        return "C"


def _get_or_create_sender(cur, name):
    """جلب معرف المرسل أو إنشاؤه — يمنع التكرار عبر UNIQUE"""
    cur.execute("SELECT id FROM senders WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO senders (name) VALUES (?)", (name,))
    return cur.lastrowid


def import_chat(chat_path, start_date, start_time, end_date, end_time,
                db_path=None, shift_code=None):
    """
    استيراد ملف واتساب كامل إلى قاعدة البيانات

    المدخلات:
        chat_path: مسار ملف الواتساب النصي
        start_date/start_time: بداية النطاق الزمني (m/d/yy, HH:MM:SS)
        end_date/end_time: نهاية النطاق الزمني
        db_path: مسار قاعدة البيانات (اختياري — يستخدم الافتراضي)
        shift_code: رمز النوبة (اختياري — يُكتشف تلقائياً من الوقت)

    المخرجات:
        قاموس إحصائيات: messages, trips, photos, senders, skipped
    """
    if not os.path.exists(chat_path):
        raise FileNotFoundError(f"ملف الواتساب غير موجود: {chat_path}")

    # استخراج الرسائل بالدوال الموجودة — لا نعيد اختراع العجلة
    messages = parse_messages(chat_path, start_date, start_time, end_date, end_time)
    if not messages:
        return {"messages": 0, "trips": 0, "photos": 0, "senders": 0, "skipped": 0}

    conn = get_connection(db_path)
    cur = conn.cursor()

    stats = {"messages": 0, "trips": 0, "photos": 0, "senders": 0, "skipped": 0}
    # تتبع المرسلين الجدد في هذا الاستيراد
    seen_senders = set()

    try:
        for msg in messages:
            sender_name = msg["sender"]
            date_val = msg["date"]
            time_val = msg["time"]

            # تحديد النوبة — من المعامل أو تلقائياً
            msg_shift = shift_code or _detect_shift(time_val)

            # المرسل
            sender_id = _get_or_create_sender(cur, sender_name)
            if sender_name not in seen_senders:
                seen_senders.add(sender_name)
                stats["senders"] += 1

            # النص الأصلي
            raw_text = "\n".join(msg.get("raw_lines", msg.get("lines", [])))

            # الصور
            photo_list = extract_photos(msg.get("raw_lines", []))
            has_photos = 1 if photo_list else 0

            # إدراج الرسالة
            cur.execute("""
                INSERT INTO messages (date, time, sender_id, raw_text, source_file, shift_code, has_photos)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (date_val, time_val, sender_id, raw_text, chat_path, msg_shift, has_photos))
            message_id = cur.lastrowid
            stats["messages"] += 1

            # الصور
            for photo_name in photo_list:
                cur.execute(
                    "INSERT INTO photos (message_id, filename) VALUES (?, ?)",
                    (message_id, photo_name),
                )
                stats["photos"] += 1

            # استخراج الرحلات
            trips = extract_trips(msg)
            if not trips:
                stats["skipped"] += 1
                continue

            for trip in trips:
                # تحويل عدد الركاب لرقم — الحقل قد يحتوي نص
                pax_raw = trip.get("عدد الركاب", "0")
                try:
                    pax_count = int(str(pax_raw).strip())
                except ValueError:
                    pax_count = 0

                cur.execute("""
                    INSERT INTO trips (
                        message_id, date, shift_code,
                        flight_number, departure_time, passenger_count,
                        destination, visa_type, campaign_name,
                        status, dispatch, inspection, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'whatsapp')
                """, (
                    message_id, date_val, msg_shift,
                    trip.get("رقم الرحلة", ""),
                    trip.get("وقت الاقلاع", ""),
                    pax_count,
                    trip.get("الوجهة", ""),
                    trip.get("الفيزا", ""),
                    trip.get("اسم الحملة", ""),
                    trip.get("الحالة", ""),
                    trip.get("التفويج", ""),
                    trip.get("الكشف", ""),
                ))
                stats["trips"] += 1

        # تسجيل عملية المعالجة
        cur.execute("""
            INSERT INTO processing_runs (
                date, shift_code, input_file, start_stamp, end_stamp,
                total_messages, total_trips, total_photos
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            start_date, shift_code or _detect_shift(start_time),
            chat_path,
            f"{start_date} {start_time}", f"{end_date} {end_time}",
            stats["messages"], stats["trips"], stats["photos"],
        ))

        conn.commit()
        # تسجيل نجاح الاستيراد — عمليات ht_sc.db فقط
        log_db_op(
            f"استيراد واتساب: {stats['messages']} رسالة / {stats['trips']} رحلة",
            operation="insert", query_summary="messages+trips",
            rows_affected=stats["messages"] + stats["trips"],
            script_name="import_whatsapp", func_name="import_chat",
        )

    except Exception as e:
        conn.rollback()
        log_exception("فشل استيراد الواتساب", exc=e,
                      script_name="import_whatsapp", func_name="import_chat")
        raise
    finally:
        conn.close()

    return stats


# === التشغيل المباشر للاختبار ===
if __name__ == "__main__":
    print("هذا الملف يُستورد من السكربتات — للاختبار:")
    print("  from db.import_whatsapp import import_chat")
    print("  stats = import_chat('chat.txt', '1/1/26', '00:00:00', '12/31/26', '23:59:59')")
