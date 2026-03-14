"""
استيراد جماعي — يمسح كل مجلدات البيانات ويستورد كل الملفات المعالجة
تشغيل مباشر: python db/bulk_import.py

يبحث في:
  HT_Data_File/2026/MM-2026/YYYY-MM-DD/YYYY-MM-DD_parsed.xlsx
  HT_Data_File/2025/YYYY-MM/YYYY-MM-DD/YYYY-MM-DD_parsed.xlsx
"""

import os
import sys
import time

_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)
_SCRIPTS_DIR = os.path.join(_PROJECT_DIR, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from logger_config import log_db_op, log_exception
from db.schema import init_db, DB_PATH
from db.import_parsed import import_parsed


def find_parsed_files(data_dir):
    """البحث عن كل ملفات _parsed.xlsx في شجرة البيانات"""
    found = []
    for root, dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith("_parsed.xlsx"):
                found.append(os.path.join(root, f))
    # ترتيب زمني حسب اسم الملف
    found.sort()
    return found


def bulk_import(data_dir=None, rebuild=True):
    """
    استيراد جماعي لكل الملفات المعالجة

    المدخلات:
        data_dir: مجلد البيانات (افتراضي: HT_Data_File)
        rebuild: True = حذف القاعدة وإعادة البناء / False = إضافة فقط
    """
    if data_dir is None:
        
        data_dir = os.path.join(_PROJECT_DIR, "HT_Data_File")

    if not os.path.exists(data_dir):
        print(f"مجلد البيانات غير موجود: {data_dir}")
        return

    # إعادة بناء القاعدة إذا مطلوب
    if rebuild and os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"حُذفت القاعدة القديمة")

    conn = init_db()
    conn.close()
    print(f"القاعدة جاهزة: {DB_PATH}")

    # البحث عن الملفات
    files = find_parsed_files(data_dir)
    print(f"وُجد {len(files)} ملف معالج")

    if not files:
        return

    # الاستيراد
    start = time.time()
    total_trips = 0
    total_files = 0
    errors = []

    for i, path in enumerate(files, 1):
        filename = os.path.basename(path)
        try:
            stats = import_parsed(path)
            total_trips += stats["trips"]
            total_files += 1

            # عرض التقدم كل 10 ملفات أو عند الأخير
            if i % 10 == 0 or i == len(files):
                elapsed = time.time() - start
                rate = total_trips / elapsed if elapsed > 0 else 0
                print(f"  [{i}/{len(files)}] {total_trips} trip ({rate:.0f}/s)")

        except Exception as e:
            errors.append((filename, str(e)))

    elapsed = time.time() - start

    # الملخص النهائي
    print(f"\n{'=' * 40}")
    print(f"الملفات: {total_files}/{len(files)}")
    print(f"الرحلات: {total_trips}")
    print(f"الزمن: {elapsed:.1f} ثانية")

    # تسجيل نتيجة الاستيراد الجماعي
    log_db_op(
        f"استيراد جماعي: {total_files}/{len(files)} ملف / {total_trips} رحلة / {elapsed:.1f}ث",
        operation="bulk_insert", query_summary="trips",
        rows_affected=total_trips,
        script_name="bulk_import", func_name="bulk_import",
    )
    if errors:
        print(f"\nأخطاء ({len(errors)}):")
        for name, err in errors:
            print(f"  {name}: {err}")
            log_exception(f"فشل استيراد: {name}", exc=Exception(err),
                          script_name="bulk_import", func_name="bulk_import")

    # حجم القاعدة
    db_size = os.path.getsize(DB_PATH)
    if db_size > 1024 * 1024:
        print(f"حجم القاعدة: {db_size / (1024*1024):.1f} م.ب")
    else:
        print(f"حجم القاعدة: {db_size / 1024:.0f} ك.ب")


if __name__ == "__main__":
    bulk_import()
