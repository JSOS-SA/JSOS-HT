"""تحليل شامل لسجلات النظام خلال فترة زمنية يحددها المستخدم."""

__all__ = ["run"]

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from collections.abc import Callable

# إعداد مسارات الاستيراد
_PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from common import (
    BOLD,
    CYAN,
    DIM,
    GREEN,
    ORANGE,
    RED,
    RESET,
    ask,
    print_err,
    print_header,
    print_warn,
)

_LOGS_DB_PATH = _PROJECT_DIR / "logs" / "ht_sc_logs.db"
_MAIN_DB_PATH = _PROJECT_DIR / "db" / "ht_sc.db"


def _query_and_print(
    cursor: sqlite3.Cursor,
    title: str,
    db_name: str,
    table_name: str,
    query: str,
    params: tuple,
    format_func: Callable[[sqlite3.Row], None],
) -> None:
    """دالة مساعدة لتنفيذ استعلام وطباعة النتائج بشكل منسق."""
    print(f"\n{BOLD}{CYAN}=== {title} (DB: {db_name}, Table: {table_name}) ==={RESET}")
    try:
        rows = cursor.execute(query, params).fetchall()
        if rows:
            print(f"{ORANGE}تم العثور على {len(rows)} مشكلة:{RESET}")
            for row in rows:
                format_func(row)
        else:
            print(f"{GREEN}لا توجد مشاكل في هذه الفئة.{RESET}")
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            print(f"{DIM}الجدول '{table_name}' غير موجود في {db_name}. سيتم تخطيه.{RESET}")
        else:
            print_err(f"خطأ في قاعدة البيانات عند استعلام {table_name}: {e}")


def run() -> str:
    """الدالة الرئيسية لتشغيل التحليل."""
    print_header("تحليل شامل لسجلات النظام")
    days_str = ask("أدخل عدد الأيام الماضية للتحليل:")
    if not days_str.isdigit() or int(days_str) <= 0:
        print_err("الرجاء إدخال رقم صحيح أكبر من صفر.")
        return "back"

    days = int(days_str)
    since_date = datetime.now() - timedelta(days=days)
    date_str = since_date.strftime("%Y-%m-%d %H:%M:%S")

    print_warn(f"\nتحليل السجلات منذ {date_str}...")

    # --- الجزء الأول: تحليل قاعدة بيانات السجلات ---
    if not _LOGS_DB_PATH.exists():
        print_err(f"قاعدة بيانات السجلات غير موجودة: {_LOGS_DB_PATH}")
    else:
        conn = None
        try:
            conn = sqlite3.connect(_LOGS_DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # 1. الأخطاء والاستثناءات
            def fmt_error(r: sqlite3.Row) -> None:
                print(
                    f"  {DIM}[{r['timestamp']}]{RESET} {RED}{r['error_type']}{RESET} in {CYAN}{r['script_name']}{RESET}: {r['message']}"
                )

            _query_and_print(
                cursor,
                "الأخطاء والاستثناءات",
                "ht_sc_logs.db",
                "errors",
                "SELECT timestamp, script_name, error_type, message FROM errors WHERE timestamp >= ? ORDER BY timestamp DESC",
                (date_str,),
                fmt_error,
            )

            # 2. مشاكل جودة البيانات
            def fmt_quality(r: sqlite3.Row) -> None:
                print(
                    f"  {DIM}[{r['timestamp']}]{RESET} {ORANGE}{r['message']}{RESET} in {CYAN}{r['script_name']}{RESET} (Value: {r['actual_value']})"
                )

            _query_and_print(
                cursor,
                "مشاكل جودة البيانات",
                "ht_sc_logs.db",
                "data_quality",
                "SELECT timestamp, script_name, message, actual_value FROM data_quality WHERE timestamp >= ? ORDER BY timestamp DESC",
                (date_str,),
                fmt_quality,
            )

            # 3. عمليات الملفات الفاشلة
            def fmt_file(r: sqlite3.Row) -> None:
                print(
                    f"  {DIM}[{r['timestamp']}]{RESET} {CYAN}{r['script_name']}{RESET} failed to {RED}{r['operation']}{RESET} '{r['file_path']}'. Reason: {r['error_reason']}"
                )

            _query_and_print(
                cursor,
                "عمليات الملفات الفاشلة",
                "ht_sc_logs.db",
                "file_operations",
                "SELECT timestamp, script_name, operation, file_path, error_reason FROM file_operations WHERE timestamp >= ? AND result != 'success' ORDER BY timestamp DESC",
                (date_str,),
                fmt_file,
            )

            # 4. عمليات الحذف الخطيرة
            def fmt_db(r: sqlite3.Row) -> None:
                print(
                    f"  {DIM}[{r['timestamp']}]{RESET} {CYAN}{r['script_name']}{RESET} performed a delete: {ORANGE}{r['message']}{RESET}"
                )

            _query_and_print(
                cursor,
                "عمليات الحذف من قاعدة البيانات",
                "ht_sc_logs.db",
                "db_operations",
                "SELECT timestamp, script_name, message FROM db_operations WHERE timestamp >= ? AND operation = 'delete' ORDER BY timestamp DESC",
                (date_str,),
                fmt_db,
            )

        except sqlite3.Error as e:
            print_err(f"خطأ في الوصول لقاعدة بيانات السجلات: {e}")
        finally:
            if conn:
                conn.close()

    # --- الجزء الثاني: تحليل مخالفات المراقب ---
    if _MAIN_DB_PATH.exists():
        conn_main = None
        try:
            conn_main = sqlite3.connect(_MAIN_DB_PATH)
            conn_main.row_factory = sqlite3.Row
            cursor_main = conn_main.cursor()

            def fmt_violation(r: sqlite3.Row) -> None:
                print(f"  {DIM}[{r['created_at']}]{RESET} {ORANGE}{r['sender_name']}{RESET}: {r['details']}")

            _query_and_print(
                cursor_main,
                "مخالفات المراقب",
                "ht_sc.db",
                "violations",
                                                "SELECT created_at, sender_name, details FROM violations WHERE created_at >= ? ORDER BY created_at DESC",
                (date_str,),
                fmt_violation,
            )

        except sqlite3.Error as e:
            print_err(f"خطأ في الوصول لقاعدة البيانات الرئيسية: {e}")
        finally:
            if conn_main:
                conn_main.close()

    print()
    return "back"


if __name__ == "__main__":
    run()
