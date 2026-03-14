"""القائمة الرئيسية التفاعلية.

تستدعي السكربتات الفرعية من مجلد scripts
"""

import importlib
import platform
import shutil
import sys
import threading
import time as _time
import traceback
from pathlib import Path

# إضافة مجلد scripts للمسار حتى تعمل الاستيرادات
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from common import *
from logger_config import (
    log_action,
    log_exception,
    log_system,
    setup_logging,
)

# === قاموس السكربتات ===
SCRIPT_MAP = {
    "1": ("process", "معالجة رسائل الواتساب"),
    "2": ("file_manager", "مدير الملفات"),
    "3": ("report_builder_new", "منشئ التقارير"),
    "4": ("db_tool", "قاعدة البيانات"),
    "5": ("monitor_tool", "مراقب البيانات"),
    "6": ("analyze_logs", "تحليل شامل للسجلات"),
    "7": ("monitor_v2_tool", "مراقب v3"),
}

# === المجموعات ===
GROUPS = [
    ("المعالجة", GREEN, [("1", "معالجة رسائل الواتساب")]),
    (
        "الأدوات",
        ORANGE,
        [
            ("2", "مدير الملفات"),
            ("3", "منشئ التقارير"),
            ("4", "قاعدة البيانات"),
            ("5", "مراقب البيانات"),
            ("6", "تحليل شامل للسجلات"),
            ("7", "مراقب v3"),
        ],
    ),
]


def _log_startup():
    """تسجيل معلومات بداية التشغيل في system_events"""
    # كشف المكتبات المتوفرة
    libs = {}
    for lib in ("bidi", "psutil", "tkinter", "openpyxl"):
        try:
            importlib.import_module(lib)
            libs[lib] = True
        except ImportError:
            libs[lib] = False
    libs_str = ", ".join(f"{k}={'OK' if v else 'MISSING'}" for k, v in libs.items())

    # الذاكرة المتاحة
    mem_mb = None
    try:
        import psutil

        mem_mb = round(psutil.virtual_memory().available / (1024 * 1024), 1)
    except (ImportError, OSError):
        pass

    log_system(
        "بداية تشغيل النظام",
        event_type="startup",
        script_name="main",
        python_version=platform.python_version(),
        os_info=platform.platform(),
        is_wt=1 if IS_WT else 0,
        available_memory_mb=mem_mb,
        working_directory=str(Path.cwd()),
        libraries_status=libs_str,
    )


def show_menu():
    """عرض القائمة"""
    clear_screen()

    # سطر الحالة أولاً
    print_status_header()

    # المجموعات
    for title, color, items in GROUPS:
        print_group(title, color, items)

    # إغلاق
    # المحاذاة تتم تلقائياً عبر _auto_print
    print(f"  {RED}{BOLD}0{RESET}  إغلاق")
    print()


def run_script(choice):
    """تشغيل سكربت مع عرض حالة التنفيذ"""
    if choice not in SCRIPT_MAP:
        return False

    module_name, desc = SCRIPT_MAP[choice]

    print_status(STATUS_RUNNING)

    try:
        mod = importlib.import_module(module_name)
        importlib.reload(mod)
        result = mod.run()
        print_status(STATUS_OK)
        # السكربت يُقرر: "back" = رجوع مباشر للقائمة بدون سؤال
        if result == "back":
            return "back"
    except Exception as e:
        print_status(STATUS_FAIL)
        print_err(f"{desc}: {e}")
        print_err(traceback.format_exc())
        # تسجيل الخطأ في قاعدة السجلات
        log_exception(
            f"خطأ في سكربت {desc}",
            exc=e,
            script_name=module_name,
            func_name="run",
            context=f"الخيار {choice}",
        )

    return True


# === مراقب حجم النافذة ===
# يعيد رسم القائمة تلقائياً عند تغيير حجم النافذة
_in_menu = False


def _resize_watcher():
    """خيط خلفي يراقب عرض الطرفية كل نصف ثانية"""
    last_cols = shutil.get_terminal_size().columns
    while True:
        _time.sleep(0.5)
        cols = shutil.get_terminal_size().columns
        if cols != last_cols:
            last_cols = cols
            if _in_menu:
                # تغيّر الحجم أثناء عرض القائمة - أعد الرسم
                show_menu()
                # إعادة طباعة سطر الإدخال بعد إعادة الرسم
                print(f"{GREEN}اختر:{RESET} ", end="", flush=True)


# تشغيل المراقب كخيط خلفي (ينتهي مع البرنامج)
threading.Thread(target=_resize_watcher, daemon=True).start()


def main():
    """الحلقة الرئيسية"""
    global _in_menu

    # تفعيل نظام التسجيل وتسجيل بداية التشغيل
    setup_logging()
    _log_startup()

    try:
        while True:
            show_menu()
            # تسجيل عرض القائمة
            log_action(
                "عرض القائمة الرئيسية",
                action_type="menu_display",
                script_name="main",
                func_name="main",
            )
            _in_menu = True
            choice = ask("اختر:")
            _in_menu = False

            # Enter فارغ = إعادة رسم القائمة بالحجم الجديد
            if not choice:
                continue

            if choice == "0":
                # تسجيل إغلاق النظام
                log_system("إغلاق النظام", event_type="shutdown", script_name="main")
                print_ok("إلى اللقاء!")
                sys.exit(0)

            elif choice in SCRIPT_MAP:
                # تسجيل اختيار صحيح
                _, desc = SCRIPT_MAP[choice]
                log_action(
                    f"اختيار: {desc}",
                    action_type="menu_choice",
                    prompt_text="اختر:",
                    user_response=choice,
                    is_valid=1,
                    script_name="main",
                    func_name="main",
                )
                run_script(choice)

            else:
                # تسجيل اختيار غير صحيح
                log_action(
                    f"اختيار غير صحيح: {choice}",
                    action_type="menu_choice",
                    prompt_text="اختر:",
                    user_response=choice,
                    is_valid=0,
                    script_name="main",
                    func_name="main",
                )
                print_err("اختيار غير صحيح!")

    except KeyboardInterrupt:
        # المستخدم ضغط Ctrl+C
        log_system("إغلاق النظام بـ Ctrl+C", event_type="shutdown", script_name="main")
        print()
        sys.exit(0)

    except Exception as e:
        # استثناء غير متوقع - تسجيل كامل
        log_exception(
            "استثناء غير متوقع في الحلقة الرئيسية",
            exc=e,
            script_name="main",
            func_name="main",
        )
        print_err(f"خطأ غير متوقع: {e}")
        print_err(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
