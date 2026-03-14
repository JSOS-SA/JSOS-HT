"""خيار 13: مراقب البيانات — غلاف بايثون لإدارة مراقب واتساب (Node.js)"""

import os
import signal
import subprocess
import sys
from datetime import datetime

# استيراد الدوال المشتركة
from common import (
    BOLD,
    CYAN,
    GREEN,
    ORANGE,
    RED,
    RESET,
    ask,
    print_err,
    print_ok,
    print_warn,
    sync_start_watcher,
    sync_stop_watcher,
)

# مسار مجلد المراقب
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MONITOR_DIR = os.path.join(BASE_DIR, "monitor")
STATE_DIR = os.path.join(MONITOR_DIR, "state")
PID_FILE = os.path.join(STATE_DIR, "monitor.pid")

# مسار القاعدة
DB_PATH = os.path.join(BASE_DIR, "db", "ht_sc.db")


def _check_node() -> bool:
    """تحقق من وجود Node.js على النظام"""
    try:
        result = subprocess.run(
            ["node", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _check_npm_install() -> bool:
    """فحص node_modules وتثبيت الاعتماديات إذا مفقودة"""
    node_modules = os.path.join(MONITOR_DIR, "node_modules")
    if os.path.isdir(node_modules):
        return True

    print_warn("الاعتماديات غير مثبتة — جاري التثبيت...")
    try:
        result = subprocess.run(
            ["npm", "install"],
            cwd=MONITOR_DIR,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            print_ok("تم تثبيت الاعتماديات")
            return True
        print_err("فشل التثبيت: " + result.stderr[:200])
        return False
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        print_err(f"خطأ: {e}")
        return False


def _start_monitor() -> None:
    """تشغيل المراقب كعملية خلفية"""
    # فحص هل يعمل مسبقاً
    if _is_running():
        print_warn("المراقب يعمل مسبقاً")
        return

    # فحص المتطلبات
    if not _check_node():
        print_err("Node.js غير موجود — ثبّته أولاً")
        return

    if not _check_npm_install():
        return

    # إنشاء مجلد الحالة
    os.makedirs(STATE_DIR, exist_ok=True)

    try:
        # تشغيل كعملية منفصلة — لا تتأثر بإغلاق النظام البايثوني
        proc = subprocess.Popen(
            ["node", "index.js"],
            cwd=MONITOR_DIR,
            # لا نلتقط المخرجات — المراقب يكتب في نافذته الخاصة
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )

        # حفظ PID
        with open(PID_FILE, "w") as f:
            f.write(str(proc.pid))

        print_ok(f"المراقب يعمل — المعرف: {proc.pid}")
        print_ok("امسح رمز QR من نافذة المتصفح")

        # مزامنة الإكسل معطّلة مؤقتاً — تسبب قفل الملف أثناء كتابة المراقب
        # if sync_start_watcher(DB_PATH):
        #     print_ok("مزامنة القاعدة تعمل تلقائياً")
        # else:
        #     print_warn("فشل تشغيل مزامنة القاعدة")
    except Exception as e:
        print_err(f"فشل التشغيل: {e}")


def _stop_monitor() -> None:
    """إيقاف المراقب"""
    if not os.path.exists(PID_FILE):
        print_warn("لا يوجد معرف عملية — المراقب متوقف")
        return

    try:
        with open(PID_FILE) as f:
            pid = int(f.read().strip())

        # إرسال إشارة إنهاء
        if sys.platform == "win32":
            # ويندوز: taskkill مع شجرة العمليات
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True,
                timeout=10,
            )
        else:
            os.kill(pid, signal.SIGTERM)

        # حذف ملف PID
        os.remove(PID_FILE)
        # إيقاف مزامنة القاعدة
        sync_stop_watcher()
        print_ok("تم إيقاف المراقب والمزامنة")
    except ProcessLookupError:
        os.remove(PID_FILE)
        sync_stop_watcher()
        print_warn("العملية انتهت مسبقاً")
    except Exception as e:
        print_err(f"خطأ الإيقاف: {e}")


def _is_running() -> bool:
    """فحص هل المراقب يعمل عبر PID"""
    if not os.path.exists(PID_FILE):
        return False

    try:
        with open(PID_FILE) as f:
            pid = int(f.read().strip())

        if sys.platform == "win32":
            # ويندوز: فحص عبر tasklist
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return str(pid) in result.stdout
        # لينكس: إرسال إشارة فارغة
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, ValueError, FileNotFoundError):
        return False


def _get_status() -> None:
    """عرض حالة المراقب"""
    running = _is_running()

    if running:
        with open(PID_FILE) as f:
            pid = f.read().strip()
        print_ok(f"المراقب يعمل — المعرف: {pid}")

        # قراءة إحصائيات من ملف الحالة
        stats_file = os.path.join(STATE_DIR, "stats.txt")
        if os.path.exists(stats_file):
            with open(stats_file) as f:
                print_ok(f.read().strip())

        # وقت التشغيل
        start_file = os.path.join(STATE_DIR, "start_time.txt")
        if os.path.exists(start_file):
            with open(start_file) as f:
                start_ts = int(f.read().strip())
            elapsed = int(datetime.now().timestamp()) - start_ts
            hours = elapsed // 3600
            mins = (elapsed % 3600) // 60
            print_ok(f"مدة التشغيل: {hours} ساعة و {mins} دقيقة")
    else:
        print_warn("المراقب متوقف")


def _get_db_connection():
    """اتصال بالقاعدة للقراءة فقط"""
    import sqlite3

    if not os.path.exists(DB_PATH):
        print_err("القاعدة غير موجودة")
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _show_today_stats() -> None:
    """إحصائيات اليوم من القاعدة"""
    conn = _get_db_connection()
    if not conn:
        return

    try:
        now = datetime.now()
        # صيغة التاريخ: D-MM-YYYY (نفس صيغة المراقب)
        today = f"{now.day}-{now.month:02d}-{now.year}"

        # عدد الرحلات والركاب من المراقب اليوم
        row = conn.execute(
            "SELECT COUNT(*) as trips, COALESCE(SUM(passenger_count),0) as pax "
            "FROM trips WHERE date=? AND source='monitor'",
            (today,),
        ).fetchone()

        print_ok(f"رحلات اليوم: {row['trips']}")
        print_ok(f"ركاب اليوم: {row['pax']}")

        # عدد المخالفات اليوم
        v_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM violations WHERE date=?",
            (today,),
        ).fetchone()
        print_ok(f"مخالفات اليوم: {v_row['cnt']}")

        # توزيع النوبات
        shifts = conn.execute(
            "SELECT shift_code, COUNT(*) as cnt FROM trips "
            "WHERE date=? AND source='monitor' GROUP BY shift_code ORDER BY shift_code",
            (today,),
        ).fetchall()
        if shifts:
            for s in shifts:
                print_ok(f"  نوبة {s['shift_code']}: {s['cnt']} رحلة")
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


def _show_violations() -> None:
    """عرض آخر 20 مخالفة لليوم"""
    conn = _get_db_connection()
    if not conn:
        return

    try:
        now = datetime.now()
        today = f"{now.day}-{now.month:02d}-{now.year}"

        rows = conn.execute(
            "SELECT date, shift_code, sender_name, msg_time, details, employee_count "
            "FROM violations WHERE date=? ORDER BY id DESC LIMIT 20",
            (today,),
        ).fetchall()

        if not rows:
            print_warn("لا توجد مخالفات اليوم")
            return

        print_ok(f"آخر {len(rows)} مخالفة:")
        for r in rows:
            # عرض نثري بدل جدول — لتوافق العربية في الطرفية
            print(
                f"  {ORANGE}{r['sender_name']}{RESET}"
                f" [{r['shift_code']}] {r['msg_time']}"
                f" — {r['details']}"
                f" (العداد: {r['employee_count']})",
            )
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


def _show_submenu() -> None:
    """عرض القائمة الفرعية"""
    print()
    print(f"  {CYAN}{BOLD}مراقب البيانات{RESET}")
    print(f"  {GREEN}1{RESET}  تشغيل المراقب")
    print(f"  {GREEN}2{RESET}  إيقاف المراقب")
    print(f"  {GREEN}3{RESET}  حالة المراقب")
    print(f"  {GREEN}4{RESET}  إحصائيات اليوم")
    print(f"  {GREEN}5{RESET}  عرض المخالفات")
    print(f"  {RED}0{RESET}  رجوع")
    print()


def run() -> str | None:
    """الحلقة الرئيسية — تُرجع 'back' للعودة للقائمة الرئيسية"""
    while True:
        _show_submenu()
        choice = ask("اختر:")

        if choice == "0" or not choice:
            return "back"
        if choice == "1":
            _start_monitor()
        elif choice == "2":
            _stop_monitor()
        elif choice == "3":
            _get_status()
        elif choice == "4":
            _show_today_stats()
        elif choice == "5":
            _show_violations()
        else:
            print_err("اختيار غير صحيح")

        # انتظار ضغطة لمتابعة القائمة
        ask("اضغط Enter للمتابعة...")
