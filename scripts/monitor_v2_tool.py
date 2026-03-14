"""خيار 7: مراقب v3 — غلاف بايثون لإدارة مراقب واتساب الجديد (Node.js)"""

import os
import signal
import subprocess
import sys
from datetime import datetime

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
)

# مسار مجلد المراقب الجديد
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MONITOR_DIR = os.path.join(BASE_DIR, "monitor_v2")
STATE_DIR = os.path.join(MONITOR_DIR, "state")
PID_FILE = os.path.join(STATE_DIR, "monitor_v2.pid")
DB_PATH = os.path.join(BASE_DIR, "db", "ht_sc.db")


# ─── أدوات مساعدة ───


def _today_str() -> str:
    """تاريخ اليوم بصيغة المراقب D-MM-YYYY."""
    now = datetime.now()
    return f"{now.day}-{now.month:02d}-{now.year}"


def _get_db():
    """اتصال بالقاعدة للقراءة فقط."""
    import sqlite3

    if not os.path.exists(DB_PATH):
        print_err("القاعدة غير موجودة")
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _check_node() -> bool:
    """تحقق من وجود Node.js."""
    try:
        r = subprocess.run(
            ["node", "--version"],
            capture_output=True, text=True, timeout=10,
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _check_npm_install() -> bool:
    """فحص وتثبيت الاعتماديات."""
    if os.path.isdir(os.path.join(MONITOR_DIR, "node_modules")):
        return True
    print_warn("الاعتماديات غير مثبتة — جاري التثبيت...")
    try:
        r = subprocess.run(
            ["npm", "install"], cwd=MONITOR_DIR,
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode == 0:
            print_ok("تم تثبيت الاعتماديات")
            return True
        print_err("فشل التثبيت: " + r.stderr[:200])
        return False
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        print_err(f"خطأ: {e}")
        return False


def _is_running() -> bool:
    """فحص هل المراقب يعمل."""
    if not os.path.exists(PID_FILE):
        return False
    try:
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        if sys.platform == "win32":
            r = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True, text=True, timeout=5,
            )
            return str(pid) in r.stdout
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, ValueError, FileNotFoundError):
        return False


# ─── 1. تشغيل ───


def _start_monitor() -> None:
    """تشغيل المراقب v3 كعملية خلفية."""
    if _is_running():
        print_warn("المراقب v3 يعمل مسبقاً")
        return
    if not _check_node():
        print_err("Node.js غير موجود — ثبّته أولاً")
        return
    if not _check_npm_install():
        return

    os.makedirs(STATE_DIR, exist_ok=True)
    try:
        proc = subprocess.Popen(
            ["node", "index.js"], cwd=MONITOR_DIR,
            creationflags=(
                subprocess.CREATE_NEW_PROCESS_GROUP
                if sys.platform == "win32" else 0
            ),
        )
        with open(PID_FILE, "w") as f:
            f.write(str(proc.pid))
        print_ok(f"المراقب v3 يعمل — المعرف: {proc.pid}")
        print_ok("امسح رمز QR من نافذة المتصفح")
    except Exception as e:
        print_err(f"فشل التشغيل: {e}")


# ─── 2. إيقاف ───


def _stop_monitor() -> None:
    """إيقاف المراقب v3."""
    if not os.path.exists(PID_FILE):
        print_warn("المراقب v3 متوقف أصلاً")
        return
    try:
        with open(PID_FILE) as f:
            pid = int(f.read().strip())
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, timeout=10,
            )
        else:
            os.kill(pid, signal.SIGTERM)
        os.remove(PID_FILE)
        print_ok("تم إيقاف المراقب v3")
    except ProcessLookupError:
        os.remove(PID_FILE)
        print_warn("العملية انتهت مسبقاً")
    except Exception as e:
        print_err(f"خطأ الإيقاف: {e}")


# ─── 3. حالة المراقب ───


def _get_status() -> None:
    """عرض حالة المراقب مع وقت التشغيل."""
    if not _is_running():
        print_warn("المراقب v3 متوقف")
        return

    with open(PID_FILE) as f:
        pid = f.read().strip()
    print_ok(f"المراقب v3 يعمل — المعرف: {pid}")

    stats_file = os.path.join(STATE_DIR, "stats.txt")
    if os.path.exists(stats_file):
        with open(stats_file) as f:
            print_ok(f.read().strip())

    start_file = os.path.join(STATE_DIR, "start_time.txt")
    if os.path.exists(start_file):
        with open(start_file) as f:
            start_ts = int(f.read().strip())
        elapsed = int(datetime.now().timestamp()) - start_ts
        h, m = elapsed // 3600, (elapsed % 3600) // 60
        print_ok(f"مدة التشغيل: {h} ساعة و {m} دقيقة")


# ─── 4. إحصائيات اليوم ───


def _show_today_stats() -> None:
    """رحلات وركاب ومخالفات اليوم مع توزيع النوبات."""
    conn = _get_db()
    if not conn:
        return
    try:
        today = _today_str()
        row = conn.execute(
            "SELECT COUNT(*) as trips, COALESCE(SUM(passenger_count),0) as pax "
            "FROM trips WHERE date=? AND source='monitor'", (today,),
        ).fetchone()
        print_ok(f"رحلات اليوم: {row['trips']}")
        print_ok(f"ركاب اليوم: {row['pax']}")

        v = conn.execute(
            "SELECT COUNT(*) as cnt FROM violations WHERE date=?", (today,),
        ).fetchone()
        print_ok(f"مخالفات اليوم: {v['cnt']}")

        shifts = conn.execute(
            "SELECT shift_code, COUNT(*) as cnt FROM trips "
            "WHERE date=? AND source='monitor' GROUP BY shift_code ORDER BY shift_code",
            (today,),
        ).fetchall()
        for s in shifts:
            print_ok(f"  نوبة {s['shift_code']}: {s['cnt']} رحلة")
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


# ─── 5. عرض المخالفات ───


def _show_violations() -> None:
    """آخر 20 مخالفة لليوم."""
    conn = _get_db()
    if not conn:
        return
    try:
        today = _today_str()
        rows = conn.execute(
            "SELECT shift_code, sender_name, msg_time, details, employee_count "
            "FROM violations WHERE date=? ORDER BY id DESC LIMIT 20", (today,),
        ).fetchall()
        if not rows:
            print_warn("لا توجد مخالفات اليوم")
            return
        print_ok(f"آخر {len(rows)} مخالفة:")
        for r in rows:
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


# ─── 6. آخر 10 رحلات ───


def _show_last_trips() -> None:
    """عرض آخر 10 رحلات مسجلة مع المرسل والوقت."""
    conn = _get_db()
    if not conn:
        return
    try:
        today = _today_str()
        rows = conn.execute(
            "SELECT t.flight_number, t.departure_time, t.passenger_count, "
            "t.destination, t.shift_code, s.name as sender "
            "FROM trips t "
            "LEFT JOIN messages m ON t.message_id = m.id "
            "LEFT JOIN senders s ON m.sender_id = s.id "
            "WHERE t.date=? AND t.source='monitor' "
            "ORDER BY t.id DESC LIMIT 10", (today,),
        ).fetchall()
        if not rows:
            print_warn("لا توجد رحلات مسجلة اليوم")
            return
        print_ok("آخر 10 رحلات:")
        for i, r in enumerate(rows, 1):
            print(
                f"  {GREEN}{i}.{RESET} {r['flight_number']}"
                f" — {r['departure_time']}"
                f" — {r['passenger_count']} راكب"
                f" — {r['destination']}"
                f" [{r['shift_code']}]"
                f" — {CYAN}{r['sender']}{RESET}",
            )
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


# ─── 7. أكثر المرسلين نشاطاً ───


def _show_top_senders() -> None:
    """ترتيب الموظفين حسب عدد الرسائل اليوم."""
    conn = _get_db()
    if not conn:
        return
    try:
        today = _today_str()
        rows = conn.execute(
            "SELECT s.name, COUNT(*) as cnt "
            "FROM messages m "
            "JOIN senders s ON m.sender_id = s.id "
            "WHERE m.date=? AND m.source_file='monitor' "
            "GROUP BY s.name ORDER BY cnt DESC LIMIT 10", (today,),
        ).fetchall()
        if not rows:
            print_warn("لا توجد رسائل مسجلة اليوم")
            return
        print_ok("أكثر المرسلين نشاطاً اليوم:")
        for i, r in enumerate(rows, 1):
            print(f"  {GREEN}{i}.{RESET} {r['name']} — {r['cnt']} رسالة")
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


# ─── 8. أكثر المخالفين ───


def _show_top_violators() -> None:
    """ترتيب الموظفين حسب عدد المخالفات التراكمي."""
    conn = _get_db()
    if not conn:
        return
    try:
        rows = conn.execute(
            "SELECT sender_name, COUNT(*) as cnt "
            "FROM violations "
            "GROUP BY sender_name ORDER BY cnt DESC LIMIT 15",
        ).fetchall()
        if not rows:
            print_warn("لا توجد مخالفات مسجلة")
            return
        print_ok("أكثر المخالفين (تراكمي):")
        for i, r in enumerate(rows, 1):
            print(f"  {ORANGE}{i}.{RESET} {r['sender_name']} — {r['cnt']} مخالفة")
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


# ─── 9. بحث برقم رحلة ───


def _search_flight() -> None:
    """البحث عن رحلة محددة في سجلات اليوم."""
    flight = ask("رقم الرحلة:")
    if not flight:
        return
    conn = _get_db()
    if not conn:
        return
    try:
        today = _today_str()
        rows = conn.execute(
            "SELECT t.flight_number, t.departure_time, t.passenger_count, "
            "t.destination, t.visa_type, t.status, t.dispatch, "
            "t.shift_code, s.name as sender "
            "FROM trips t "
            "LEFT JOIN messages m ON t.message_id = m.id "
            "LEFT JOIN senders s ON m.sender_id = s.id "
            "WHERE t.date=? AND t.source='monitor' "
            "AND t.flight_number LIKE ?",
            (today, f"%{flight}%"),
        ).fetchall()
        if not rows:
            print_warn(f"لا توجد نتائج لـ {flight} اليوم")
            return
        print_ok(f"عدد النتائج: {len(rows)}")
        for r in rows:
            print(
                f"  {r['flight_number']}"
                f" — {r['departure_time']}"
                f" — {r['passenger_count']} راكب"
                f" — {r['destination']}"
                f" — {r['visa_type']}"
                f" — {r['status']}"
                f" — {r['dispatch']}"
                f" [{r['shift_code']}]"
                f" — {CYAN}{r['sender']}{RESET}",
            )
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


# ─── 10. توزيع الوجهات ───


def _show_destinations() -> None:
    """عدد الرحلات والركاب لكل وجهة اليوم."""
    conn = _get_db()
    if not conn:
        return
    try:
        today = _today_str()
        rows = conn.execute(
            "SELECT destination, COUNT(*) as trips, "
            "COALESCE(SUM(passenger_count),0) as pax "
            "FROM trips WHERE date=? AND source='monitor' "
            "AND destination != '' "
            "GROUP BY destination ORDER BY trips DESC", (today,),
        ).fetchall()
        if not rows:
            print_warn("لا توجد وجهات مسجلة اليوم")
            return
        print_ok("توزيع الوجهات اليوم:")
        for r in rows:
            print(f"  {r['destination']} — {r['trips']} رحلة — {r['pax']} راكب")
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


# ─── 11. توزيع أنواع الفيزا ───


def _show_visa_dist() -> None:
    """عدد الركاب حسب نوع الفيزا اليوم."""
    conn = _get_db()
    if not conn:
        return
    try:
        today = _today_str()
        rows = conn.execute(
            "SELECT visa_type, COUNT(*) as trips, "
            "COALESCE(SUM(passenger_count),0) as pax "
            "FROM trips WHERE date=? AND source='monitor' "
            "AND visa_type != '' "
            "GROUP BY visa_type ORDER BY pax DESC", (today,),
        ).fetchall()
        if not rows:
            print_warn("لا توجد بيانات فيزا اليوم")
            return
        print_ok("توزيع أنواع الفيزا اليوم:")
        for r in rows:
            print(f"  {r['visa_type']} — {r['trips']} رحلة — {r['pax']} راكب")
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


# ─── 12. مقارنة النوبات ───


def _compare_shifts() -> None:
    """مقارنة رحلات وركاب ومخالفات كل نوبة اليوم."""
    conn = _get_db()
    if not conn:
        return
    try:
        today = _today_str()
        # رحلات وركاب
        shifts = conn.execute(
            "SELECT shift_code, COUNT(*) as trips, "
            "COALESCE(SUM(passenger_count),0) as pax "
            "FROM trips WHERE date=? AND source='monitor' "
            "GROUP BY shift_code ORDER BY shift_code", (today,),
        ).fetchall()
        # مخالفات
        v_shifts = conn.execute(
            "SELECT shift_code, COUNT(*) as cnt "
            "FROM violations WHERE date=? "
            "GROUP BY shift_code ORDER BY shift_code", (today,),
        ).fetchall()
        v_map = {r['shift_code']: r['cnt'] for r in v_shifts}

        if not shifts:
            print_warn("لا توجد بيانات اليوم")
            return
        print_ok("مقارنة النوبات اليوم:")
        for s in shifts:
            code = s['shift_code']
            v_cnt = v_map.get(code, 0)
            print(
                f"  نوبة {BOLD}{code}{RESET}"
                f" — {s['trips']} رحلة"
                f" — {s['pax']} راكب"
                f" — {ORANGE}{v_cnt} مخالفة{RESET}",
            )
    except Exception as e:
        print_err(f"خطأ: {e}")
    finally:
        conn.close()


# ─── 13. تنظيف ملفات الحالة ───


def _clean_state() -> None:
    """حذف ملفات الحالة المؤقتة."""
    if not os.path.isdir(STATE_DIR):
        print_warn("مجلد الحالة غير موجود")
        return

    confirm = ask("حذف ملفات الحالة المؤقتة؟ (نعم/لا):")
    if confirm not in ("نعم", "ن", "y", "yes"):
        print_warn("تم الإلغاء")
        return

    # لا نحذف PID إذا المراقب يعمل
    deleted = 0
    kept = 0
    for f in os.listdir(STATE_DIR):
        fp = os.path.join(STATE_DIR, f)
        if not os.path.isfile(fp):
            continue
        # حماية ملف PID أثناء التشغيل
        if f == "monitor_v2.pid" and _is_running():
            kept += 1
            continue
        os.remove(fp)
        deleted += 1

    print_ok(f"تم حذف {deleted} ملف")
    if kept:
        print_warn(f"تم الاحتفاظ بـ {kept} ملف (المراقب يعمل)")


# ─── القائمة الفرعية ───


def _show_submenu() -> None:
    """عرض القائمة الفرعية."""
    print()
    print(f"  {CYAN}{BOLD}مراقب v3{RESET}")
    print()
    print(f"  {GREEN}1{RESET}  تشغيل المراقب")
    print(f"  {GREEN}2{RESET}  إيقاف المراقب")
    print(f"  {GREEN}3{RESET}  حالة المراقب")
    print(f"  {GREEN}4{RESET}  إحصائيات اليوم")
    print(f"  {GREEN}5{RESET}  عرض المخالفات")
    print(f"  {GREEN}6{RESET}  آخر 10 رحلات")
    print(f"  {GREEN}7{RESET}  أكثر المرسلين نشاطاً")
    print(f"  {GREEN}8{RESET}  أكثر المخالفين")
    print(f"  {GREEN}9{RESET}  بحث برقم رحلة")
    print(f"  {GREEN}10{RESET} توزيع الوجهات")
    print(f"  {GREEN}11{RESET} توزيع أنواع الفيزا")
    print(f"  {GREEN}12{RESET} مقارنة النوبات")
    print(f"  {GREEN}13{RESET} تنظيف ملفات الحالة")
    print(f"  {RED}0{RESET}  رجوع")
    print()


_ACTIONS = {
    "1": _start_monitor,
    "2": _stop_monitor,
    "3": _get_status,
    "4": _show_today_stats,
    "5": _show_violations,
    "6": _show_last_trips,
    "7": _show_top_senders,
    "8": _show_top_violators,
    "9": _search_flight,
    "10": _show_destinations,
    "11": _show_visa_dist,
    "12": _compare_shifts,
    "13": _clean_state,
}


def run() -> str | None:
    """الحلقة الرئيسية — تُرجع 'back' للعودة للقائمة الرئيسية."""
    while True:
        _show_submenu()
        choice = ask("اختر:")

        if choice == "0" or not choice:
            return "back"

        action = _ACTIONS.get(choice)
        if action:
            action()
        else:
            print_err("اختيار غير صحيح")

        ask("اضغط Enter للمتابعة...")
