"""مدير الملفات التفاعلي.

تنقل حر داخل المجلدات + بحث بفلاتر + فتح ملفات
المسارات تُحفظ تلقائياً في ملف إعدادات
"""

import hashlib
import json
import os

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
    print_ok,
    print_warn,
    read_excel_data,
)
from logger_config import log_action, log_exception, log_file_op

# ملف حفظ المسارات بجوار السكربت
SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".file_manager_paths.json")


def compute_file_hash(filepath):
    """حساب بصمة رقمية للملف - أول 64 كيلوبايت فقط لحماية الذاكرة على 2 جيجا"""
    try:
        hasher = hashlib.sha256()
        with open(filepath, "rb") as f:
            # 64KB كافية للتمييز بين الملفات بدون استهلاك الذاكرة
            chunk = f.read(65536)
            hasher.update(chunk)
        digest = hasher.hexdigest()
        log_file_op(
            f"بصمة: {os.path.basename(filepath)}",
            operation="read",
            file_path=filepath,
            result="success",
            script_name="file_manager",
            func_name="compute_file_hash",
        )
        return digest
    except (OSError, PermissionError) as e:
        log_exception(
            f"فشل حساب البصمة: {filepath}",
            exc=e,
            script_name="file_manager",
            func_name="compute_file_hash",
        )
        return None


def _migrate_old_paths(old_paths):
    """تحويل المسارات القديمة (نصوص) إلى البنية الجديدة (كائنات)"""
    new_paths = []
    for p in old_paths:
        if isinstance(p, str):
            norm = os.path.normpath(p)
            entry = {
                "path": norm,
                "name": os.path.basename(norm),
                "type": "dir" if os.path.isdir(norm) else "file",
                "hash": None,
            }
            # حساب البصمة للملفات فقط - المجلدات لا بصمة لها
            if entry["type"] == "file" and os.path.exists(norm):
                entry["hash"] = compute_file_hash(norm)
            new_paths.append(entry)
        elif isinstance(p, dict):
            # صيغة جديدة مسبقاً - تمر كما هي
            new_paths.append(p)
    return new_paths


# أماكن البحث عند فقدان المسارات - مستوى واحد فقط لحماية الذاكرة
_SEARCH_DIRS = [
    os.path.expanduser("~/Desktop"),
    os.path.expanduser("~/Documents"),
    os.path.expanduser("~/Downloads"),
    # مجلد المشروع الأب (HT_SC)
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
]


def _find_by_hash(target_hash, search_dirs):
    """البحث عن ملف بنفس البصمة - مستوى واحد فقط في كل مجلد"""
    if not target_hash:
        return []
    matches = []
    for search_dir in search_dirs:
        if not os.path.isdir(search_dir):
            continue
        try:
            for name in os.listdir(search_dir):
                fpath = os.path.join(search_dir, name)
                # ملفات فقط - لا ندخل المجلدات الفرعية
                if os.path.isfile(fpath):
                    h = compute_file_hash(fpath)
                    if h == target_hash:
                        matches.append(fpath)
        except PermissionError:
            continue
    return matches


def _find_by_name(target_name, is_file, search_dirs):
    """البحث عن ملف/مجلد بنفس الاسم في الأماكن الشائعة"""
    matches = []
    for search_dir in search_dirs:
        if not os.path.isdir(search_dir):
            continue
        candidate = os.path.join(search_dir, target_name)
        if (is_file and os.path.isfile(candidate)) or (not is_file and os.path.isdir(candidate)):
            matches.append(candidate)
    return matches


def _ask_choose_match(name, matches):
    """عرض خيارات متعددة عند وجود أكثر من نتيجة"""
    print_warn(f"عدة نتائج لـ: {name}")
    for i, m in enumerate(matches, 1):
        print(f"    {CYAN}{i}{RESET}  {m}")
    print(f"    {RED}0{RESET}  تخطي")
    choice = ask("اختر:")
    if choice and choice.isdigit() and 1 <= int(choice) <= len(matches):
        return matches[int(choice) - 1]
    return None


def _ask_manual_resolve(name):
    """سؤال المستخدم المباشر عند عدم العثور على شيء"""
    print_warn(f"لم يتم العثور على: {name}")
    print(f"    {CYAN}1{RESET}  تحديث المسار يدوياً")
    print(f"    {RED}2{RESET}  حذفه من القائمة")
    print(f"    {DIM}0{RESET}  تخطي الآن")
    choice = ask("اختر:")
    if choice == "1":
        new_path = ask("أدخل المسار الجديد:")
        if new_path:
            norm = os.path.normpath(new_path)
            if os.path.exists(norm):
                return norm
            print_err("المسار غير موجود!")
    elif choice == "2":
        # علامة خاصة تعني حذف من القائمة
        return "__DELETE__"
    return None


def _resolve_missing(entry):
    """محاولة إيجاد مسار مفقود بأربع استراتيجيات متدرجة.

    يُرجع: مسار جديد أو __DELETE__ أو None (تخطي)
    """
    name = entry.get("name", "")
    is_file = entry.get("type", "dir") == "file"
    target_hash = entry.get("hash")
    # 1. البحث بالبصمة (للملفات فقط)
    if is_file and target_hash:
        hash_matches = _find_by_hash(target_hash, _SEARCH_DIRS)
        if len(hash_matches) == 1:
            print_ok(f"وُجد بالبصمة: {os.path.basename(hash_matches[0])}")
            return hash_matches[0]
        if len(hash_matches) > 1:
            chosen = _ask_choose_match(name, hash_matches)
            if chosen:
                return chosen
    # 2. البحث بالاسم
    name_matches = _find_by_name(name, is_file, _SEARCH_DIRS)
    if len(name_matches) == 1:
        print_ok(f"وُجد بالاسم: {name_matches[0]}")
        return name_matches[0]
    if len(name_matches) > 1:
        # 3. عرض الخيارات
        chosen = _ask_choose_match(name, name_matches)
        if chosen:
            return chosen
    # 4. سؤال المستخدم مباشرة
    return _ask_manual_resolve(name)


def load_paths():
    """تحميل المسارات المحفوظة مع التكيف الذكي للمسارات المفقودة"""
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("paths", [])
            if not raw:
                return []
            # كشف الصيغة القديمة (قائمة نصوص) وتحويلها تلقائياً
            changed = False
            if isinstance(raw[0], str):
                raw = _migrate_old_paths(raw)
                changed = True  # التحويل يستوجب الحفظ
            # فحص كل مسار والتكيف مع المفقود
            resolved = []
            for entry in raw:
                path = entry.get("path", "")
                if os.path.exists(path):
                    # المسار موجود - يمر كما هو
                    resolved.append(entry)
                else:
                    # مسار مفقود - محاولة الإيجاد
                    new_path = _resolve_missing(entry)
                    if new_path == "__DELETE__":
                        # المستخدم اختار الحذف
                        changed = True
                    elif new_path:
                        # تحديث الكائن بالمسار الجديد
                        entry["path"] = os.path.normpath(new_path)
                        entry["name"] = os.path.basename(new_path)
                        if entry["type"] == "file" and os.path.isfile(new_path):
                            entry["hash"] = compute_file_hash(new_path)
                        resolved.append(entry)
                        changed = True
                    else:
                        # المستخدم تخطى - نبقيه للمحاولة لاحقاً
                        resolved.append(entry)
            # حفظ فوري إذا تغير شيء
            if changed:
                save_paths(resolved)
            return resolved
        except Exception as e:
            # تنبيه المستخدم بدل التجاهل الصامت - ملف الإعدادات قد يكون تالفاً
            print_warn(f"تحذير: تعذر قراءة الإعدادات: {e}")
    return []


def save_paths(paths) -> None:
    """حفظ المسارات في ملف الإعدادات"""
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump({"paths": paths}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print_err(f"حفظ الإعدادات: {e}")


def _open_dialog(mode):
    """فتح نافذة اختيار حسب النوع - تسقط بهدوء إذا غير متوفرة.

    mode: 'file' ملف واحد، 'files' عدة ملفات، 'dir' مجلد
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
    except ImportError:
        # tkinter غير مثبتة - رجوع للإدخال اليدوي
        print_warn("نافذة الاختيار غير متوفرة - أدخل المسار يدوياً")
        path = ask("أدخل المسار:")
        if path:
            norm = os.path.normpath(path)
            if os.path.exists(norm):
                return [norm]
        return []
    root = tk.Tk()
    root.withdraw()
    # النافذة فوق كل النوافذ حتى لا تضيع خلف الطرفية
    root.attributes("-topmost", True)
    result = []
    if mode == "file":
        path = filedialog.askopenfilename(title="اختر ملف")
        if path:
            result = [path]
    elif mode == "files":
        paths = filedialog.askopenfilenames(title="اختر ملفات")
        if paths:
            result = list(paths)
    elif mode == "dir":
        path = filedialog.askdirectory(title="اختر مجلد")
        if path:
            result = [path]
    root.destroy()
    return result


def _add_selected(paths, selected):
    """إضافة المسارات المختارة للقائمة مع فحص التكرار"""
    added = 0
    for raw_path in selected:
        norm = os.path.normpath(raw_path)
        if not os.path.exists(norm):
            continue
        # فحص التكرار بالمسار الكامل
        if any(entry["path"] == norm for entry in paths):
            continue
        # بناء كائن المسار الجديد
        is_file = os.path.isfile(norm)
        entry = {
            "path": norm,
            "name": os.path.basename(norm),
            "type": "file" if is_file else "dir",
            "hash": compute_file_hash(norm) if is_file else None,
        }
        paths.append(entry)
        added += 1
    if added > 0:
        save_paths(paths)
        if added == 1:
            print_ok("تمت الإضافة")
        else:
            print_ok(f"تمت إضافة {added} مسارات")
    else:
        print_warn("لم يُضف شيء جديد")
    return paths


def add_path(paths):
    """إضافة مسار جديد عبر نافذة اختيار أو إدخال يدوي"""
    print(f"    {CYAN}1{RESET}  اختيار ملف واحد")
    print(f"    {CYAN}2{RESET}  اختيار عدة ملفات")
    print(f"    {CYAN}3{RESET}  اختيار مجلد")
    print(f"    {RED}0{RESET}  إلغاء")
    choice = ask("اختر:")
    if not choice or choice == "0":
        return paths
    # فتح النافذة حسب الاختيار
    modes = {"1": "file", "2": "files", "3": "dir"}
    mode = modes.get(choice)
    if not mode:
        print_err("اختيار غير صحيح!")
        return paths
    selected = _open_dialog(mode)
    if not selected:
        print_warn("لم يتم اختيار شيء")
        return paths
    return _add_selected(paths, selected)


def choose_root(paths):
    """اختيار المسار الجذري من القائمة المحفوظة"""
    if not paths:
        print_warn("لا توجد مسارات محفوظة")
        paths = add_path(paths)
        if not paths:
            return None, paths
        return paths[0]["path"], paths

    if len(paths) == 1:
        # مسار واحد فقط - استخدمه مباشرة
        return paths[0]["path"], paths

    # عرض المسارات المتاحة
    print_header("المسارات المحفوظة")
    for i, entry in enumerate(paths, 1):
        etype = entry.get("type", "dir")
        name = entry.get("name", "")
        path = entry.get("path", "")
        if etype == "file":
            # ملف: رقم أخضر + اسم + حجم
            try:
                size = format_size(os.path.getsize(path))
            except OSError:
                size = "?"
            print(f"    {GREEN}{i}{RESET}  {name}  {DIM}{size}{RESET}")
        else:
            # مجلد: رقم سماوي + اسم
            print(f"    {CYAN}{i}{RESET}  {name}/")
        # المسار الكامل خافت في السطر التالي
        print(f"       {DIM}{path}{RESET}")
    print(f"    {GREEN}+{RESET}  إضافة مسار جديد")
    print()

    choice = ask("اختر رقم المسار:")
    if choice == "+":
        paths = add_path(paths)
        if paths:
            return paths[-1]["path"], paths
        return None, paths

    if choice.isdigit() and 1 <= int(choice) <= len(paths):
        return paths[int(choice) - 1]["path"], paths

    return None, paths


def format_size(size_bytes) -> str:
    """تحويل الحجم من بايت إلى نص مقروء"""
    if size_bytes < 1024:
        return f"{size_bytes} ب"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes // 1024} ك.ب"
    return f"{size_bytes // (1024 * 1024)} م.ب"


def get_file_type(filename):
    """تحديد نوع الملف حسب الامتداد"""
    ext = os.path.splitext(filename)[1].lower()
    types = {
        ".xlsx": "إكسل",
        ".xls": "إكسل",
        ".docx": "وورد",
        ".doc": "وورد",
        ".pdf": "بي دي إف",
        ".jpg": "صورة",
        ".jpeg": "صورة",
        ".png": "صورة",
        ".txt": "نص",
        ".csv": "نص",
    }
    return types.get(ext, ext or "ملف")


def browse(path) -> None:
    """تصفح المجلدات والملفات تفاعلياً بشجرة متصلة.

    يسمح بالدخول والخروج بحرية حتى يختار المستخدم الرجوع
    """
    while True:
        if not os.path.exists(path):
            print_err("المسار غير موجود!")
            return

        # جمع المحتوى
        try:
            entries = sorted(os.listdir(path))
        except PermissionError:
            print_err("لا توجد صلاحية للوصول!")
            log_file_op(
                "فشل الوصول للمجلد",
                operation="listdir",
                file_path=path,
                result="fail",
                error_reason="PermissionError",
                script_name="file_manager",
                func_name="browse",
            )
            return

        dirs = [e for e in entries if os.path.isdir(os.path.join(path, e))]
        files = [e for e in entries if os.path.isfile(os.path.join(path, e))]
        log_file_op(
            f"مسح مجلد: {len(dirs)} مجلد + {len(files)} ملف",
            operation="listdir",
            file_path=path,
            result="success",
            script_name="file_manager",
            func_name="browse",
        )

        # عرض المسار الحالي كجذر الشجرة
        print(f"\n{BOLD}{os.path.basename(path)}/{RESET}")

        # ترقيم موحد: المجلدات أولاً ثم الملفات
        items = []
        all_entries = []
        for d in dirs:
            all_entries.append(("dir", d))
        for f in files:
            all_entries.append(("file", f))

        for i, (etype, name) in enumerate(all_entries):
            idx = i + 1
            is_last = i == len(all_entries) - 1
            # أحرف الشجرة المتصلة
            connector = "└── " if is_last else "├── "

            if etype == "dir":
                items.append(("dir", os.path.join(path, name), name))
                print(f"{connector}{CYAN}{idx}{RESET} {name}/")
            else:
                fpath = os.path.join(path, name)
                try:
                    size = format_size(os.path.getsize(fpath))
                except OSError:
                    size = "?"
                items.append(("file", fpath, name))
                print(f"{connector}{GREEN}{idx}{RESET} {name}  {DIM}{size}{RESET}")

        if not items:
            print("└── (فارغ)")

        print()

        # خيارات التنقل
        choice = ask("رقم للدخول/الفتح، 0 للرجوع:")

        if not choice or choice == "0":
            return

        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(items):
                item_type, item_path, _item_name = items[idx - 1]
                if item_type == "dir":
                    browse(item_path)
                else:
                    open_file(item_path)
            else:
                print_err("رقم غير صحيح!")
        else:
            print_err("أدخل رقماً صحيحاً")


def open_file(fpath) -> None:
    """فتح ملف بالبرنامج الافتراضي أو عرض محتواه"""
    if not os.path.exists(fpath):
        print_err("الملف غير موجود!")
        return

    fname = os.path.basename(fpath)
    ext = os.path.splitext(fname)[1].lower()

    print(f"\n  {BOLD}{fname}{RESET}")
    print(f"  {DIM}{format_size(os.path.getsize(fpath))} - {get_file_type(fname)}{RESET}\n")

    # خيارات حسب نوع الملف
    options = [("1", "فتح بالبرنامج الافتراضي")]

    if ext in (".xlsx", ".xls"):
        options.append(("2", "عرض محتوى الإكسل"))
        options.append(("3", "بحث بفلتر في الأعمدة"))
    elif ext in (".txt", ".csv"):
        options.append(("2", "عرض المحتوى"))
    elif ext in (".docx",):
        options.append(("2", "عرض محتوى الوورد"))

    options.append(("0", "رجوع"))

    for num, desc in options:
        color = RED if num == "0" else CYAN
        print(f"    {color}{num}{RESET}  {desc}")

    choice = ask("\nاختر:")

    if choice == "1":
        try:
            os.startfile(fpath)
            print_ok("تم فتح الملف")
            log_file_op(
                f"فتح ملف: {fname}",
                operation="open",
                file_path=fpath,
                result="success",
                script_name="file_manager",
                func_name="open_file",
            )
        except AttributeError:
            import subprocess

            subprocess.Popen(["xdg-open", fpath])
            print_ok("تم فتح الملف")
        except Exception as e:
            print_err(f"فشل الفتح: {e}")
            log_exception(
                f"فشل فتح الملف: {fname}",
                exc=e,
                script_name="file_manager",
                func_name="open_file",
            )

    elif choice == "2" and ext in (".xlsx", ".xls"):
        show_excel_content(fpath)

    elif choice == "3" and ext in (".xlsx", ".xls"):
        filter_excel(fpath)

    elif choice == "2" and ext in (".txt", ".csv"):
        show_text_content(fpath)

    elif choice == "2" and ext in (".docx",):
        show_docx_content(fpath)


def show_excel_content(fpath) -> None:
    """عرض محتوى ملف إكسل بشكل منظم"""
    headers, rows = read_excel_data(fpath)
    if headers is None:
        return

    total = len(rows)
    print(f"\n  {BOLD}{total}{RESET} صف، {BOLD}{len(headers)}{RESET} عمود\n")

    # عرض أسماء الأعمدة
    print(f"  {CYAN}الأعمدة:{RESET}")
    for i, h in enumerate(headers, 1):
        print(f"    {i}. {h}")

    # عرض أول 10 صفوف
    print(f"\n  {CYAN}أول {min(10, total)} صفوف:{RESET}\n")
    for row_idx, row in enumerate(rows[:10]):
        print(f"  {DIM}صف {row_idx + 1}:{RESET}")
        for col_idx, val in enumerate(row):
            if col_idx < len(headers) and val is not None:
                val_str = str(val).strip()
                if val_str:
                    # عمود: قيمة - كل زوج في سطره
                    print(f"    {headers[col_idx]}: {val_str}")
        print()

    if total > 10:
        print(f"  {DIM}... و {total - 10} صفوف أخرى{RESET}")


def filter_excel(fpath) -> None:
    """بحث بفلتر في أعمدة الإكسل"""
    headers, rows = read_excel_data(fpath)
    if headers is None:
        return

    while True:
        # عرض الأعمدة المتاحة
        print(f"\n  {CYAN}الأعمدة المتاحة:{RESET}")
        for i, h in enumerate(headers, 1):
            print(f"    {CYAN}{i}{RESET}  {h}")
        print(f"    {RED}0{RESET}  رجوع")

        col_choice = ask("\nاختر رقم العمود:")
        if not col_choice or col_choice == "0":
            return

        if not col_choice.isdigit() or not (1 <= int(col_choice) <= len(headers)):
            print_err("رقم غير صحيح!")
            continue

        col_idx = int(col_choice) - 1
        col_name = headers[col_idx]

        # جمع القيم الفريدة في هذا العمود
        unique_vals = {}
        for row in rows:
            if col_idx < len(row) and row[col_idx] is not None:
                val = str(row[col_idx]).strip()
                if val:
                    unique_vals[val] = unique_vals.get(val, 0) + 1

        if not unique_vals:
            print_warn("العمود فارغ")
            continue

        # ترتيب القيم حسب العدد
        sorted_vals = sorted(unique_vals.items(), key=lambda x: -x[1])

        print(f"\n  {BOLD}{col_name}{RESET}  ({len(unique_vals)} قيمة فريدة)\n")

        # عرض القيم مع عددها
        display_vals = sorted_vals[:30]  # حد 30 قيمة للعرض
        for i, (val, count) in enumerate(display_vals, 1):
            print(f"    {GREEN}{i}{RESET}  {val}  {DIM}({count}){RESET}")

        if len(sorted_vals) > 30:
            print(f"    {DIM}... و {len(sorted_vals) - 30} قيمة أخرى{RESET}")

        print(f"\n    {ORANGE}*{RESET}  بحث نصي في العمود")
        print(f"    {RED}0{RESET}  رجوع للأعمدة")

        val_choice = ask("\nاختر رقم القيمة أو * للبحث:")
        if not val_choice or val_choice == "0":
            continue

        # تحديد الفلتر
        filter_val = None
        search_mode = False

        if val_choice == "*":
            filter_val = ask("اكتب نص البحث:")
            if not filter_val:
                continue
            search_mode = True
        elif val_choice.isdigit() and 1 <= int(val_choice) <= len(display_vals):
            filter_val = display_vals[int(val_choice) - 1][0]
        else:
            print_err("اختيار غير صحيح!")
            continue

        # عرض الصفوف المطابقة
        matches = []
        for row in rows:
            if col_idx < len(row) and row[col_idx] is not None:
                val = str(row[col_idx]).strip()
                if search_mode:
                    if filter_val in val:
                        matches.append(row)
                elif val == filter_val:
                    matches.append(row)

        if not matches:
            print_warn("لا توجد نتائج")
            continue

        print(f"\n  {BOLD}{len(matches)}{RESET} نتيجة\n")

        # عرض أول 20 نتيجة
        for row_idx, row in enumerate(matches[:20]):
            print(f"  {DIM}نتيجة {row_idx + 1}:{RESET}")
            for ci, val in enumerate(row):
                if ci < len(headers) and val is not None:
                    val_str = str(val).strip()
                    if val_str:
                        # تمييز العمود المُفلتر
                        if ci == col_idx:
                            print(f"    {GREEN}{headers[ci]}: {val_str}{RESET}")
                        else:
                            print(f"    {headers[ci]}: {val_str}")
            print()

        if len(matches) > 20:
            print(f"  {DIM}... و {len(matches) - 20} نتيجة أخرى{RESET}")


def show_text_content(fpath) -> None:
    """عرض محتوى ملف نصي"""
    try:
        with open(fpath, encoding="utf-8") as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        # محاولة بترميز آخر
        with open(fpath, encoding="cp1256") as f:
            lines = f.readlines()

    total = len(lines)
    print(f"\n  {total} سطر\n")

    for i, line in enumerate(lines[:30]):
        print(f"  {DIM}{i + 1:3}{RESET}  {line.rstrip()}")

    if total > 30:
        print(f"\n  {DIM}... و {total - 30} سطر آخر{RESET}")


def show_docx_content(fpath) -> None:
    """عرض محتوى ملف وورد"""
    try:
        import docx
    except ImportError:
        print_err("python-docx غير مثبتة")
        return

    try:
        doc = docx.Document(fpath)
        paras = [p.text for p in doc.paragraphs if p.text.strip()]
        del doc  # تحرير الذاكرة

        print(f"\n  {len(paras)} فقرة\n")
        for _i, text in enumerate(paras[:30]):
            print(f"  {text}")

        if len(paras) > 30:
            print(f"\n  {DIM}... و {len(paras) - 30} فقرة أخرى{RESET}")
    except Exception as e:
        print_err(f"قراءة الوورد: {e}")


def search_files(root_path) -> None:
    """بحث في أسماء الملفات مع إمكانية الفتح"""
    query = ask("كلمة البحث:")
    if not query:
        return

    results = []
    for root, _dirs, files in os.walk(root_path):
        for f in files:
            if query.lower() in f.lower():
                results.append(os.path.join(root, f))
        # حماية: حد 500 نتيجة
        if len(results) >= 500:
            break

    if not results:
        print_warn("لم يُعثر على نتائج")
        log_file_op(
            f"بحث بدون نتائج: {query}",
            operation="listdir",
            file_path=root_path,
            result="success",
            script_name="file_manager",
            func_name="search_files",
        )
        return

    log_file_op(
        f"بحث '{query}': {len(results)} نتيجة",
        operation="listdir",
        file_path=root_path,
        result="success",
        script_name="file_manager",
        func_name="search_files",
    )
    # عرض النتائج مع إمكانية الفتح
    while True:
        print(f"\n  {BOLD}{len(results)}{RESET} نتيجة\n")

        display = results[:20]
        for i, fpath in enumerate(display, 1):
            rel = os.path.relpath(fpath, root_path)
            try:
                size = format_size(os.path.getsize(fpath))
            except OSError:
                size = "؟"
            print(f"    {GREEN}{i}{RESET}  {rel}  {DIM}{size}{RESET}")

        if len(results) > 20:
            print(f"\n    {DIM}... و {len(results) - 20} نتيجة أخرى{RESET}")

        choice = ask("\nرقم لفتح الملف، 0 للرجوع:")
        if not choice or choice == "0":
            return

        if choice.isdigit() and 1 <= int(choice) <= len(display):
            open_file(display[int(choice) - 1])
        else:
            print_err("رقم غير صحيح!")


def browse_all(paths) -> None:
    """تصفح محتويات جميع المجلدات المحفوظة في عرض موحد"""
    # جمع المجلدات فقط - الملفات لا تُتصفح
    dir_paths = [e for e in paths if e.get("type") == "dir" and os.path.isdir(e.get("path", ""))]
    if not dir_paths:
        print_warn("لا توجد مجلدات للتصفح")
        return
    # مجلد واحد فقط - تصفح مباشر بدون تجميع
    if len(dir_paths) == 1:
        browse(dir_paths[0]["path"])
        return
    # جمع محتويات كل المجلدات مع تتبع المصدر
    items = []  # (نوع، مسار_كامل، اسم_العرض، مصدر)
    for entry in dir_paths:
        dpath = entry["path"]
        dname = entry.get("name", os.path.basename(dpath))
        try:
            for name in sorted(os.listdir(dpath)):
                full = os.path.join(dpath, name)
                if os.path.isdir(full):
                    items.append(("dir", full, name, dname))
                elif os.path.isfile(full):
                    items.append(("file", full, name, dname))
        except PermissionError:
            continue
    if not items:
        print_warn("المجلدات فارغة")
        return
    # تسجيل عدد العناصر المجمعة من كل المجلدات
    log_file_op(
        f"تصفح الكل: {len(items)} عنصر من {len(dir_paths)} مجلد",
        operation="listdir",
        result="success",
        script_name="file_manager",
        func_name="browse_all",
    )
    while True:
        print(f"\n{BOLD}تصفح الكل{RESET}  {DIM}({len(items)}){RESET}\n")
        # عرض مرقم - المجلدات أولاً ثم الملفات
        dirs_list = [x for x in items if x[0] == "dir"]
        files_list = [x for x in items if x[0] == "file"]
        ordered = dirs_list + files_list
        for i, (etype, fpath, name, src) in enumerate(ordered, 1):
            if etype == "dir":
                print(f"    {CYAN}{i}{RESET}  {name}/  {DIM}({src}){RESET}")
            else:
                try:
                    size = format_size(os.path.getsize(fpath))
                except OSError:
                    size = "?"
                print(f"    {GREEN}{i}{RESET}  {name}  {DIM}{size}  ({src}){RESET}")
        print(f"    {RED}0{RESET}  رجوع")
        choice = ask("اختر:")
        if not choice or choice == "0":
            return
        if choice.isdigit() and 1 <= int(choice) <= len(ordered):
            etype, fpath, name, src = ordered[int(choice) - 1]
            if etype == "dir":
                browse(fpath)
            else:
                open_file(fpath)
        else:
            print_err("رقم غير صحيح!")


def search_all(paths) -> None:
    """بحث بالأسماء عبر جميع المسارات المحفوظة - كل شيء مرقم"""
    # جمع كل الملفات من كل المسارات
    all_files = []
    for entry in paths:
        epath = entry.get("path", "")
        if not os.path.exists(epath):
            continue
        if os.path.isfile(epath):
            # ملف مباشر - أضفه كما هو
            all_files.append(epath)
        elif os.path.isdir(epath):
            # مجلد - اجمع محتوياته بعمق
            for root, _dirs, files in os.walk(epath):
                for f in files:
                    all_files.append(os.path.join(root, f))
                # حماية الذاكرة: حد 2000 ملف
                if len(all_files) >= 2000:
                    break
    if not all_files:
        print_warn("لا توجد ملفات للبحث")
        return
    # الفلتر هو الاستثناء الوحيد للإدخال النصي
    query = ask("كلمة البحث:")
    if not query:
        return
    # البحث في الأسماء
    results = [f for f in all_files if query.lower() in os.path.basename(f).lower()]
    if not results:
        print_warn("لم يُعثر على نتائج")
        log_file_op(
            f"بحث شامل بدون نتائج: {query}",
            operation="listdir",
            result="success",
            script_name="file_manager",
            func_name="search_all",
        )
        return
    log_file_op(
        f"بحث شامل '{query}': {len(results)} نتيجة من {len(all_files)} ملف",
        operation="listdir",
        result="success",
        script_name="file_manager",
        func_name="search_all",
    )
    # عرض النتائج مرقمة
    while True:
        print(f"\n  {BOLD}{len(results)}{RESET} نتيجة\n")
        display = results[:30]
        for i, fpath in enumerate(display, 1):
            name = os.path.basename(fpath)
            # المجلد الأب كمرجع
            parent = os.path.basename(os.path.dirname(fpath))
            try:
                size = format_size(os.path.getsize(fpath))
            except OSError:
                size = "?"
            print(f"    {GREEN}{i}{RESET}  {name}  {DIM}{size}  ({parent}){RESET}")
        if len(results) > 30:
            print(f"    {DIM}... و {len(results) - 30} نتيجة اخرى{RESET}")
        print(f"    {RED}0{RESET}  رجوع")
        choice = ask("اختر:")
        if not choice or choice == "0":
            return
        if choice.isdigit() and 1 <= int(choice) <= len(display):
            open_file(display[int(choice) - 1])
        else:
            print_err("رقم غير صحيح!")


def enter_path(paths) -> None:
    """الدخول على مسار محفوظ بالرقم - عرض قائمة مرقمة"""
    if not paths:
        print_warn("لا توجد مسارات محفوظة")
        return
    print(f"\n{BOLD}المسارات المحفوظة{RESET}\n")
    for i, entry in enumerate(paths, 1):
        etype = entry.get("type", "dir")
        name = entry.get("name", "")
        path = entry.get("path", "")
        if etype == "file":
            try:
                size = format_size(os.path.getsize(path))
            except OSError:
                size = "?"
            print(f"    {GREEN}{i}{RESET}  {name}  {DIM}{size}{RESET}")
        else:
            print(f"    {CYAN}{i}{RESET}  {name}/")
        # المسار الكامل خافت
        print(f"       {DIM}{path}{RESET}")
    print(f"    {RED}0{RESET}  رجوع")
    choice = ask("اختر:")
    if not choice or choice == "0":
        return
    if choice.isdigit() and 1 <= int(choice) <= len(paths):
        entry = paths[int(choice) - 1]
        epath = entry.get("path", "")
        if os.path.isfile(epath):
            open_file(epath)
        elif os.path.isdir(epath):
            browse(epath)
        else:
            print_err("المسار غير موجود!")
    else:
        print_err("رقم غير صحيح!")


def edit_paths(paths):
    """تعديل المسارات المحفوظة - حذف، إعادة تسمية، نقل"""
    if not paths:
        print_warn("لا توجد مسارات للتعديل")
        return paths
    while True:
        print(f"\n{BOLD}تعديل المسارات{RESET}\n")
        for i, entry in enumerate(paths, 1):
            etype = entry.get("type", "dir")
            name = entry.get("name", "")
            suffix = "/" if etype == "dir" else ""
            print(f"    {CYAN}{i}{RESET}  {name}{suffix}")
        print(f"    {RED}0{RESET}  رجوع")
        choice = ask("اختر مسار للتعديل:")
        if not choice or choice == "0":
            return paths
        if not choice.isdigit() or not (1 <= int(choice) <= len(paths)):
            print_err("رقم غير صحيح!")
            continue
        idx = int(choice) - 1
        entry = paths[idx]
        name = entry.get("name", "")
        epath = entry.get("path", "")
        # قائمة العمليات على المسار المختار
        print(f"\n  {BOLD}{name}{RESET}")
        print(f"  {DIM}{epath}{RESET}\n")
        print(f"    {RED}1{RESET}  حذف من القائمة")
        print(f"    {RED}2{RESET}  حذف الملف/المجلد نهائياً")
        print(f"    {CYAN}3{RESET}  إعادة تسمية")
        print(f"    {CYAN}4{RESET}  نقل")
        print(f"    {DIM}0{RESET}  رجوع")
        op = ask("اختر:")
        if not op or op == "0":
            continue
        if op == "1":
            # حذف من القائمة فقط - لا يمس الملف
            paths.pop(idx)
            save_paths(paths)
            print_ok("تم الحذف من القائمة")
            log_action(
                f"حذف من القائمة: {name}",
                action_type="edit",
                script_name="file_manager",
                func_name="edit_paths",
            )
        elif op == "2":
            # حذف نهائي - تأكيد مزدوج
            print_warn("هل تريد حذف الملف/المجلد نهائياً من القرص؟")
            print(f"    {RED}1{RESET}  نعم، احذف نهائياً")
            print(f"    {DIM}0{RESET}  إلغاء")
            confirm = ask("اختر:")
            if confirm == "1":
                try:
                    if os.path.isfile(epath):
                        os.remove(epath)
                    elif os.path.isdir(epath):
                        import shutil

                        shutil.rmtree(epath)
                    paths.pop(idx)
                    save_paths(paths)
                    print_ok("تم الحذف نهائياً")
                    # تسجيل الحذف النهائي — عملية خطيرة
                    log_file_op(
                        f"حذف نهائي: {name}",
                        operation="delete",
                        file_path=epath,
                        result="success",
                        script_name="file_manager",
                        func_name="edit_paths",
                    )
                except Exception as e:
                    print_err(f"فشل الحذف: {e}")
                    log_exception(
                        f"فشل الحذف النهائي: {name}",
                        exc=e,
                        script_name="file_manager",
                        func_name="edit_paths",
                    )
        elif op == "3":
            # إعادة تسمية
            new_name = ask("الاسم الجديد:")
            if not new_name:
                continue
            parent = os.path.dirname(epath)
            new_path = os.path.join(parent, new_name)
            try:
                os.rename(epath, new_path)
                # تحديث الكائن
                entry["path"] = os.path.normpath(new_path)
                entry["name"] = new_name
                if entry["type"] == "file":
                    entry["hash"] = compute_file_hash(new_path)
                save_paths(paths)
                print_ok("تم التسمية")
                log_file_op(
                    f"إعادة تسمية: {name} → {new_name}",
                    operation="rename",
                    file_path=new_path,
                    result="success",
                    script_name="file_manager",
                    func_name="edit_paths",
                )
            except Exception as e:
                print_err(f"فشلت التسمية: {e}")
                log_exception(
                    f"فشل إعادة التسمية: {name}",
                    exc=e,
                    script_name="file_manager",
                    func_name="edit_paths",
                )
        elif op == "4":
            # نقل - فتح نافذة اختيار مجلد الوجهة
            dest_list = _open_dialog("dir")
            if not dest_list:
                continue
            dest_dir = dest_list[0]
            new_path = os.path.join(dest_dir, entry.get("name", ""))
            try:
                import shutil

                shutil.move(epath, new_path)
                entry["path"] = os.path.normpath(new_path)
                if entry["type"] == "file":
                    entry["hash"] = compute_file_hash(new_path)
                save_paths(paths)
                print_ok("تم النقل")
                log_file_op(
                    f"نقل: {name} → {dest_dir}",
                    operation="move",
                    file_path=new_path,
                    result="success",
                    script_name="file_manager",
                    func_name="edit_paths",
                )
            except Exception as e:
                print_err(f"فشل النقل: {e}")
                log_exception(
                    f"فشل النقل: {name}",
                    exc=e,
                    script_name="file_manager",
                    func_name="edit_paths",
                )
    return paths


def detailed_search(paths) -> None:
    """البحث التفصيلي - فلترة متعددة الأعمدة في ملفات إكسل"""
    if not paths:
        print_warn("لا توجد مسارات محفوظة")
        return
    # اختيار المسار المحفوظ
    print(f"\n{BOLD}اختر مسار للبحث{RESET}\n")
    for i, entry in enumerate(paths, 1):
        etype = entry.get("type", "dir")
        name = entry.get("name", "")
        if etype == "dir":
            print(f"    {CYAN}{i}{RESET}  {name}/")
        else:
            print(f"    {GREEN}{i}{RESET}  {name}")
    print(f"    {RED}0{RESET}  رجوع")
    choice = ask("اختر:")
    if not choice or choice == "0":
        return
    if not choice.isdigit() or not (1 <= int(choice) <= len(paths)):
        print_err("رقم غير صحيح!")
        return
    selected = paths[int(choice) - 1]
    spath = selected.get("path", "")
    # تحديد ملف الإكسل حسب نوع المسار
    target_file = None
    if os.path.isfile(spath):
        ext = os.path.splitext(spath)[1].lower()
        if ext not in (".xlsx", ".xls"):
            print_err("الملف ليس إكسل!")
            return
        target_file = spath
    elif os.path.isdir(spath):
        # جمع ملفات الإكسل من المجلد بعمق - حد 200 ملف للذاكرة
        excel_files = []
        for root, _dirs, files in os.walk(spath):
            for f in files:
                if f.lower().endswith((".xlsx", ".xls")):
                    excel_files.append(os.path.join(root, f))
            if len(excel_files) >= 200:
                break
        if not excel_files:
            print_warn("لا توجد ملفات إكسل")
            return
        # عرض الملفات مرقمة بالمسار النسبي
        print(f"\n{BOLD}ملفات الإكسل{RESET}  {DIM}({len(excel_files)}){RESET}\n")
        show_files = excel_files[:30]
        for i, fpath in enumerate(show_files, 1):
            rel = os.path.relpath(fpath, spath)
            try:
                size = format_size(os.path.getsize(fpath))
            except OSError:
                size = "?"
            print(f"    {GREEN}{i}{RESET}  {rel}  {DIM}{size}{RESET}")
        if len(excel_files) > 30:
            print(f"    {DIM}... و {len(excel_files) - 30} ملف اخر{RESET}")
        print(f"    {RED}0{RESET}  رجوع")
        f_choice = ask("اختر:")
        if not f_choice or f_choice == "0":
            return
        if not f_choice.isdigit() or not (1 <= int(f_choice) <= len(show_files)):
            print_err("رقم غير صحيح!")
            return
        target_file = show_files[int(f_choice) - 1]
    else:
        print_err("المسار غير موجود!")
        return
    # قراءة الملف - حماية من الملفات الفارغة أو التالفة
    try:
        headers, rows = read_excel_data(target_file)
    except Exception as e:
        print_err(f"فشل قراءة الملف: {e}")
        return
    if headers is None or not rows:
        print_warn("الملف فارغ أو لا يحتوي بيانات")
        return
    # حساب القيم الفريدة لكل عمود - الأكثر تكراراً أولاً
    col_uniques = []
    for ci in range(len(headers)):
        vals = {}
        for row in rows:
            if ci < len(row) and row[ci] is not None:
                v = str(row[ci]).strip()
                if v:
                    vals[v] = vals.get(v, 0) + 1
        # ترتيب بالتكرار ثم أخذ أول 5
        sorted_v = sorted(vals.keys(), key=lambda x: -vals[x])
        preview = sorted_v[:5]
        extra = len(vals) - 5 if len(vals) > 5 else 0
        col_uniques.append((preview, extra))
    # عرض الأعمدة مع القيم الفريدة على نفس السطر
    print(f"\n{BOLD}{os.path.basename(target_file)}{RESET}  {DIM}{len(rows)} صف{RESET}\n")
    for ci, header in enumerate(headers):
        preview, extra = col_uniques[ci]
        vals_str = ", ".join(preview)
        if extra > 0:
            vals_str += f" +{extra}"
        # اقتطاع القيم الطويلة لحماية عرض الطرفية
        if len(vals_str) > 50:
            vals_str = vals_str[:47] + "..."
        print(f"    {CYAN}{ci + 1}{RESET}  {header}  {DIM}{vals_str}{RESET}")
    print(f"    {RED}0{RESET}  رجوع\n")
    # عدد الفلاتر
    num_str = ask("عدد الفلاتر:")
    if not num_str or num_str == "0":
        return
    if not num_str.isdigit() or int(num_str) < 1:
        print_err("رقم غير صحيح!")
        return
    num_filters = int(num_str)
    # إدخال الفلاتر: رقم العمود ثم قيمة الفلتر
    filters = []
    for i in range(num_filters):
        col_str = ask(f"رقم العمود {i + 1}:")
        if not col_str or not col_str.isdigit():
            print_err("رقم غير صحيح!")
            return
        col_idx = int(col_str) - 1
        if not (0 <= col_idx < len(headers)):
            print_err("رقم خارج النطاق!")
            return
        # قيمة الفلتر - الاستثناء الوحيد للإدخال النصي
        val = ask(f"قيمة الفلتر {i + 1}:")
        if not val:
            # فارغ = تخطي هذا الفلتر
            continue
        filters.append((col_idx, val))
    if not filters:
        print_warn("لم يتم إدخال فلاتر")
        return
    # تطبيق الفلاتر - كل الشروط يجب أن تتحقق معاً
    matches = []
    for row in rows:
        ok = True
        for col_idx, filter_val in filters:
            if col_idx >= len(row) or row[col_idx] is None:
                ok = False
                break
            cell = str(row[col_idx]).strip()
            # بحث جزئي بدون حساسية لحالة الحروف
            if filter_val.lower() not in cell.lower():
                ok = False
                break
        if ok:
            matches.append(row)
    if not matches:
        print_warn("لا توجد نتائج")
        log_file_op(
            f"بحث تفصيلي بدون نتائج ({len(filters)} فلتر)",
            operation="read",
            file_path=target_file,
            result="success",
            script_name="file_manager",
            func_name="detailed_search",
        )
        return
    # تسجيل نتائج البحث التفصيلي
    log_file_op(
        f"بحث تفصيلي: {len(matches)} نتيجة من {len(rows)} صف ({len(filters)} فلتر)",
        operation="read",
        file_path=target_file,
        result="success",
        script_name="file_manager",
        func_name="detailed_search",
    )
    # عرض النتائج مع تمييز الأعمدة المُفلترة
    while True:
        print(f"\n  {BOLD}{len(matches)}{RESET} نتيجة\n")
        display = matches[:20]
        for row_idx, row in enumerate(display, 1):
            print(f"  {DIM}نتيجة {row_idx}{RESET}")
            for ci, val in enumerate(row):
                if ci < len(headers) and val is not None:
                    val_str = str(val).strip()
                    if val_str:
                        is_filtered = any(f[0] == ci for f in filters)
                        if is_filtered:
                            print(f"    {GREEN}{headers[ci]}: {val_str}{RESET}")
                        else:
                            print(f"    {headers[ci]}: {val_str}")
            print()
        if len(matches) > 20:
            print(f"  {DIM}... و {len(matches) - 20} نتيجة اخرى{RESET}")
        print(f"    {RED}0{RESET}  رجوع")
        r_choice = ask("اختر:")
        if not r_choice or r_choice == "0":
            return


def _show_paths_header(paths) -> None:
    """عرض أسماء المسارات المحفوظة في رأس القائمة - بدون مسارات كاملة"""
    if not paths:
        return
    for entry in paths:
        etype = entry.get("type", "dir")
        name = entry.get("name", "")
        path = entry.get("path", "")
        if etype == "dir":
            print(f"  {name}/")
        else:
            # ملف: اسم + حجم
            try:
                size = format_size(os.path.getsize(path))
            except OSError:
                size = "?"
            print(f"  {name}  {DIM}{size}{RESET}")


def run() -> str | None:
    """الحلقة الرئيسية لمدير الملفات - قائمة موحدة"""
    # تحميل المسارات المحفوظة
    paths = load_paths()
    # إضافة المسار الافتراضي إذا موجود ولم يُضف
    default_path = "C:/Users/ps5mk/Desktop/HT_SC_/HT_Data_File"  # داخل المشروع
    if os.path.isdir(default_path):
        norm = os.path.normpath(default_path)
        exists = any(entry["path"] == norm for entry in paths)
        if not exists:
            entry = {
                "path": norm,
                "name": os.path.basename(norm),
                "type": "dir",
                "hash": None,
            }
            paths.insert(0, entry)
            save_paths(paths)
    while True:
        print_header("مدير الملفات")
        # رأس القائمة: أسماء المسارات فقط
        _show_paths_header(paths)
        print()
        # القائمة الموحدة
        print(f"    {CYAN}1{RESET}  تصفح الكل")
        print(f"    {CYAN}2{RESET}  بحث")
        print(f"    {GREEN}+{RESET}  اضافة مسار")
        print(f"    {CYAN}3{RESET}  الدخول على مسار")
        print(f"    {CYAN}4{RESET}  تعديل المسارات")
        print(f"    {CYAN}5{RESET}  البحث التفصيلي")
        print(f"    {CYAN}6{RESET}  الإحصائيات والتقارير")
        print(f"    {RED}0{RESET}  رجوع")
        print(f"    {RED}00{RESET}  القائمة الرئيسية")
        choice = ask("اختر:")
        if not choice or choice == "0":
            return "back"
        if choice == "00":
            return "back"
        # تسجيل اختيار المستخدم في القائمة
        labels = {
            "1": "تصفح الكل",
            "2": "بحث",
            "+": "إضافة مسار",
            "3": "الدخول على مسار",
            "4": "تعديل المسارات",
            "5": "البحث التفصيلي",
            "6": "الإحصائيات",
        }
        if choice in labels:
            log_action(
                f"مدير الملفات: {labels[choice]}",
                action_type="menu_choice",
                user_response=choice,
                is_valid=1,
                script_name="file_manager",
                func_name="run",
            )
        if choice == "1":
            browse_all(paths)
        elif choice == "2":
            search_all(paths)
        elif choice == "+":
            paths = add_path(paths)
        elif choice == "3":
            enter_path(paths)
        elif choice == "4":
            paths = edit_paths(paths)
        elif choice == "5":
            detailed_search(paths)
        elif choice == "6":
            from reports_generator import run_reports_menu

            run_reports_menu(paths)
        else:
            print_err("اختيار غير صحيح!")
