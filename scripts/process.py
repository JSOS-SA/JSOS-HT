"""سكربت 2: معالجة رسائل الواتساب وتحويلها إلى إكسل"""

import os
import time as _time

import xlsxwriter

from common import (
    FIXED_COLS,
    TRIP_COLS,
    ask,
    ask_file_dialog,
    extract_photos,
    extract_trips,
    parse_messages,
    parse_stamp,
    print_err,
    print_header,
    print_ok,
    print_warn,
)
from logger_config import (
    log_action,
    log_exception,
    log_file_op,
    log_processing,
    log_quality,
)


def get_inputs():
    """طلب المدخلات من المستخدم"""
    print_header("معالجة رسائل الواتساب")
    print_warn("انسخ الطابع الزمني من الواتساب كما هو")

    path = ask_file_dialog("اختر ملف الواتساب", [("ملفات نصية", "*.txt"), ("جميع الملفات", "*.*")])
    if not path:
        return None

    # تسجيل حجم ملف الواتساب
    try:
        fsize = os.path.getsize(path)
        log_file_op(
            f"ملف واتساب: {os.path.basename(path)}",
            operation="open",
            file_path=path,
            file_size_bytes=fsize,
            result="success",
            script_name="process",
            func_name="get_inputs",
        )
    except Exception:
        pass

    raw_start = ask("طابع البداية (مثل [2/9/26، 05:49:00]):")
    start_date, start_time = parse_stamp(raw_start)
    if not start_date:
        print_err("خطأ: صيغة طابع البداية غير صحيحة!")
        log_quality(
            "صيغة طابع بداية غير صحيحة",
            actual_value=raw_start,
            expected_type="timestamp",
            issue_type="invalid_date",
            script_name="process",
            func_name="get_inputs",
        )
        return None

    raw_end = ask("طابع النهاية (مثل [2/9/26، 13:44:51]):")
    end_date, end_time = parse_stamp(raw_end)
    if not end_date:
        print_err("خطأ: صيغة طابع النهاية غير صحيحة!")
        log_quality(
            "صيغة طابع نهاية غير صحيحة",
            actual_value=raw_end,
            expected_type="timestamp",
            issue_type="invalid_date",
            script_name="process",
            func_name="get_inputs",
        )
        return None

    print_ok(f"\nالبداية: {start_date} - {start_time}")
    print_ok(f"النهاية: {end_date} - {end_time}")

    # تسجيل الطوابع المدخلة
    log_action(
        f"طوابع: {start_date} {start_time} → {end_date} {end_time}",
        action_type="input",
        script_name="process",
        func_name="get_inputs",
    )

    output = ask_file_dialog("اختر مكان حفظ الإكسل", [("Excel", "*.xlsx")])
    if not output:
        return None
    # إضافة .xlsx إذا لم يكن موجوداً
    if not output.lower().endswith(".xlsx"):
        output = output + ".xlsx"

    return path, start_date, start_time, end_date, end_time, output


def write_excel(rows, output_path) -> None:
    """كتابة البيانات في ملف إكسل"""
    max_trips = max(len(r["trips"]) for r in rows) if rows else 1

    # الكتابة في ملف مؤقت أولاً لحماية البيانات من الانقطاع
    tmp_path = output_path + ".tmp"
    wb = xlsxwriter.Workbook(tmp_path)
    try:
        ws = wb.add_worksheet("بيانات")

        hdr_fmt = wb.add_format(
            {
                "bold": True,
                "bg_color": "#4472C4",
                "font_color": "white",
                "border": 1,
                "align": "center",
                "valign": "vcenter",
                "text_wrap": True,
                "font_name": "Arial",
                "font_size": 11,
            },
        )
        cell_fmt = wb.add_format(
            {
                "border": 1,
                "align": "center",
                "valign": "vcenter",
                "font_name": "Arial",
                "font_size": 11,
            },
        )
        photo_fmt = wb.add_format(
            {
                "border": 1,
                "align": "left",
                "valign": "vcenter",
                "font_name": "Arial",
                "font_size": 9,
                "text_wrap": True,
            },
        )

        # رؤوس الأعمدة الثابتة
        col = 0
        for h in FIXED_COLS:
            ws.write(0, col, h, hdr_fmt)
            col += 1

        # رؤوس أعمدة الرحلات
        for t_idx in range(max_trips):
            suffix = f" {t_idx + 1}" if max_trips > 1 else ""
            for h in TRIP_COLS:
                ws.write(0, col, f"{h}{suffix}", hdr_fmt)
                col += 1

        # البيانات
        for r, row in enumerate(rows, 1):
            ws.write(r, 0, row["date"], cell_fmt)
            ws.write(r, 1, row["time"], cell_fmt)
            ws.write(r, 2, row["sender"], cell_fmt)
            ws.write(r, 3, row["photos"], photo_fmt)

            for t_idx, trip in enumerate(row["trips"]):
                base_col = 4 + t_idx * len(TRIP_COLS)
                for c, col_name in enumerate(TRIP_COLS):
                    ws.write(r, base_col + c, trip.get(col_name, ""), cell_fmt)

        # عرض الأعمدة
        ws.set_column(0, 0, 12)
        ws.set_column(1, 1, 10)
        ws.set_column(2, 2, 22)
        ws.set_column(3, 3, 40)

        for t_idx in range(max_trips):
            base = 4 + t_idx * len(TRIP_COLS)
            ws.set_column(base, base, 14)
            ws.set_column(base + 1, base + 1, 12)
            ws.set_column(base + 2, base + 2, 12)
            ws.set_column(base + 3, base + 3, 14)
            ws.set_column(base + 4, base + 4, 10)
            ws.set_column(base + 5, base + 5, 20)
            ws.set_column(base + 6, base + 6, 12)
            ws.set_column(base + 7, base + 7, 18)
            ws.set_column(base + 8, base + 8, 18)

        ws.right_to_left()
    finally:
        wb.close()

    # إجبار الكتابة على القرص قبل النقل - حماية من انقطاع التيار
    # r+b لأن fsync يحتاج وضع كتابة على ويندوز
    with open(tmp_path, "r+b") as f:
        os.fsync(f.fileno())
    log_file_op(
        "fsync على الملف المؤقت",
        operation="write",
        file_path=tmp_path,
        result="success",
        script_name="process",
        func_name="write_excel",
    )
    os.replace(tmp_path, output_path)
    # تسجيل حجم الملف النهائي
    try:
        out_size = os.path.getsize(output_path)
        log_file_op(
            f"كتابة إكسل: {os.path.basename(output_path)}",
            operation="write",
            file_path=output_path,
            file_size_bytes=out_size,
            result="success",
            script_name="process",
            func_name="write_excel",
        )
    except Exception:
        pass


def _save_to_db(chat_path, start_date, start_time, end_date, end_time) -> None:
    """حفظ البيانات في قاعدة SQLite — عملية صامتة لا توقف المعالجة عند الفشل"""
    try:
        import sys

        # المسار النسبي لمجلد المشروع
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if project_dir not in sys.path:
            sys.path.insert(0, project_dir)
        from db.import_whatsapp import import_chat

        stats = import_chat(chat_path, start_date, start_time, end_date, end_time)
        print_ok(f"القاعدة: {stats['trips']} رحلة / {stats['messages']} رسالة")
    except Exception as e:
        # فشل القاعدة لا يوقف العمل — الإكسل هو المخرج الأساسي
        print_warn(f"تخطي حفظ القاعدة: {e}")
        log_exception("فشل حفظ القاعدة", exc=e, script_name="process", func_name="_save_to_db")


def run() -> str:
    """تشغيل المعالجة"""
    inputs = get_inputs()
    if not inputs:
        return "back"

    path, start_date, start_time, end_date, end_time, output = inputs
    t0 = _time.time()

    try:
        print_warn("\nجارٍ المعالجة...")
        # تسجيل بداية المعالجة
        log_processing(
            "بداية معالجة واتساب",
            operation_type="whatsapp_processing",
            input_file=path,
            output_file=output,
            script_name="process",
            func_name="run",
        )

        messages = parse_messages(path, start_date, start_time, end_date, end_time)
        print_warn(f"تم قراءة {len(messages)} رسالة...")

        rows = []
        deleted_count = 0
        incomplete_count = 0
        for msg in messages:
            full_text = " ".join(msg.get("raw_lines", []))
            # كشف الرسائل المحذوفة
            if "تم حذف هذه الرسالة" in full_text:
                deleted_count += 1
                log_quality(
                    "رسالة محذوفة",
                    source_file=path,
                    column_name="content",
                    issue_type="empty",
                    actual_value="محذوفة",
                    script_name="process",
                    func_name="run",
                )
                continue

            trips = extract_trips(msg)
            if trips:
                # كشف رحلات ناقصة البيانات
                for _t_idx, trip in enumerate(trips):
                    flight = trip.get("رقم الرحلة", "")
                    dest = trip.get("الوجهة", "")
                    pax = trip.get("عدد الركاب", "")
                    if flight and not dest:
                        incomplete_count += 1
                        log_quality(
                            "رحلة بدون وجهة",
                            source_file=path,
                            column_name="الوجهة",
                            actual_value=f"رحلة={flight}",
                            expected_type="text",
                            issue_type="empty",
                            script_name="process",
                            func_name="run",
                        )
                    if flight and not pax:
                        log_quality(
                            "رحلة بدون عدد ركاب",
                            source_file=path,
                            column_name="عدد الركاب",
                            actual_value=f"رحلة={flight}",
                            expected_type="number",
                            issue_type="empty",
                            script_name="process",
                            func_name="run",
                        )

                photos = extract_photos(msg.get("raw_lines", []))
                photo_str = " - ".join(photos) if photos else ""
                rows.append(
                    {
                        "date": msg["date"],
                        "time": msg["time"],
                        "sender": msg["sender"],
                        "photos": photo_str,
                        "trips": trips,
                    },
                )

        if not rows:
            print_err("لا توجد رحلات في هذا النطاق!")
            log_processing(
                "لا رحلات في النطاق",
                operation_type="whatsapp_processing",
                input_file=path,
                rows_read=len(messages),
                trips_extracted=0,
                duration_seconds=round(_time.time() - t0, 2),
                result="fail",
                script_name="process",
                func_name="run",
            )
            return "back"

        print_warn("جارٍ كتابة الإكسل...")
        write_excel(rows, output)

        # حفظ تلقائي في قاعدة البيانات
        _save_to_db(path, start_date, start_time, end_date, end_time)

        total_trips = sum(len(r["trips"]) for r in rows)
        total_photos = sum(1 for r in rows if r["photos"])
        duration = round(_time.time() - t0, 2)

        # تسجيل نجاح المعالجة
        log_processing(
            f"نجاح: {total_trips} رحلة من {len(messages)} رسالة",
            operation_type="whatsapp_processing",
            input_file=path,
            output_file=output,
            rows_read=len(messages),
            rows_written=len(rows),
            trips_extracted=total_trips,
            duration_seconds=duration,
            result="success",
            script_name="process",
            func_name="run",
        )

        print_header("تمت المعالجة بنجاح!")
        print_ok(f"الرسائل في النطاق: {len(messages)}")
        print_ok(f"الصفوف: {len(rows)}")
        print_ok(f"الرحلات: {total_trips}")
        print_ok(f"صفوف تحتوي صور: {total_photos}")
        print_ok(f"الملف: {output}")

    except Exception as e:
        duration = round(_time.time() - t0, 2)
        log_exception(
            "فشل المعالجة",
            exc=e,
            script_name="process",
            func_name="run",
            context=f"ملف={path}",
        )
        log_processing(
            f"فشل: {e}",
            operation_type="whatsapp_processing",
            input_file=path,
            duration_seconds=duration,
            result="fail",
            script_name="process",
            func_name="run",
        )
        print_err(f"خطأ في المعالجة: {e}")

    return "back"
