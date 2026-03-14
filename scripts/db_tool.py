"""أداة قاعدة البيانات — خيار 12 في القائمة الرئيسية.

20 خيار: إحصائيات + بحث + تحليل + استيراد وتصدير
"""

import sys
from pathlib import Path

from common import (
    BOLD,
    CYAN,
    DIM,
    GREEN,
    ORANGE,
    RED,
    RESET,
    ask,
    pause,
    print_err,
    print_header,
    print_ok,
    print_warn,
)

# مسار المشروع
_PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

_DB_PATH = _PROJECT_DIR / "db" / "ht_sc.db"


def _check_db() -> bool:
    """التحقق من وجود القاعدة — إنشاء تلقائي إذا غير موجودة"""
    if not _DB_PATH.exists():
        print_warn("القاعدة غير موجودة — جارٍ الإنشاء...")
        from db.schema import init_db

        init_db().close()
        print_ok("تم إنشاء القاعدة")
    return True


# ==========================================
# مجموعة 1: الإحصائيات
# ==========================================


def _show_db_info() -> None:
    """معلومات عامة عن القاعدة"""
    from db.queries import Q

    with Q() as q:
        total = q._fetchone("SELECT COUNT(*) as c, COALESCE(SUM(passenger_count),0) as pax FROM trips")
        days = q._fetchone("SELECT COUNT(DISTINCT date) as c FROM trips")
        span = q._fetchone("SELECT MIN(date) as first, MAX(date) as last FROM trips")
        sources = q._fetch("SELECT source, COUNT(*) as c FROM trips GROUP BY source")
        sessions = q._fetchone("SELECT COUNT(*) as c FROM comparison_sessions")
        diffs = q._fetchone("SELECT COUNT(*) as c FROM comparison_diffs WHERE status='open'")

    # حجم القاعدة
    db_size = _DB_PATH.stat().st_size
    size_text = f"{db_size / (1024 * 1024):.1f} م.ب" if db_size > 1024 * 1024 else f"{db_size / 1024:.0f} ك.ب"

    print_header("معلومات القاعدة")
    print_ok(f"الحجم: {size_text}")
    print_ok(f"الرحلات: {total['c']}")
    print_ok(f"الركاب: {total['pax']}")
    print_ok(f"الأيام: {days['c']}")
    if span["first"]:
        print_ok(f"النطاق: {span['first']} إلى {span['last']}")
    print_warn("\nالمصادر:")
    for s in sources:
        print(f"  {s['source']}: {s['c']}")
    print_ok(f"\nجلسات المقارنة: {sessions['c']}")
    print_ok(f"اختلافات مفتوحة: {diffs['c']}")


def _daily_report() -> None:
    """تقرير يومي سريع"""
    date = ask("التاريخ (مثل 2026-01-01):")
    if not date:
        return

    from db.queries import Q

    with Q() as q:
        daily = q.daily_summary(date)
        if not daily:
            print_err("لا توجد بيانات لهذا التاريخ!")
            return

        print_header(f"تقرير {date}")
        total_trips = 0
        total_pax = 0
        for d in daily:
            total_trips += d["trips"]
            total_pax += d["pax"]
            print(
                f"  نوبة {d['shift']}: {d['trips']} رحلة / {d['pax']} راكب / {d['flights']} فريدة / {d['destinations']} وجهة",
            )

        print_ok(f"\nالإجمالي: {total_trips} رحلة / {total_pax} راكب")

        # أعلى 5 وجهات
        dests = q.top_destinations(date, 5)
        if dests:
            print_warn("\nأعلى 5 وجهات:")
            for d in dests:
                print(f"  {d['value']}: {d['count']} ({d['total_pax']} راكب)")


def _monthly_report() -> None:
    """ملخص شهري"""
    prefix = ask("بداية الشهر (مثل 2026-01):")
    if not prefix:
        return

    from db.queries import Q

    with Q() as q:
        monthly = q.monthly_summary(prefix)
        if not monthly:
            print_err("لا توجد بيانات!")
            return

        print_header(f"ملخص شهر {prefix}")
        grand_trips = 0
        grand_pax = 0
        for m in monthly:
            grand_trips += m["trips"]
            grand_pax += m["pax"]
            print(f"  {m['date']}: {m['trips']} رحلة / {m['pax']} راكب / {m['flights']} فريدة")

        print_ok(f"\nالإجمالي: {grand_trips} رحلة / {grand_pax} راكب / {len(monthly)} يوم")
        if monthly:
            avg = grand_trips / len(monthly)
            print_ok(f"المتوسط اليومي: {avg:.0f} رحلة")


def _shift_stats() -> None:
    """إحصائيات النوبات — عدد رحلات/ركاب لكل نوبة"""
    date = ask("التاريخ (فارغ = كل الأيام):")

    from db.queries import Q

    with Q() as q:
        results = q.shift_stats_all(date or None)
        if not results:
            print_err("لا توجد بيانات!")
            return

        title = f"إحصائيات النوبات — {date}" if date else "إحصائيات النوبات — الإجمالي"
        print_header(title)
        grand_trips = 0
        grand_pax = 0
        for r in results:
            grand_trips += r["trips"]
            grand_pax += r["pax"]
            days_text = f" / {r['days']} يوم" if r["days"] > 1 else ""
            print(
                f"  نوبة {r['shift']}: {r['trips']} رحلة / {r['pax']} راكب / {r['flights']} فريدة / {r['destinations']} وجهة{days_text}",
            )

        print_ok(f"\nالإجمالي: {grand_trips} رحلة / {grand_pax} راكب")


def _weekly_report() -> None:
    """إحصائيات أسبوعية — آخر 7 أيام"""
    date = ask("تاريخ النهاية (مثل 2026-01-15):")
    if not date:
        return

    from db.queries import Q

    with Q() as q:
        weekly = q.weekly_summary(date)
        if not weekly:
            print_err("لا توجد بيانات لهذا الأسبوع!")
            return

        print_header(f"ملخص أسبوعي حتى {date}")
        grand_trips = 0
        grand_pax = 0
        for w in weekly:
            grand_trips += w["trips"]
            grand_pax += w["pax"]
            print(
                f"  {w['date']}: {w['trips']} رحلة / {w['pax']} راكب / {w['flights']} فريدة / {w['destinations']} وجهة",
            )

        print_ok(f"\nالإجمالي: {grand_trips} رحلة / {grand_pax} راكب / {len(weekly)} يوم")
        if weekly:
            avg = grand_trips / len(weekly)
            print_ok(f"المتوسط اليومي: {avg:.0f} رحلة")


def _compare_two_dates() -> None:
    """مقارنة يومين"""
    date1 = ask("التاريخ الأول:")
    if not date1:
        return
    date2 = ask("التاريخ الثاني:")
    if not date2:
        return

    from db.queries import Q

    with Q() as q:
        result = q.compare_two_dates(date1, date2)

    d1 = result["date1"]
    d2 = result["date2"]

    print_header(f"مقارنة {date1} مع {date2}")

    # اليوم الأول
    print_warn(f"\n{date1}:")
    print(f"  {d1['trips']} رحلة / {d1['pax']} راكب / {d1['flights']} فريدة / {d1['destinations']} وجهة")

    # اليوم الثاني
    print_warn(f"\n{date2}:")
    print(f"  {d2['trips']} رحلة / {d2['pax']} راكب / {d2['flights']} فريدة / {d2['destinations']} وجهة")

    # الفرق
    diff_trips = d2["trips"] - d1["trips"]
    diff_pax = d2["pax"] - d1["pax"]
    sign_t = "+" if diff_trips > 0 else ""
    sign_p = "+" if diff_pax > 0 else ""
    print_ok(f"\nالفرق: {sign_t}{diff_trips} رحلة / {sign_p}{diff_pax} راكب")

    # الرحلات المختلفة
    only1 = result["only_date1"]
    only2 = result["only_date2"]
    common = result["common"]
    print_ok(f"مشتركة: {len(common)}")
    if only1:
        print_err(f"في {date1} فقط ({len(only1)}):")
        for f in only1[:15]:
            print(f"  - {f}")
        if len(only1) > 15:
            print_warn(f"  ... و {len(only1) - 15} أخرى")
    if only2:
        print_err(f"في {date2} فقط ({len(only2)}):")
        for f in only2[:15]:
            print(f"  - {f}")
        if len(only2) > 15:
            print_warn(f"  ... و {len(only2) - 15} أخرى")


# ==========================================
# مجموعة 2: البحث
# ==========================================


def _search_flight() -> None:
    """البحث عن رحلة بالرقم"""
    flight = ask("رقم الرحلة:")
    if not flight:
        return

    from db.queries import Q

    with Q() as q:
        results = q.trips_by_flight(flight)
        if not results:
            print_err("لا توجد نتائج!")
            return

        print_header(f"نتائج: {flight}")
        print_ok(f"عدد السجلات: {len(results)}")
        for r in results[:20]:
            print(
                f"  {r['date']} | {r['shift_code'] or '?'} | {r['passenger_count']} راكب | {r['destination']} | {r['source']}",
            )

        if len(results) > 20:
            print_warn(f"  ... و {len(results) - 20} سجل آخر")


def _search_destination() -> None:
    """البحث بالوجهة"""
    dest = ask("الوجهة (أو جزء منها):")
    if not dest:
        return
    date = ask("التاريخ (فارغ = الكل):")

    from db.queries import Q

    with Q() as q:
        results = q.trips_by_destination(dest, date or None)
        if not results:
            print_err("لا توجد نتائج!")
            return

        print_header(f"نتائج الوجهة: {dest}")
        print_ok(f"عدد السجلات: {len(results)}")
        total_pax = 0
        for r in results[:30]:
            total_pax += r["passenger_count"] or 0
            print(
                f"  {r['date']} | {r['shift_code'] or '?'} | {r['flight_number']} | {r['passenger_count']} راكب | {r['destination']} | {r['source']}",
            )

        if len(results) > 30:
            print_warn(f"  ... و {len(results) - 30} سجل آخر")
        print_ok(f"إجمالي الركاب: {total_pax}")


def _search_campaign() -> None:
    """البحث بالحملة"""
    campaign = ask("اسم الحملة (أو جزء منه):")
    if not campaign:
        return
    date = ask("التاريخ (فارغ = الكل):")

    from db.queries import Q

    with Q() as q:
        results = q.trips_by_campaign(campaign, date or None)
        if not results:
            print_err("لا توجد نتائج!")
            return

        print_header(f"نتائج الحملة: {campaign}")
        print_ok(f"عدد السجلات: {len(results)}")
        total_pax = 0
        for r in results[:30]:
            total_pax += r["passenger_count"] or 0
            print(
                f"  {r['date']} | {r['flight_number']} | {r['passenger_count']} راكب | {r['destination']} | {r['campaign_name']}",
            )

        if len(results) > 30:
            print_warn(f"  ... و {len(results) - 30} سجل آخر")
        print_ok(f"إجمالي الركاب: {total_pax}")


def _search_sender() -> None:
    """البحث بالمرسل"""
    sender = ask("اسم المرسل (أو جزء منه):")
    if not sender:
        return

    from db.queries import Q

    with Q() as q:
        results = q.trips_by_sender_name(sender)
        if not results:
            # قد لا يكون هناك بيانات واتساب — نبحث في المرسلين المسجلين
            print_err("لا توجد نتائج!")
            print_warn("ملاحظة: البحث بالمرسل يعمل فقط مع بيانات الواتساب المستوردة")
            return

        print_header(f"نتائج المرسل: {sender}")
        print_ok(f"عدد السجلات: {len(results)}")
        for r in results[:20]:
            print(
                f"  {r['date']} | {r['shift_code'] or '?'} | {r['flight_number']} | {r['passenger_count']} راكب | {r['sender']}",
            )

        if len(results) > 20:
            print_warn(f"  ... و {len(results) - 20} سجل آخر")


def _advanced_search() -> None:
    """بحث متقدم بعدة معايير"""
    print_header("بحث متقدم")
    print_warn("أدخل المعايير المطلوبة (فارغ = تجاهل)")

    date = ask("التاريخ:")
    shift = ask("النوبة (A/B/C):")
    shift = shift.upper() if shift in ("a", "b", "c", "A", "B", "C") else None
    flight = ask("رقم الرحلة (أو جزء):")
    destination = ask("الوجهة (أو جزء):")
    campaign = ask("الحملة (أو جزء):")
    source = ask("المصدر (parsed/whatsapp/record):")
    min_pax_str = ask("أقل عدد ركاب:")
    max_pax_str = ask("أكثر عدد ركاب:")

    # تحويل الأرقام
    min_pax = int(min_pax_str) if min_pax_str and min_pax_str.isdigit() else None
    max_pax = int(max_pax_str) if max_pax_str and max_pax_str.isdigit() else None

    from db.queries import Q

    with Q() as q:
        results = q.advanced_search(
            date=date or None,
            shift=shift,
            flight=flight or None,
            destination=destination or None,
            campaign=campaign or None,
            source=source or None,
            min_pax=min_pax,
            max_pax=max_pax,
        )

    if not results:
        print_err("لا توجد نتائج!")
        return

    print_header(f"نتائج البحث المتقدم: {len(results)} سجل")
    total_pax = 0
    for r in results[:30]:
        total_pax += r["passenger_count"] or 0
        print(
            f"  {r['date']} | {r['shift_code'] or '?'} | {r['flight_number']} | {r['passenger_count']} راكب | {r['destination']} | {r['source']}",
        )

    if len(results) > 30:
        print_warn(f"  ... و {len(results) - 30} سجل آخر")
    print_ok(f"المعروض: {min(30, len(results))} من {len(results)} / إجمالي الركاب: {total_pax}")


# ==========================================
# مجموعة 3: التحليل
# ==========================================


def _top_destinations() -> None:
    """أعلى 10 وجهات"""
    date = ask("التاريخ أو الشهر (فارغ = الكل):")

    from db.queries import Q

    with Q() as q:
        results = q.top_destinations(date or None, 10)
        if not results:
            print_err("لا توجد بيانات!")
            return

        title = f"أعلى 10 وجهات — {date}" if date else "أعلى 10 وجهات — الإجمالي"
        print_header(title)
        for i, r in enumerate(results, 1):
            print(f"  {i}. {r['value']}: {r['count']} رحلة / {r['total_pax']} راكب")


def _top_campaigns() -> None:
    """أعلى 10 حملات"""
    date = ask("التاريخ (فارغ = الكل):")

    from db.queries import Q

    with Q() as q:
        results = q.top_campaigns(date or None, 10)
        if not results:
            print_err("لا توجد بيانات!")
            return

        title = f"أعلى 10 حملات — {date}" if date else "أعلى 10 حملات — الإجمالي"
        print_header(title)
        for i, r in enumerate(results, 1):
            print(f"  {i}. {r['value']}: {r['count']} رحلة / {r['total_pax']} راكب")


def _duplicate_flights() -> None:
    """الرحلات المكررة"""
    date = ask("التاريخ (فارغ = الكل):")

    from db.queries import Q

    with Q() as q:
        results = q.duplicate_flights(date or None)
        if not results:
            print_ok("لا توجد رحلات مكررة!")
            return

        print_header("الرحلات المكررة")
        print_ok(f"عدد: {len(results)}")
        for r in results[:30]:
            print(f"  {r['date']} | {r['source']} | {r['flight_number']}: {r['count']} مرة / {r['total_pax']} راكب")

        if len(results) > 30:
            print_warn(f"  ... و {len(results) - 30} أخرى")


def _compare_sources() -> None:  # noqa: C901 — دالة تفاعلية مترابطة
    """مقارنة المصادر"""
    date = ask("التاريخ:")
    if not date:
        return

    from db.queries import Q

    with Q() as q:
        sources = q.compare_sources(date)
        if not sources:
            print_err("لا توجد بيانات لهذا التاريخ!")
            return

        print_header("نتائج المقارنة")

        # عدد الرحلات لكل مصدر
        counts = q.trip_count_by_date(date)
        for c in counts:
            print_ok(f"  {c['source']}: {c['count']} رحلة / {c['total_pax']} راكب")

        # المقارنة بين المصادر
        all_sources = list(sources.keys())
        for i, s1 in enumerate(all_sources):
            for s2 in all_sources[i + 1 :]:
                only_s1 = sources[s1] - sources[s2]
                only_s2 = sources[s2] - sources[s1]
                common = sources[s1] & sources[s2]

                print_warn(f"\n{s1} مقابل {s2}:")
                print_ok(f"  مشتركة: {len(common)}")

                if only_s1:
                    print_err(f"  في {s1} فقط ({len(only_s1)}):")
                    for f in sorted(only_s1)[:10]:
                        print(f"    - {f}")
                    if len(only_s1) > 10:
                        print_warn(f"    ... و {len(only_s1) - 10} أخرى")

                if only_s2:
                    print_err(f"  في {s2} فقط ({len(only_s2)}):")
                    for f in sorted(only_s2)[:10]:
                        print(f"    - {f}")
                    if len(only_s2) > 10:
                        print_warn(f"    ... و {len(only_s2) - 10} أخرى")

                if not only_s1 and not only_s2:
                    print_ok("  متطابقة تماماً!")

        # التكرارات المختلفة
        mismatches = q.flight_count_mismatch(date)
        by_flight = {}
        for m in mismatches:
            fl = m["flight_number"]
            if fl not in by_flight:
                by_flight[fl] = {}
            by_flight[fl][m["source"]] = m["count"]
        diff_flights = {fl: srcs for fl, srcs in by_flight.items() if len(set(srcs.values())) > 1}
        if diff_flights:
            print_warn(f"\nرحلات بتكرار مختلف ({len(diff_flights)}):")
            for fl, srcs in sorted(diff_flights.items())[:15]:
                parts = [f"{s}={c}" for s, c in srcs.items()]
                print(f"  - {fl}: {' / '.join(parts)}")


def _zero_pax() -> None:
    """رحلات بدون ركاب"""
    date = ask("التاريخ (فارغ = الكل):")

    from db.queries import Q

    with Q() as q:
        results = q.zero_pax_trips(date or None)
        if not results:
            print_ok("لا توجد رحلات بدون ركاب!")
            return

        print_header("رحلات بدون ركاب")
        print_ok(f"عدد: {len(results)}")
        for r in results[:30]:
            print(
                f"  {r['date']} | {r['shift_code'] or '?'} | {r['flight_number']} | {r['destination']} | {r['source']}",
            )

        if len(results) > 30:
            print_warn(f"  ... و {len(results) - 30} أخرى")


# ==========================================
# مجموعة 4: الاستيراد والتصدير
# ==========================================


def _import_one() -> None:
    """استيراد ملف واحد"""
    from common import ask_file

    path = ask_file("مسار ملف الإكسل المعالج:", extensions=[".xlsx"])
    if not path:
        return

    shift = ask("رمز النوبة (A/B/C أو فارغ):")
    shift = shift.upper() if shift in ("a", "b", "c", "A", "B", "C") else None

    from db.import_parsed import import_parsed

    stats = import_parsed(path, shift=shift)
    print_ok(f"تم: {stats['trips']} رحلة من {stats['rows']} صف")


def _bulk_import() -> None:
    """استيراد جماعي لكل الملفات"""
    print_warn("سيحذف القاعدة الحالية ويعيد بناءها من كل الملفات")
    confirm = ask("متأكد؟ (نعم/لا):")
    if confirm not in ("نعم", "y", "yes"):
        print_warn("تم الإلغاء")
        return

    from db.bulk_import import bulk_import

    bulk_import(rebuild=True)


def _export_to_excel() -> None:
    """تصدير نتائج استعلام لملف إكسل"""
    print_header("تصدير لإكسل")
    print(f"  {CYAN}1{RESET} رحلات تاريخ محدد")
    print(f"  {CYAN}2{RESET} رحلات وجهة محددة")
    print(f"  {CYAN}3{RESET} رحلات حملة محددة")
    print(f"  {CYAN}4{RESET} ملخص شهري")
    choice = ask("نوع التصدير:")

    from db.queries import Q

    headers = None
    rows = None
    filename = "export"

    with Q() as q:
        if choice == "1":
            date = ask("التاريخ:")
            if not date:
                return
            headers, rows = q.export_query(
                "SELECT date, shift_code, flight_number, departure_time, "
                "passenger_count, destination, visa_type, campaign_name, "
                "status, dispatch, inspection, source FROM trips WHERE date = ? ORDER BY shift_code, departure_time",
                (date,),
            )
            filename = f"{date}_export"

        elif choice == "2":
            dest = ask("الوجهة:")
            if not dest:
                return
            headers, rows = q.export_query(
                "SELECT date, shift_code, flight_number, departure_time, "
                "passenger_count, destination, campaign_name, source "
                "FROM trips WHERE UPPER(destination) LIKE UPPER(?) ORDER BY date",
                (f"%{dest}%",),
            )
            filename = f"dest_{dest}_export"

        elif choice == "3":
            campaign = ask("الحملة:")
            if not campaign:
                return
            headers, rows = q.export_query(
                "SELECT date, shift_code, flight_number, departure_time, "
                "passenger_count, destination, campaign_name, source "
                "FROM trips WHERE UPPER(campaign_name) LIKE UPPER(?) ORDER BY date",
                (f"%{campaign}%",),
            )
            filename = f"camp_{campaign}_export"

        elif choice == "4":
            prefix = ask("الشهر (مثل 2026-01):")
            if not prefix:
                return
            headers, rows = q.export_query(
                "SELECT date, COUNT(*) as trips, "
                "COALESCE(SUM(passenger_count),0) as pax, "
                "COUNT(DISTINCT flight_number) as flights, "
                "COUNT(DISTINCT destination) as destinations "
                "FROM trips WHERE date LIKE ? GROUP BY date ORDER BY date",
                (f"{prefix}%",),
            )
            filename = f"{prefix}_monthly_export"

        else:
            print_err("اختيار غير صحيح!")
            return

    if not rows:
        print_err("لا توجد بيانات للتصدير!")
        return

    # كتابة الإكسل
    try:
        import xlsxwriter
    except ImportError:
        print_err("مكتبة xlsxwriter غير مثبتة!")
        return

    # حفظ في مجلد المشروع
    out_path = str(_PROJECT_DIR / f"{filename}.xlsx")
    wb = xlsxwriter.Workbook(out_path)
    ws = wb.add_worksheet("بيانات")

    # رؤوس عربية
    header_map = {
        "date": "التاريخ",
        "shift_code": "النوبة",
        "flight_number": "رقم الرحلة",
        "departure_time": "وقت الاقلاع",
        "passenger_count": "عدد الركاب",
        "destination": "الوجهة",
        "visa_type": "الفيزا",
        "campaign_name": "اسم الحملة",
        "status": "الحالة",
        "dispatch": "التفويج",
        "inspection": "الكشف",
        "source": "المصدر",
        "trips": "الرحلات",
        "pax": "الركاب",
        "flights": "الفريدة",
        "destinations": "الوجهات",
    }
    # تنسيق الرأس
    hfmt = wb.add_format({"bold": True, "bg_color": "#4472C4", "font_color": "white", "align": "center"})
    for col, h in enumerate(headers):
        ws.write(0, col, header_map.get(h, h), hfmt)

    # البيانات
    for row_idx, row in enumerate(rows, 1):
        for col_idx, val in enumerate(row):
            ws.write(row_idx, col_idx, val)

    wb.close()
    print_ok(f"تم التصدير: {len(rows)} صف")
    print_ok(f"الملف: {out_path}")


def _delete_by_date() -> None:
    """حذف بيانات تاريخ محدد"""
    date = ask("التاريخ المراد حذفه:")
    if not date:
        return

    # عرض ما سيُحذف أولاً
    from db.queries import Q

    with Q() as q:
        counts = q.trip_count_by_date(date)
        if not counts:
            print_err("لا توجد بيانات لهذا التاريخ!")
            return

        total = sum(c["count"] for c in counts)
        print_warn(f"سيُحذف {total} رحلة:")
        for c in counts:
            print(f"  {c['source']}: {c['count']}")

    # هل نحذف مصدر محدد أو الكل؟
    source = ask("المصدر (فارغ = الكل):")

    confirm = ask("متأكد من الحذف؟ (نعم/لا):")
    if confirm not in ("نعم", "y", "yes"):
        print_warn("تم الإلغاء")
        return

    with Q() as q:
        deleted = q.delete_by_date(date, source or None)
    print_ok(f"تم حذف {deleted} رحلة")


def _delete_by_source() -> None:
    """حذف بيانات مصدر محدد"""
    from db.queries import Q

    with Q() as q:
        sources = q.available_sources()
        if not sources:
            print_err("القاعدة فارغة!")
            return

        print_warn("المصادر المتاحة:")
        for s in sources:
            print(f"  {s['source']}: {s['count']} رحلة / {s['pax']} راكب")

    source = ask("المصدر المراد حذفه:")
    if not source:
        return

    confirm = ask(f"متأكد من حذف كل رحلات {source}؟ (نعم/لا):")
    if confirm not in ("نعم", "y", "yes"):
        print_warn("تم الإلغاء")
        return

    with Q() as q:
        deleted = q.delete_by_source(source)
    print_ok(f"تم حذف {deleted} رحلة من مصدر {source}")


# ==========================================
# القائمة الرئيسية للأداة
# ==========================================

# المجموعات مع خياراتها
_GROUPS = [
    (
        "الإحصائيات",
        GREEN,
        [
            ("1", "معلومات القاعدة"),
            ("2", "تقرير يومي"),
            ("3", "ملخص شهري"),
            ("4", "إحصائيات النوبات"),
            ("5", "إحصائيات أسبوعية"),
            ("6", "مقارنة يومين"),
        ],
    ),
    (
        "البحث",
        CYAN,
        [
            ("7", "بحث برقم الرحلة"),
            ("8", "بحث بالوجهة"),
            ("9", "بحث بالحملة"),
            ("10", "بحث بالمرسل"),
            ("11", "بحث متقدم"),
        ],
    ),
    (
        "التحليل",
        ORANGE,
        [
            ("12", "أعلى 10 وجهات"),
            ("13", "أعلى 10 حملات"),
            ("14", "الرحلات المكررة"),
            ("15", "مقارنة المصادر"),
            ("16", "رحلات بدون ركاب"),
        ],
    ),
    (
        "الاستيراد والتصدير",
        DIM,
        [
            ("17", "استيراد ملف واحد"),
            ("18", "استيراد جماعي (يحذف القديم)"),
            ("19", "تصدير لإكسل"),
            ("20", "حذف بيانات تاريخ"),
            ("21", "حذف بيانات مصدر"),
        ],
    ),
]

# ربط الأرقام بالدوال
_ACTIONS = {
    "1": _show_db_info,
    "2": _daily_report,
    "3": _monthly_report,
    "4": _shift_stats,
    "5": _weekly_report,
    "6": _compare_two_dates,
    "7": _search_flight,
    "8": _search_destination,
    "9": _search_campaign,
    "10": _search_sender,
    "11": _advanced_search,
    "12": _top_destinations,
    "13": _top_campaigns,
    "14": _duplicate_flights,
    "15": _compare_sources,
    "16": _zero_pax,
    "17": _import_one,
    "18": _bulk_import,
    "19": _export_to_excel,
    "20": _delete_by_date,
    "21": _delete_by_source,
}


def run() -> str | None:
    """نقطة الدخول من القائمة الرئيسية"""
    if not _check_db():
        return "back"

    while True:
        print_header("أداة قاعدة البيانات")

        # عرض المجموعات
        for title, color, items in _GROUPS:
            print(f"\n  {color}{BOLD}{title}{RESET}")
            for num, desc in items:
                print(f"    {color}{num}{RESET}  {desc}")

        # خيار الرجوع
        print(f"\n  {RED}{BOLD}0{RESET}  رجوع")
        print()

        choice = ask("اختر:")

        if choice == "0":
            return "back"

        action = _ACTIONS.get(choice)
        if action:
            try:
                action()
            except Exception as e:
                print_err(f"خطأ: {e}")
        else:
            print_err("اختيار غير صحيح!")

        pause()
