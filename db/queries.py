"""
استعلامات قاعدة البيانات — واجهة موحدة لكل السكربتات
بدلاً من فتح إكسل وتكرار الحسابات، استعلام واحد يكفي

الاستخدام:
    from db.queries import Q
    q = Q()
    q.trips_by_date("1/15/26")
    q.stats_by_shift("A", "1/15/26")
    q.close()
"""

import os
import sys

_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

from logger_config import log_db_op
from db.schema import get_connection, DB_PATH


class Q:
    """واجهة الاستعلامات — كل دالة تُرجع قائمة قواميس"""

    def __init__(self, db_path=None):
        self.conn = get_connection(db_path)

    def close(self):
        """إغلاق الاتصال"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _fetch(self, sql, params=()):
        """تنفيذ استعلام وإرجاع النتائج كقواميس"""
        rows = self.conn.execute(sql, params).fetchall()
        # تحويل sqlite3.Row إلى قاموس عادي
        return [dict(r) for r in rows]

    def _fetchone(self, sql, params=()):
        """تنفيذ استعلام وإرجاع صف واحد كقاموس"""
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    # ==========================================
    # استعلامات الرحلات
    # ==========================================

    def trips_by_date(self, date, source=None):
        """كل الرحلات في تاريخ معين — مع فلتر اختياري للمصدر"""
        if source:
            return self._fetch(
                "SELECT * FROM trips WHERE date = ? AND source = ? ORDER BY departure_time",
                (date, source),
            )
        return self._fetch(
            "SELECT * FROM trips WHERE date = ? ORDER BY source, departure_time",
            (date,),
        )

    def trips_by_shift(self, date, shift_code, source=None):
        """رحلات تاريخ ونوبة محددة"""
        if source:
            return self._fetch(
                "SELECT * FROM trips WHERE date = ? AND shift_code = ? AND source = ? ORDER BY departure_time",
                (date, shift_code, source),
            )
        return self._fetch(
            "SELECT * FROM trips WHERE date = ? AND shift_code = ? ORDER BY source, departure_time",
            (date, shift_code),
        )

    def trips_by_flight(self, flight_number):
        """كل سجلات رحلة معينة من كل المصادر — للمقارنة السريعة"""
        return self._fetch(
            "SELECT * FROM trips WHERE UPPER(flight_number) = UPPER(?) ORDER BY date, source",
            (flight_number,),
        )

    def trip_count_by_date(self, date):
        """عدد الرحلات لكل مصدر في تاريخ معين"""
        return self._fetch("""
            SELECT source, COUNT(*) as count, SUM(passenger_count) as total_pax
            FROM trips WHERE date = ?
            GROUP BY source
        """, (date,))

    # ==========================================
    # الإحصائيات — بديل stats.py
    # ==========================================

    def stats_by_shift(self, shift_code, date=None, source=None):
        """إحصائيات نوبة — رحلات، ركاب، وجهات
        source: فلتر المصدر (اختياري — بدونه يجمع كل المصادر)"""
        where = "WHERE shift_code = ?"
        params = [shift_code]
        if date:
            where += " AND date = ?"
            params.append(date)
        if source:
            where += " AND source = ?"
            params.append(source)

        return self._fetchone(f"""
            SELECT
                COUNT(*)                    as total_trips,
                COALESCE(SUM(passenger_count), 0) as total_pax,
                COUNT(DISTINCT flight_number) as unique_flights,
                COUNT(DISTINCT destination)   as unique_destinations
            FROM trips
            {where}
        """, params)

    def stats_overview(self, date):
        """ملخص شامل ليوم كامل — كل المصادر والنوبات"""
        return self._fetch("""
            SELECT
                shift_code,
                source,
                COUNT(*)                    as trips,
                COALESCE(SUM(passenger_count), 0) as pax,
                COUNT(DISTINCT flight_number) as flights
            FROM trips
            WHERE date = ?
            GROUP BY shift_code, source
            ORDER BY shift_code, source
        """, (date,))

    def unique_values(self, column, date=None, source=None):
        """القيم الفريدة لعمود معين — وجهات، فيزا، حملات، إلخ
        column: اسم العمود في جدول trips (مثل destination, visa_type)"""
        # حماية من حقن SQL — نتحقق من اسم العمود
        allowed = {
            "destination", "visa_type", "campaign_name", "status",
            "dispatch", "inspection", "flight_number",
        }
        if column not in allowed:
            raise ValueError(f"عمود غير مسموح: {column}")

        where_parts = [f"{column} IS NOT NULL AND {column} != ''"]
        params = []
        if date:
            where_parts.append("date = ?")
            params.append(date)
        if source:
            where_parts.append("source = ?")
            params.append(source)

        where = " AND ".join(where_parts)
        return self._fetch(f"""
            SELECT {column} as value, COUNT(*) as count
            FROM trips
            WHERE {where}
            GROUP BY {column}
            ORDER BY count DESC
        """, params)

    def empty_fields(self, date, source="whatsapp"):
        """الحقول الفارغة لكل عمود — بديل حساب empty_count في stats.py"""
        return self._fetchone("""
            SELECT
                SUM(CASE WHEN flight_number   = '' OR flight_number   IS NULL THEN 1 ELSE 0 END) as empty_flight,
                SUM(CASE WHEN departure_time  = '' OR departure_time  IS NULL THEN 1 ELSE 0 END) as empty_time,
                SUM(CASE WHEN passenger_count = 0                             THEN 1 ELSE 0 END) as empty_pax,
                SUM(CASE WHEN destination     = '' OR destination     IS NULL THEN 1 ELSE 0 END) as empty_dest,
                SUM(CASE WHEN visa_type       = '' OR visa_type       IS NULL THEN 1 ELSE 0 END) as empty_visa,
                SUM(CASE WHEN campaign_name   = '' OR campaign_name   IS NULL THEN 1 ELSE 0 END) as empty_campaign,
                SUM(CASE WHEN status          = '' OR status          IS NULL THEN 1 ELSE 0 END) as empty_status,
                SUM(CASE WHEN dispatch        = '' OR dispatch        IS NULL THEN 1 ELSE 0 END) as empty_dispatch,
                SUM(CASE WHEN inspection      = '' OR inspection      IS NULL THEN 1 ELSE 0 END) as empty_inspection,
                COUNT(*) as total
            FROM trips
            WHERE date = ? AND source = ?
        """, (date, source))

    # ==========================================
    # المقارنة — بديل compare.py
    # ==========================================

    def compare_sources(self, date, shift_code=None):
        """مقارنة أرقام الرحلات بين المصادر المختلفة"""
        where = "WHERE date = ?"
        params = [date]
        if shift_code:
            where += " AND shift_code = ?"
            params.append(shift_code)

        # رحلات كل مصدر
        sources = self._fetch(f"""
            SELECT source, GROUP_CONCAT(DISTINCT UPPER(flight_number)) as flights
            FROM trips
            {where} AND flight_number != ''
            GROUP BY source
        """, params)

        result = {}
        for s in sources:
            flights_str = s["flights"] or ""
            result[s["source"]] = set(flights_str.split(",")) if flights_str else set()

        return result

    def flights_only_in(self, date, source):
        """رحلات موجودة في مصدر واحد فقط — غائبة عن البقية"""
        all_sources = self.compare_sources(date)
        target = all_sources.get(source, set())
        others = set()
        for s, flights in all_sources.items():
            if s != source:
                others |= flights
        only = target - others
        return sorted(only)

    def flight_count_mismatch(self, date):
        """رحلات تتكرر بعدد مختلف بين المصادر"""
        return self._fetch("""
            SELECT flight_number, source, COUNT(*) as count
            FROM trips
            WHERE date = ? AND flight_number != ''
            GROUP BY flight_number, source
            ORDER BY flight_number, source
        """, (date,))

    # ==========================================
    # المرسلين والرسائل
    # ==========================================

    def senders_stats(self, date=None):
        """إحصائيات المرسلين — عدد الرسائل والرحلات لكل مرسل"""
        if date:
            return self._fetch("""
                SELECT s.name,
                       COUNT(DISTINCT m.id) as messages,
                       COUNT(t.id) as trips,
                       COALESCE(SUM(t.passenger_count), 0) as total_pax
                FROM senders s
                JOIN messages m ON m.sender_id = s.id
                LEFT JOIN trips t ON t.message_id = m.id
                WHERE m.date = ?
                GROUP BY s.id
                ORDER BY trips DESC
            """, (date,))
        return self._fetch("""
            SELECT s.name,
                   COUNT(DISTINCT m.id) as messages,
                   COUNT(t.id) as trips,
                   COALESCE(SUM(t.passenger_count), 0) as total_pax
            FROM senders s
            JOIN messages m ON m.sender_id = s.id
            LEFT JOIN trips t ON t.message_id = m.id
            GROUP BY s.id
            ORDER BY trips DESC
        """)

    def messages_by_date(self, date):
        """عدد الرسائل حسب النوع لتاريخ معين"""
        return self._fetchone("""
            SELECT
                COUNT(*) as total,
                SUM(has_photos) as with_photos,
                COUNT(DISTINCT sender_id) as unique_senders
            FROM messages
            WHERE date = ?
        """, (date,))

    # ==========================================
    # التسجيل (RECORD)
    # ==========================================

    def records_by_date(self, date, shift_code=None):
        """بيانات التسجيل لتاريخ معين"""
        if shift_code:
            return self._fetch(
                "SELECT * FROM records WHERE date = ? AND shift_code = ? ORDER BY row_num",
                (date, shift_code),
            )
        return self._fetch(
            "SELECT * FROM records WHERE date = ? ORDER BY shift_code, row_num",
            (date,),
        )

    def record_stats(self, date, shift_code=None):
        """إحصائيات التسجيل — تجميع من كل خانات الرحلات (1-10)."""
        where = "WHERE date = ?"
        params = [date]
        if shift_code:
            where += " AND shift_code = ?"
            params.append(shift_code)

        # تجميع الركاب من كل الخانات العشر
        pax_sum = " + ".join(
            f"COALESCE(passenger_count_{n}, 0)" for n in range(1, 11)
        )

        # الرحلات الفريدة — استعلام فرعي لكل خانة
        flight_unions = " UNION ".join(
            f"SELECT flight_number_{n} as fn FROM records "
            f"{where} AND flight_number_{n} IS NOT NULL "
            f"AND flight_number_{n} != ''"
            for n in range(1, 11)
        )

        # الوجهات الفريدة
        dest_unions = " UNION ".join(
            f"SELECT destination_{n} as dn FROM records "
            f"{where} AND destination_{n} IS NOT NULL "
            f"AND destination_{n} != ''"
            for n in range(1, 11)
        )

        # الاستعلام الرئيسي
        row = self._fetchone(f"""
            SELECT
                COUNT(*) as total_rows,
                COALESCE(SUM(trip_count), 0) as total_trips,
                COALESCE(SUM({pax_sum}), 0) as total_pax
            FROM records
            {where}
        """, params)

        # عدد الرحلات الفريدة
        flights_count = self._fetchone(
            f"SELECT COUNT(DISTINCT fn) as c FROM ({flight_unions})",
            params * 10,
        )
        # عدد الوجهات الفريدة
        dests_count = self._fetchone(
            f"SELECT COUNT(DISTINCT dn) as c FROM ({dest_unions})",
            params * 10,
        )

        if row:
            row["unique_flights"] = (flights_count or {}).get("c", 0)
            row["unique_destinations"] = (dests_count or {}).get("c", 0)

        return row

    # ==========================================
    # جلسات المقارنة والاختلافات
    # ==========================================

    def comparison_sessions(self, date=None):
        """قائمة جلسات المقارنة"""
        if date:
            return self._fetch(
                "SELECT * FROM comparison_sessions WHERE date = ? ORDER BY created_at DESC",
                (date,),
            )
        return self._fetch(
            "SELECT * FROM comparison_sessions ORDER BY created_at DESC LIMIT 20"
        )

    def diffs_by_session(self, session_id, status=None):
        """الاختلافات في جلسة مقارنة"""
        if status:
            return self._fetch(
                "SELECT * FROM comparison_diffs WHERE session_id = ? AND status = ? ORDER BY id",
                (session_id, status),
            )
        return self._fetch(
            "SELECT * FROM comparison_diffs WHERE session_id = ? ORDER BY id",
            (session_id,),
        )

    def diff_summary(self, session_id):
        """ملخص الاختلافات حسب النوع"""
        return self._fetch("""
            SELECT type, status, COUNT(*) as count
            FROM comparison_diffs
            WHERE session_id = ?
            GROUP BY type, status
            ORDER BY type
        """, (session_id,))

    # ==========================================
    # سجل عمليات المعالجة
    # ==========================================

    def processing_history(self, limit=20):
        """آخر عمليات المعالجة"""
        return self._fetch("""
            SELECT * FROM processing_runs
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))

    # ==========================================
    # استعلامات تجميعية متقدمة
    # ==========================================

    def daily_summary(self, date, source=None):
        """ملخص يومي شامل — رحلات + ركاب + وجهات لكل نوبة"""
        where = "WHERE date = ?"
        params = [date]
        if source:
            where += " AND source = ?"
            params.append(source)
        return self._fetch(f"""
            SELECT
                COALESCE(shift_code, '?') as shift,
                COUNT(*) as trips,
                COALESCE(SUM(passenger_count), 0) as pax,
                COUNT(DISTINCT destination) as destinations,
                COUNT(DISTINCT flight_number) as flights
            FROM trips
            {where}
            GROUP BY shift_code
            ORDER BY shift_code
        """, params)

    def monthly_summary(self, year_month_prefix, source=None):
        """ملخص شهري — يدعم صيغتين:
        '1/' لصيغة m/d/yy أو '2026-01' لصيغة YYYY-MM-DD"""
        where = "WHERE date LIKE ?"
        params = [f"{year_month_prefix}%"]
        if source:
            where += " AND source = ?"
            params.append(source)
        return self._fetch(f"""
            SELECT
                date,
                COUNT(*) as trips,
                COALESCE(SUM(passenger_count), 0) as pax,
                COUNT(DISTINCT flight_number) as flights
            FROM trips
            {where}
            GROUP BY date
            ORDER BY date
        """, params)

    def top_destinations(self, date=None, limit=10, source=None):
        """أكثر الوجهات تكراراً"""
        where_parts = ["destination != ''"]
        params = []
        if date:
            where_parts.append("date = ?")
            params.append(date)
        if source:
            where_parts.append("source = ?")
            params.append(source)
        where = " AND ".join(where_parts)
        params.append(limit)
        return self._fetch(f"""
            SELECT destination as value, COUNT(*) as count,
                   COALESCE(SUM(passenger_count), 0) as total_pax
            FROM trips
            WHERE {where}
            GROUP BY destination ORDER BY count DESC LIMIT ?
        """, params)

    def shared_flights(self, date, source=None):
        """الرحلات المشتركة (تظهر أكثر من مرة) في تاريخ معين"""
        where = "WHERE date = ? AND flight_number != ''"
        params = [date]
        if source:
            where += " AND source = ?"
            params.append(source)
        return self._fetch(f"""
            SELECT flight_number, COUNT(*) as count
            FROM trips
            {where}
            GROUP BY flight_number
            HAVING count > 1
            ORDER BY count DESC
        """, params)

    # ==========================================
    # استعلامات إضافية — أداة القاعدة الموسعة
    # ==========================================

    def trips_by_destination(self, destination, date=None):
        """بحث بالوجهة — يدعم البحث الجزئي"""
        where = "WHERE UPPER(destination) LIKE UPPER(?)"
        params = [f"%{destination}%"]
        if date:
            where += " AND date = ?"
            params.append(date)
        return self._fetch(f"""
            SELECT date, shift_code, flight_number, departure_time,
                   passenger_count, destination, campaign_name, source
            FROM trips {where}
            ORDER BY date DESC, departure_time
        """, params)

    def trips_by_campaign(self, campaign, date=None):
        """بحث بالحملة — يدعم البحث الجزئي"""
        where = "WHERE UPPER(campaign_name) LIKE UPPER(?)"
        params = [f"%{campaign}%"]
        if date:
            where += " AND date = ?"
            params.append(date)
        return self._fetch(f"""
            SELECT date, shift_code, flight_number, departure_time,
                   passenger_count, destination, campaign_name, source
            FROM trips {where}
            ORDER BY date DESC, departure_time
        """, params)

    def trips_by_sender_name(self, sender_name):
        """بحث بالمرسل — يدعم البحث الجزئي عبر جدول الرسائل"""
        return self._fetch("""
            SELECT t.date, t.shift_code, t.flight_number, t.departure_time,
                   t.passenger_count, t.destination, s.name as sender
            FROM trips t
            JOIN messages m ON t.message_id = m.id
            JOIN senders s ON m.sender_id = s.id
            WHERE UPPER(s.name) LIKE UPPER(?)
            ORDER BY t.date DESC, t.departure_time
        """, (f"%{sender_name}%",))

    def advanced_search(self, date=None, shift=None, flight=None,
                        destination=None, campaign=None, source=None,
                        min_pax=None, max_pax=None):
        """بحث متقدم بعدة معايير — كل المعايير اختيارية"""
        where_parts = []
        params = []
        if date:
            where_parts.append("date = ?")
            params.append(date)
        if shift:
            where_parts.append("shift_code = ?")
            params.append(shift)
        if flight:
            where_parts.append("UPPER(flight_number) LIKE UPPER(?)")
            params.append(f"%{flight}%")
        if destination:
            where_parts.append("UPPER(destination) LIKE UPPER(?)")
            params.append(f"%{destination}%")
        if campaign:
            where_parts.append("UPPER(campaign_name) LIKE UPPER(?)")
            params.append(f"%{campaign}%")
        if source:
            where_parts.append("source = ?")
            params.append(source)
        if min_pax is not None:
            where_parts.append("passenger_count >= ?")
            params.append(min_pax)
        if max_pax is not None:
            where_parts.append("passenger_count <= ?")
            params.append(max_pax)

        where = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        return self._fetch(f"""
            SELECT date, shift_code, flight_number, departure_time,
                   passenger_count, destination, visa_type,
                   campaign_name, status, source
            FROM trips {where}
            ORDER BY date DESC, departure_time
            LIMIT 100
        """, params)

    def top_campaigns(self, date=None, limit=10):
        """أعلى الحملات تكراراً"""
        where_parts = ["campaign_name != '' AND campaign_name IS NOT NULL"]
        params = []
        if date:
            where_parts.append("date = ?")
            params.append(date)
        where = " AND ".join(where_parts)
        params.append(limit)
        return self._fetch(f"""
            SELECT campaign_name as value, COUNT(*) as count,
                   COALESCE(SUM(passenger_count), 0) as total_pax
            FROM trips
            WHERE {where}
            GROUP BY campaign_name ORDER BY count DESC LIMIT ?
        """, params)

    def duplicate_flights(self, date=None, min_count=2):
        """الرحلات المكررة — نفس الرقم أكثر من مرة في نفس المصدر"""
        where = "WHERE flight_number != ''"
        params = []
        if date:
            where += " AND date = ?"
            params.append(date)
        params.append(min_count)
        return self._fetch(f"""
            SELECT date, source, flight_number, COUNT(*) as count,
                   COALESCE(SUM(passenger_count), 0) as total_pax
            FROM trips
            {where}
            GROUP BY date, source, flight_number
            HAVING count >= ?
            ORDER BY count DESC, date DESC
            LIMIT 100
        """, params)

    def zero_pax_trips(self, date=None):
        """رحلات بدون ركاب — صفر أو فارغ"""
        where = "WHERE (passenger_count = 0 OR passenger_count IS NULL)"
        params = []
        if date:
            where += " AND date = ?"
            params.append(date)
        return self._fetch(f"""
            SELECT date, shift_code, flight_number, departure_time,
                   destination, campaign_name, source
            FROM trips {where}
            ORDER BY date DESC, departure_time
            LIMIT 200
        """, params)

    def weekly_summary(self, end_date):
        """ملخص أسبوعي — آخر 7 أيام من التاريخ المحدد"""
        return self._fetch("""
            SELECT date,
                   COUNT(*) as trips,
                   COALESCE(SUM(passenger_count), 0) as pax,
                   COUNT(DISTINCT flight_number) as flights,
                   COUNT(DISTINCT destination) as destinations
            FROM trips
            WHERE date <= ? AND date >= date(?, '-6 days')
            GROUP BY date
            ORDER BY date
        """, (end_date, end_date))

    def compare_two_dates(self, date1, date2):
        """مقارنة يومين — إحصائيات جنباً إلى جنب"""
        # إحصائيات اليوم الأول
        d1 = self._fetchone("""
            SELECT COUNT(*) as trips,
                   COALESCE(SUM(passenger_count), 0) as pax,
                   COUNT(DISTINCT flight_number) as flights,
                   COUNT(DISTINCT destination) as destinations
            FROM trips WHERE date = ?
        """, (date1,))
        # إحصائيات اليوم الثاني
        d2 = self._fetchone("""
            SELECT COUNT(*) as trips,
                   COALESCE(SUM(passenger_count), 0) as pax,
                   COUNT(DISTINCT flight_number) as flights,
                   COUNT(DISTINCT destination) as destinations
            FROM trips WHERE date = ?
        """, (date2,))
        # رحلات فريدة لكل يوم
        f1 = set(r["flight_number"] for r in self._fetch(
            "SELECT DISTINCT flight_number FROM trips WHERE date = ? AND flight_number != ''",
            (date1,)))
        f2 = set(r["flight_number"] for r in self._fetch(
            "SELECT DISTINCT flight_number FROM trips WHERE date = ? AND flight_number != ''",
            (date2,)))
        return {
            "date1": {"date": date1, **(d1 or {})},
            "date2": {"date": date2, **(d2 or {})},
            "only_date1": sorted(f1 - f2),
            "only_date2": sorted(f2 - f1),
            "common": sorted(f1 & f2),
        }

    def shift_stats_all(self, date=None):
        """إحصائيات كل النوبات دفعة واحدة"""
        where = "WHERE date = ?" if date else ""
        params = [date] if date else []
        return self._fetch(f"""
            SELECT
                COALESCE(shift_code, '?') as shift,
                COUNT(*) as trips,
                COALESCE(SUM(passenger_count), 0) as pax,
                COUNT(DISTINCT flight_number) as flights,
                COUNT(DISTINCT destination) as destinations,
                COUNT(DISTINCT date) as days
            FROM trips
            {where}
            GROUP BY shift_code
            ORDER BY shift_code
        """, params)

    def delete_by_date(self, date, source=None):
        """حذف رحلات تاريخ معين — يُرجع عدد المحذوف"""
        if source:
            count = self.conn.execute(
                "SELECT COUNT(*) FROM trips WHERE date = ? AND source = ?",
                (date, source)).fetchone()[0]
            self.conn.execute(
                "DELETE FROM trips WHERE date = ? AND source = ?",
                (date, source))
        else:
            count = self.conn.execute(
                "SELECT COUNT(*) FROM trips WHERE date = ?",
                (date,)).fetchone()[0]
            self.conn.execute("DELETE FROM trips WHERE date = ?", (date,))
        self.conn.commit()
        # تسجيل الحذف — عملية خطيرة
        log_db_op(f"حذف رحلات: {date} ({source or 'الكل'}) = {count} صف",
                  operation="delete", query_summary="trips",
                  rows_affected=count,
                  script_name="queries", func_name="delete_by_date")
        return count

    def delete_by_source(self, source):
        """حذف كل رحلات مصدر معين — يُرجع عدد المحذوف"""
        count = self.conn.execute(
            "SELECT COUNT(*) FROM trips WHERE source = ?",
            (source,)).fetchone()[0]
        self.conn.execute("DELETE FROM trips WHERE source = ?", (source,))
        self.conn.commit()
        log_db_op(f"حذف رحلات مصدر: {source} = {count} صف",
                  operation="delete", query_summary="trips",
                  rows_affected=count,
                  script_name="queries", func_name="delete_by_source")
        return count

    def available_dates(self, limit=30):
        """آخر التواريخ المتاحة في القاعدة"""
        return self._fetch("""
            SELECT date, COUNT(*) as trips,
                   COALESCE(SUM(passenger_count), 0) as pax
            FROM trips
            GROUP BY date
            ORDER BY date DESC
            LIMIT ?
        """, (limit,))

    def available_sources(self):
        """المصادر المتاحة مع عدد الرحلات"""
        return self._fetch("""
            SELECT source, COUNT(*) as count,
                   COALESCE(SUM(passenger_count), 0) as pax
            FROM trips GROUP BY source ORDER BY count DESC
        """)

    def export_query(self, sql, params=()):
        """تنفيذ استعلام مخصص للتصدير — يُرجع رؤوس وصفوف"""
        cur = self.conn.execute(sql, params)
        headers = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return headers, [list(r) for r in rows]


# === التشغيل المباشر — عرض الاستعلامات المتاحة ===
if __name__ == "__main__":
    print("واجهة الاستعلامات — الدوال المتاحة:\n")
    # عرض كل الدوال العامة مع وصفها
    for name in sorted(dir(Q)):
        if name.startswith("_"):
            continue
        method = getattr(Q, name)
        if callable(method) and method.__doc__:
            # أول سطر من التوثيق فقط
            doc = method.__doc__.strip().split("\n")[0]
            print(f"  {name}: {doc}")
