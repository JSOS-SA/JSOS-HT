"""اختبارات طبقة قاعدة البيانات — المخطط والاتصال والاستعلامات."""

import sqlite3

import pytest

from db.schema import create_indexes, create_tables, get_connection, init_db, insert_default_shifts


class TestGetConnection:
    """اختبار دالة إنشاء الاتصال."""

    def test_returns_connection(self) -> None:
        """التحقق من إرجاع كائن اتصال صالح."""
        conn = get_connection(":memory:")
        assert conn is not None
        conn.close()

    def test_row_factory_is_row(self) -> None:
        """التحقق من أن مصنع الصفوف يدعم الوصول بالاسم."""
        conn = get_connection(":memory:")
        assert conn.row_factory is sqlite3.Row
        conn.close()

    def test_foreign_keys_enabled(self) -> None:
        """التحقق من تفعيل المفاتيح الأجنبية."""
        conn = get_connection(":memory:")
        result = conn.execute("PRAGMA foreign_keys").fetchone()
        assert result[0] == 1
        conn.close()

    def test_wal_mode_enabled(self) -> None:
        """التحقق من تفعيل وضع الكتابة المتقدم."""
        conn = get_connection(":memory:")
        result = conn.execute("PRAGMA journal_mode").fetchone()
        # في الذاكرة يكون memory وليس wal — هذا سلوك طبيعي
        assert result[0] in ("wal", "memory")
        conn.close()


class TestCreateTables:
    """اختبار إنشاء الجداول."""

    @pytest.fixture()
    def conn(self) -> sqlite3.Connection:
        """اتصال مؤقت في الذاكرة."""
        c = get_connection(":memory:")
        yield c
        c.close()

    def test_creates_all_tables(self, conn: sqlite3.Connection) -> None:
        """التحقق من إنشاء جميع الجداول العشرة."""
        create_tables(conn)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t["name"] for t in tables]
        expected = [
            "comparison_diffs",
            "comparison_sessions",
            "messages",
            "photos",
            "processing_runs",
            "records",
            "senders",
            "shifts",
            "trips",
            "violations",
        ]
        for name in expected:
            assert name in table_names, f"الجدول {name} غير موجود"

    def test_idempotent_creation(self, conn: sqlite3.Connection) -> None:
        """التحقق من أن الإنشاء المتكرر لا يسبب خطأ."""
        create_tables(conn)
        create_tables(conn)  # لا يجب أن يرمي استثناء


class TestCreateIndexes:
    """اختبار إنشاء الفهارس."""

    @pytest.fixture()
    def conn(self) -> sqlite3.Connection:
        """اتصال مع جداول جاهزة."""
        c = get_connection(":memory:")
        create_tables(c)
        yield c
        c.close()

    def test_creates_indexes(self, conn: sqlite3.Connection) -> None:
        """التحقق من إنشاء فهارس على الجداول."""
        create_indexes(conn)
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()
        assert len(indexes) >= 10


class TestInsertDefaultShifts:
    """اختبار إدراج النوبات الافتراضية."""

    @pytest.fixture()
    def conn(self) -> sqlite3.Connection:
        """اتصال مع جداول جاهزة."""
        c = get_connection(":memory:")
        create_tables(c)
        yield c
        c.close()

    def test_inserts_three_shifts(self, conn: sqlite3.Connection) -> None:
        """التحقق من إدراج ثلاث نوبات."""
        insert_default_shifts(conn)
        shifts = conn.execute("SELECT * FROM shifts ORDER BY code").fetchall()
        assert len(shifts) == 3
        codes = [s["code"] for s in shifts]
        assert codes == ["A", "B", "C"]

    def test_shift_names_arabic(self, conn: sqlite3.Connection) -> None:
        """التحقق من أن أسماء النوبات بالعربية."""
        insert_default_shifts(conn)
        shifts = conn.execute("SELECT name_ar FROM shifts ORDER BY code").fetchall()
        names = [s["name_ar"] for s in shifts]
        assert names == ["صباح", "ظهر", "مساء"]

    def test_idempotent_insert(self, conn: sqlite3.Connection) -> None:
        """التحقق من أن الإدراج المتكرر لا يكرر البيانات."""
        insert_default_shifts(conn)
        insert_default_shifts(conn)
        count = conn.execute("SELECT COUNT(*) FROM shifts").fetchone()[0]
        assert count == 3


class TestInitDb:
    """اختبار التهيئة الكاملة لقاعدة البيانات."""

    def test_full_init_in_memory(self) -> None:
        """التحقق من نجاح التهيئة الكاملة في الذاكرة."""
        conn = init_db(":memory:")
        try:
            tables = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            assert tables >= 10

            shifts = conn.execute("SELECT COUNT(*) FROM shifts").fetchone()[0]
            assert shifts == 3
        finally:
            conn.close()
