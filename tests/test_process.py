"""اختبارات سكربت معالجة رسائل الواتساب — parse_messages و extract_trips."""

import tempfile
from pathlib import Path

from common import extract_trips, parse_messages


class TestParseMessages:
    """اختبار دالة قراءة الملف واستخراج الرسائل في النطاق الزمني."""

    def _write_chat_file(self, content: str) -> str:
        """كتابة محتوى محادثة واتساب في ملف مؤقت وإرجاع مساره."""
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".txt",
            delete=False,
        )
        tmp.write(content)
        tmp.close()
        return tmp.name

    def test_single_message_in_range(self) -> None:
        """التحقق من استخراج رسالة واحدة ضمن النطاق."""
        chat = (
            "[1/15/26، 06:00:00] أحمد: رقم الرحلة : SV100\n"
            "عدد الركاب : 45\n"
        )
        path = self._write_chat_file(chat)
        try:
            msgs = parse_messages(path, "1/15/26", "05:00:00", "1/15/26", "23:59:59")
            assert len(msgs) == 1
            assert msgs[0]["sender"] == "أحمد"
            assert msgs[0]["date"] == "1/15/26"
        finally:
            Path(path).unlink()

    def test_message_outside_range_excluded(self) -> None:
        """التحقق من استبعاد رسالة خارج النطاق الزمني."""
        chat = (
            "[1/15/26، 04:00:00] أحمد: رسالة مبكرة\n"
            "[1/15/26، 06:00:00] سعيد: رقم الرحلة : SV200\n"
        )
        path = self._write_chat_file(chat)
        try:
            msgs = parse_messages(path, "1/15/26", "05:00:00", "1/15/26", "23:59:59")
            assert len(msgs) == 1
            assert msgs[0]["sender"] == "سعيد"
        finally:
            Path(path).unlink()

    def test_empty_file_returns_empty(self) -> None:
        """التحقق من إرجاع قائمة فارغة لملف فارغ."""
        path = self._write_chat_file("")
        try:
            msgs = parse_messages(path, "1/15/26", "05:00:00", "1/15/26", "23:59:59")
            assert msgs == []
        finally:
            Path(path).unlink()

    def test_multiline_message(self) -> None:
        """التحقق من ضم أسطر متعددة لنفس الرسالة."""
        chat = (
            "[1/15/26، 06:00:00] محمد: رقم الرحلة : SV300\n"
            "عدد الركاب : 50\n"
            "الوجهة : مكة\n"
        )
        path = self._write_chat_file(chat)
        try:
            msgs = parse_messages(path, "1/15/26", "05:00:00", "1/15/26", "23:59:59")
            assert len(msgs) == 1
            assert len(msgs[0]["lines"]) == 3
        finally:
            Path(path).unlink()


class TestExtractTripsIntegration:
    """اختبار تكاملي — استخراج رحلات من رسالة محللة."""

    def test_trip_with_all_fields(self) -> None:
        """التحقق من استخراج رحلة كاملة الحقول."""
        msg = {
            "lines": [
                "رقم الرحلة : SV500",
                "وقت الاقلاع : 08:30",
                "عدد الركاب : 120",
                "الوجهة : المدينة المنورة",
                "الفيزا : حج",
                "الحملة : حملة الراجحي",
                "الحالة : وصل",
                "التفويج : تم",
                "الكشف : تم",
            ],
            "raw_lines": [],
        }
        trips = extract_trips(msg)
        assert trips is not None
        assert len(trips) == 1
        trip = trips[0]
        assert trip["رقم الرحلة"] == "SV500"
        assert trip["عدد الركاب"] == "120"
        assert trip["الوجهة"] == "المدينة المنورة"

    def test_attachment_only_message(self) -> None:
        """التحقق من تجاهل رسالة تحتوي مرفق فقط."""
        msg = {
            "lines": ["<المُرفق: IMG-001.jpg>"],
            "raw_lines": [],
        }
        assert extract_trips(msg) is None
