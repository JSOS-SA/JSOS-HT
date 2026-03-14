"""اختبارات المكتبة المشتركة — الدوال النقية في common.py."""

import pytest

from common import (
    clean,
    date_to_tuple,
    extract_photos,
    extract_trips,
    parse_stamp,
    stamp_to_num,
    strip_extra,
    time_to_sec,
)


class TestClean:
    """اختبار دالة تنظيف النص من علامات الاتجاه."""

    def test_removes_rtl_mark(self) -> None:
        """التحقق من إزالة علامة الاتجاه من اليمين لليسار."""
        assert clean("\u200fنص عربي") == "نص عربي"

    def test_removes_ltr_mark(self) -> None:
        """التحقق من إزالة علامة الاتجاه من اليسار لليمين."""
        assert clean("\u200eنص عربي") == "نص عربي"

    def test_removes_zero_width_space(self) -> None:
        """التحقق من إزالة المسافة الصفرية."""
        assert clean("نص\u200bعربي") == "نصعربي"

    def test_removes_multiple_bidi_marks(self) -> None:
        """التحقق من إزالة عدة علامات اتجاه متتالية."""
        assert clean("\u200f\u200e\u200fنص") == "نص"

    def test_strips_whitespace(self) -> None:
        """التحقق من إزالة المسافات الطرفية."""
        assert clean("  نص عربي  ") == "نص عربي"

    def test_empty_string(self) -> None:
        """التحقق من التعامل مع نص فارغ."""
        assert clean("") == ""

    def test_plain_text_unchanged(self) -> None:
        """التحقق من عدم تغيير نص لا يحتوي علامات."""
        assert clean("نص عادي") == "نص عادي"


class TestParseStamp:
    """اختبار دالة تحليل طابع الواتساب الزمني."""

    def test_valid_stamp_with_brackets(self) -> None:
        """التحقق من تحليل طابع صحيح بأقواس مربعة."""
        date, time = parse_stamp("[1/21/26، 22:52:40]")
        assert date == "1/21/26"
        assert time == "22:52:40"

    def test_valid_stamp_with_comma(self) -> None:
        """التحقق من تحليل طابع بفاصلة إنجليزية."""
        date, time = parse_stamp("[2/9/26, 05:49:00]")
        assert date == "2/9/26"
        assert time == "05:49:00"

    def test_valid_stamp_with_bidi_marks(self) -> None:
        """التحقق من تحليل طابع يحتوي علامات اتجاه."""
        date, time = parse_stamp("\u200f[1/15/26، 13:45:00]\u200f")
        assert date == "1/15/26"
        assert time == "13:45:00"

    def test_invalid_stamp_returns_none(self) -> None:
        """التحقق من إرجاع قيمة فارغة لطابع غير صحيح."""
        date, time = parse_stamp("نص عشوائي")
        assert date is None
        assert time is None

    def test_empty_stamp_returns_none(self) -> None:
        """التحقق من إرجاع قيمة فارغة لنص فارغ."""
        date, time = parse_stamp("")
        assert date is None
        assert time is None


class TestStripExtra:
    """اختبار دالة حذف علامات المرفقات."""

    def test_removes_attachment_tag(self) -> None:
        """التحقق من حذف وسم المرفق."""
        result = strip_extra("نص <المُرفق: IMG-123.jpg> نص")
        assert "<المُرفق" not in result
        assert "نص" in result

    def test_removes_no_image_text(self) -> None:
        """التحقق من حذف نص عدم إدراج الصورة."""
        result = strip_extra("لم يتم إدراج الصورة")
        assert result == ""

    def test_plain_text_unchanged(self) -> None:
        """التحقق من عدم تغيير نص عادي."""
        assert strip_extra("نص عادي") == "نص عادي"


class TestTimeToSec:
    """اختبار دالة تحويل الوقت إلى ثوانٍ."""

    def test_midnight(self) -> None:
        """التحقق من تحويل منتصف الليل."""
        assert time_to_sec("00:00:00") == 0

    def test_one_hour(self) -> None:
        """التحقق من تحويل ساعة واحدة."""
        assert time_to_sec("01:00:00") == 3600

    def test_mixed_time(self) -> None:
        """التحقق من تحويل وقت مركب."""
        assert time_to_sec("05:49:00") == 5 * 3600 + 49 * 60

    def test_full_time(self) -> None:
        """التحقق من تحويل وقت كامل بالثواني."""
        assert time_to_sec("13:45:30") == 13 * 3600 + 45 * 60 + 30


class TestDateToTuple:
    """اختبار دالة تحويل التاريخ إلى ثلاثية."""

    def test_basic_date(self) -> None:
        """التحقق من تحويل تاريخ أساسي."""
        assert date_to_tuple("1/15/26") == (26, 1, 15)

    def test_double_digit_month(self) -> None:
        """التحقق من تحويل تاريخ بشهر مزدوج الأرقام."""
        assert date_to_tuple("12/31/25") == (25, 12, 31)


class TestStampToNum:
    """اختبار دالة تحويل التاريخ والوقت إلى رقم قابل للمقارنة."""

    def test_earlier_is_smaller(self) -> None:
        """التحقق من أن الأبكر أصغر رقمياً."""
        early = stamp_to_num("1/15/26", "05:00:00")
        late = stamp_to_num("1/15/26", "13:00:00")
        assert early < late

    def test_different_dates(self) -> None:
        """التحقق من أن التاريخ الأحدث أكبر رقمياً."""
        day1 = stamp_to_num("1/15/26", "12:00:00")
        day2 = stamp_to_num("1/16/26", "12:00:00")
        assert day1 < day2


class TestExtractPhotos:
    """اختبار دالة استخراج أسماء الصور."""

    def test_single_photo(self) -> None:
        """التحقق من استخراج صورة واحدة."""
        lines = ["<المُرفق: IMG-20260115-WA0001.jpg>"]
        result = extract_photos(lines)
        assert result == ["IMG-20260115-WA0001.jpg"]

    def test_multiple_photos(self) -> None:
        """التحقق من استخراج عدة صور."""
        lines = [
            "<المُرفق: IMG-001.jpg>",
            "نص عادي",
            "<المُرفق: IMG-002.jpg>",
        ]
        result = extract_photos(lines)
        assert len(result) == 2
        assert result[0] == "IMG-001.jpg"
        assert result[1] == "IMG-002.jpg"

    def test_no_photos(self) -> None:
        """التحقق من إرجاع قائمة فارغة بدون صور."""
        lines = ["نص عادي بدون صور"]
        result = extract_photos(lines)
        assert result == []

    def test_empty_lines(self) -> None:
        """التحقق من التعامل مع قائمة أسطر فارغة."""
        assert extract_photos([]) == []


class TestExtractTrips:
    """اختبار دالة استخراج الرحلات من رسالة."""

    def test_single_trip(self) -> None:
        """التحقق من استخراج رحلة واحدة."""
        msg = {
            "lines": [
                "رقم الرحلة : SV1234",
                "وقت الاقلاع : 08:30",
                "عدد الركاب : 45",
                "الوجهة : المدينة",
            ],
            "raw_lines": [],
        }
        trips = extract_trips(msg)
        assert trips is not None
        assert len(trips) == 1
        assert trips[0]["رقم الرحلة"] == "SV1234"
        assert trips[0]["عدد الركاب"] == "45"

    def test_deleted_message_returns_none(self) -> None:
        """التحقق من تجاهل الرسائل المحذوفة."""
        msg = {
            "lines": ["تم حذف هذه الرسالة"],
            "raw_lines": [],
        }
        assert extract_trips(msg) is None

    def test_empty_lines_returns_none(self) -> None:
        """التحقق من إرجاع فارغ لرسالة بدون محتوى."""
        msg = {"lines": [], "raw_lines": []}
        assert extract_trips(msg) is None

    def test_no_flight_number_returns_none(self) -> None:
        """التحقق من تجاهل رسالة بدون رقم رحلة."""
        msg = {
            "lines": ["نص عشوائي لا يحتوي بيانات رحلة"],
            "raw_lines": [],
        }
        assert extract_trips(msg) is None

    def test_two_trips_in_one_message(self) -> None:
        """التحقق من استخراج رحلتين من رسالة واحدة."""
        msg = {
            "lines": [
                "رقم الرحلة : SV100",
                "عدد الركاب : 20",
                "الوجهة : مكة",
                "رقم الرحلة : SV200",
                "عدد الركاب : 30",
                "الوجهة : المدينة",
            ],
            "raw_lines": [],
        }
        trips = extract_trips(msg)
        assert trips is not None
        assert len(trips) == 2
        assert trips[0]["رقم الرحلة"] == "SV100"
        assert trips[1]["رقم الرحلة"] == "SV200"
