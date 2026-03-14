#!/usr/bin/env python3
"""نظام التقارير التفاعلي — مشروع JEDCO HT_SC.

ملف مستقل يولّد تقارير PDF من بيانات إكسل (ورقة RECORD)
يستخدم Plotly للرسوم البيانية و reportlab لبناء PDF
"""

import argparse
import io
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

# ضبط ترميز الطرفية لدعم العربية على ويندوز
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import contextlib

import arabic_reshaper
import pandas as pd
import plotly.graph_objects as go
from bidi.algorithm import get_display
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# ═══════════════════════════════════════════════════════
# الجزء 2: الثوابت وإعداد الخط العربي
# ═══════════════════════════════════════════════════════

# ألوان JEDCO الرسمية
NAVY = "#1B2A4A"  # كحلي — العناوين والرؤوس
BLUE = "#2E5090"  # أزرق — عناصر ثانوية
TEAL = "#3AAFA9"  # تركوازي — تمييز
GOLD = "#D4A843"  # ذهبي — تمييز ثانوي
GRAY = "#6C757D"  # رمادي — نصوص فرعية
WHITE = "#FFFFFF"
LIGHT_BG = "#F8F9FA"  # خلفية فاتحة للصفوف المتبادلة

# ألوان النوبات
SHIFT_COLORS = {
    "A": "#2E5090",
    "B": "#D4A843",
    "C": "#3AAFA9",
}

# تسجيل الخط العربي — نستخدم Tahoma لدعم العربية
FONT_PATH = r"C:\Windows\Fonts\tahoma.ttf"
FONT_BOLD_PATH = r"C:\Windows\Fonts\tahomabd.ttf"

if os.path.exists(FONT_PATH):
    pdfmetrics.registerFont(TTFont("Arabic", FONT_PATH))
    pdfmetrics.registerFont(TTFont("ArabicBold", FONT_BOLD_PATH))
else:
    # احتياطي — Arial
    pdfmetrics.registerFont(TTFont("Arabic", r"C:\Windows\Fonts\arial.ttf"))
    pdfmetrics.registerFont(TTFont("ArabicBold", r"C:\Windows\Fonts\arialbd.ttf"))


def arabic(text: str) -> str:
    """معالجة النص العربي ليُعرض بشكل صحيح في PDF — إعادة تشكيل + اتجاه"""
    if not text:
        return ""
    reshaped = arabic_reshaper.reshape(str(text))
    return get_display(reshaped)


def _is_arabic(text: str) -> bool:
    """كشف وجود حروف عربية في النص"""
    return any("\u0600" <= ch <= "ۿ" or "ﭐ" <= ch <= "\ufdff" or "ﹰ" <= ch <= "\ufeff" for ch in str(text))


def _arabic_if_needed(text: str) -> str:
    """تطبيق معالجة العربية فقط إذا كان النص يحتوي حروفاً عربية"""
    val = str(text)
    if _is_arabic(val):
        return arabic(val)
    return val


# إعدادات Plotly المشتركة
LAYOUT_DEFAULTS = {
    "font": {"family": "Tahoma, Arial", "size": 13},
    "plot_bgcolor": WHITE,
    "paper_bgcolor": WHITE,
    "margin": {"l": 40, "r": 40, "t": 60, "b": 40},
}


# ═══════════════════════════════════════════════════════
# الجزء 3: البيانات التجريبية
# ═══════════════════════════════════════════════════════


def get_demo_data() -> pd.DataFrame:
    """إنشاء بيانات تجريبية — 18 سجل تحاكي ورقة RECORD"""
    base_date = datetime(2026, 3, 1)
    records = [
        # النوبة A — رحلات صباحية
        (
            base_date,
            "A",
            "RJ101",
            "B-01",
            45,
            "05:30",
            "07:00",
            "Royal Jordanian",
            "AMM",
            "عمل",
            "النقل الجماعي",
            "أردني",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "A",
            "RJ101",
            "B-02",
            38,
            "05:45",
            "07:00",
            "Royal Jordanian",
            "AMM",
            "عمل",
            "النقل الجماعي",
            "مصري",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "A",
            "SV201",
            "B-03",
            50,
            "06:00",
            "08:00",
            "Saudia",
            "RUH",
            "زيارة",
            "الراشد",
            "سعودي",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "A",
            "SV201",
            "B-04",
            42,
            "06:15",
            "08:00",
            "Saudia",
            "RUH",
            "عمل",
            "الراشد",
            "يمني",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "A",
            "QJ301",
            "B-05",
            35,
            "06:30",
            "09:00",
            "Qatar Airways",
            "DOH",
            "سياحة",
            "النقل الجماعي",
            "أردني",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "A",
            "EK401",
            "B-06",
            48,
            "07:00",
            "10:00",
            "Emirates",
            "DXB",
            "عمل",
            "الأهلية",
            "إماراتي",
            "مغادر",
            "تم",
        ),
        # النوبة B — رحلات ظهرية
        (
            base_date,
            "B",
            "RJ102",
            "B-07",
            40,
            "12:00",
            "14:00",
            "Royal Jordanian",
            "CAI",
            "زيارة",
            "النقل الجماعي",
            "مصري",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "B",
            "RJ102",
            "B-08",
            33,
            "12:15",
            "14:00",
            "Royal Jordanian",
            "CAI",
            "عمل",
            "النقل الجماعي",
            "أردني",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "B",
            "FZ501",
            "B-09",
            44,
            "13:00",
            "15:30",
            "Flynas",
            "JED",
            "عمرة",
            "الراشد",
            "باكستاني",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "B",
            "FZ501",
            "B-10",
            28,
            "13:15",
            "15:30",
            "Flynas",
            "JED",
            "عمرة",
            "الراشد",
            "هندي",
            "متعثر",
            "معلّق",
        ),
        (
            base_date,
            "B",
            "GF601",
            "B-11",
            36,
            "14:00",
            "16:00",
            "Gulf Air",
            "BAH",
            "سياحة",
            "الأهلية",
            "بحريني",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "B",
            "MS701",
            "B-12",
            50,
            "14:30",
            "17:00",
            "EgyptAir",
            "CAI",
            "زيارة",
            "النقل الجماعي",
            "مصري",
            "مغادر",
            "تم",
        ),
        # النوبة C — رحلات مسائية
        (
            base_date,
            "C",
            "RJ103",
            "B-13",
            41,
            "19:00",
            "21:00",
            "Royal Jordanian",
            "BEY",
            "سياحة",
            "النقل الجماعي",
            "لبناني",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "C",
            "SV202",
            "B-14",
            47,
            "19:30",
            "22:00",
            "Saudia",
            "MED",
            "عمرة",
            "الراشد",
            "إندونيسي",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "C",
            "SV202",
            "B-15",
            30,
            "19:45",
            "22:00",
            "Saudia",
            "MED",
            "عمرة",
            "الراشد",
            "ماليزي",
            "متعثر",
            "معلّق",
        ),
        (
            base_date,
            "C",
            "EK402",
            "B-16",
            52,
            "20:00",
            "23:00",
            "Emirates",
            "DXB",
            "عمل",
            "الأهلية",
            "إماراتي",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "C",
            "QJ302",
            "B-17",
            39,
            "20:30",
            "23:30",
            "Qatar Airways",
            "DOH",
            "زيارة",
            "النقل الجماعي",
            "قطري",
            "مغادر",
            "تم",
        ),
        (
            base_date,
            "C",
            "RJ104",
            "B-18",
            25,
            "21:00",
            "23:59",
            "Royal Jordanian",
            "AMM",
            "عمل",
            "النقل الجماعي",
            "عراقي",
            "متعثر",
            "معلّق",
        ),
    ]

    columns = [
        "DATE",
        "SHIFT",
        "FLIGHT",
        "BUS_NUMBER",
        "PAX",
        "CHECKIN_TIME",
        "STD",
        "AIRLINE",
        "DESTINATION",
        "VISA_TYPE",
        "COMPANY",
        "NATIONALITY",
        "STATUS",
        "ACTION",
    ]
    return pd.DataFrame(records, columns=columns)


# ═══════════════════════════════════════════════════════
# الجزء 4: قراءة ملف إكسل
# ═══════════════════════════════════════════════════════


def read_excel_data(file_path: str) -> pd.DataFrame:
    """قراءة ورقة RECORD من ملف إكسل — مع كشف الأعمدة ديناميكياً"""
    path = Path(file_path)
    if not path.exists():
        print(f"خطأ: الملف غير موجود — {file_path}")
        sys.exit(1)

    try:
        df = pd.read_excel(file_path, sheet_name="RECORD", engine="openpyxl")
    except ValueError:
        print("خطأ: ورقة RECORD غير موجودة في الملف")
        sys.exit(1)
    except Exception as e:
        print(f"خطأ في قراءة الملف: {e}")
        sys.exit(1)

    # الأعمدة المطلوبة
    required = {"DATE", "SHIFT", "FLIGHT", "BUS_NUMBER", "PAX", "AIRLINE", "STATUS"}
    found = set(df.columns)
    missing = required - found
    if missing:
        print(f"خطأ: أعمدة ناقصة — {missing}")
        sys.exit(1)

    return df


# ═══════════════════════════════════════════════════════
# الجزء 5: دوال التصفية
# ═══════════════════════════════════════════════════════


def filter_by_status(df: pd.DataFrame, status: str) -> pd.DataFrame:
    """تصفية حسب حالة الراكب"""
    return df[df["STATUS"] == status].copy()


def filter_by_shift(df: pd.DataFrame, shift: str) -> pd.DataFrame:
    """تصفية حسب النوبة"""
    return df[df["SHIFT"] == shift.upper()].copy()


def filter_by_date(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """تصفية حسب التاريخ — صيغة YYYY-MM-DD"""
    target = pd.to_datetime(date_str)
    df_copy = df.copy()
    df_copy["DATE"] = pd.to_datetime(df_copy["DATE"])
    return df_copy[df_copy["DATE"] == target].copy()


def filter_by_airline(df: pd.DataFrame, airline: str) -> pd.DataFrame:
    """تصفية حسب الناقل الجوي"""
    return df[df["AIRLINE"].str.contains(airline, case=False, na=False)].copy()


def filter_by_nationality(df: pd.DataFrame, nationality: str) -> pd.DataFrame:
    """تصفية حسب الجنسية"""
    col = "NATIONALITY"
    if col not in df.columns:
        return df.copy()
    return df[df[col] == nationality].copy()


# ═══════════════════════════════════════════════════════
# الجزء 6: رسوم بيانية — توزيع حسب الناقل
# ═══════════════════════════════════════════════════════


def _save_chart(fig: go.Figure, name: str, output_dir: str) -> str:
    """حفظ رسم بياني كصورة PNG"""
    path = os.path.join(output_dir, f"{name}.png")
    fig.write_image(path, width=700, height=420, scale=2)
    return path


def chart_buses_by_airline(df: pd.DataFrame, output_dir: str) -> str:
    """رسم #1 — عدد الحافلات لكل ناقل جوي"""
    data = df.groupby("AIRLINE")["BUS_NUMBER"].nunique().sort_values(ascending=True)
    fig = go.Figure(
        go.Bar(
            x=data.values,
            y=data.index,
            orientation="h",
            marker_color=BLUE,
            text=data.values,
            textposition="outside",
        ),
    )
    fig.update_layout(
        title="Buses per Airline",
        xaxis_title="Buses",
        yaxis_title="",
        **LAYOUT_DEFAULTS,
    )
    return _save_chart(fig, "buses_by_airline", output_dir)


def chart_pax_by_airline(df: pd.DataFrame, output_dir: str) -> str:
    """رسم #2 — عدد الركاب لكل ناقل جوي"""
    data = df.groupby("AIRLINE")["PAX"].sum().sort_values(ascending=True)
    fig = go.Figure(
        go.Bar(
            x=data.values,
            y=data.index,
            orientation="h",
            marker_color=TEAL,
            text=data.values,
            textposition="outside",
        ),
    )
    fig.update_layout(
        title="Passengers per Airline",
        xaxis_title="Passengers",
        yaxis_title="",
        **LAYOUT_DEFAULTS,
    )
    return _save_chart(fig, "pax_by_airline", output_dir)


# ═══════════════════════════════════════════════════════
# الجزء 7: رسوم بيانية — رحلات وجنسيات
# ═══════════════════════════════════════════════════════


def chart_buses_per_flight(df: pd.DataFrame, output_dir: str) -> str:
    """رسم #3 — عدد الحافلات لكل رحلة — أعمدة أفقية"""
    data = df.groupby("FLIGHT")["BUS_NUMBER"].nunique().sort_values(ascending=True)
    fig = go.Figure(
        go.Bar(
            x=data.values,
            y=data.index,
            orientation="h",
            marker_color=GOLD,
            text=data.values,
            textposition="outside",
        ),
    )
    fig.update_layout(
        title="Buses per Flight",
        xaxis_title="Buses",
        yaxis_title="",
        **LAYOUT_DEFAULTS,
    )
    return _save_chart(fig, "buses_per_flight", output_dir)


def chart_pax_by_nationality(df: pd.DataFrame, output_dir: str) -> str:
    """رسم #4 — توزيع الركاب حسب الجنسية — دائري مجوّف"""
    if "NATIONALITY" not in df.columns:
        # إنشاء رسم فارغ في حال عدم وجود العمود
        fig = go.Figure()
        fig.update_layout(title="Passengers by Nationality (N/A)", **LAYOUT_DEFAULTS)
        return _save_chart(fig, "pax_by_nationality", output_dir)

    data = df.groupby("NATIONALITY")["PAX"].sum().sort_values(ascending=False)
    # Kaleido يستخدم Chromium الذي يدعم العربية أصلاً — لا حاجة لمعالجة النص
    fig = go.Figure(
        go.Pie(
            labels=data.index.tolist(),
            values=data.values,
            hole=0.45,
            marker={"colors": [NAVY, BLUE, TEAL, GOLD, GRAY, "#E74C3C", "#8E44AD", "#27AE60"] * 3},
            textinfo="label+percent",
            textposition="outside",
        ),
    )
    fig.update_layout(
        title="Passengers by Nationality",
        showlegend=False,
        **LAYOUT_DEFAULTS,
    )
    return _save_chart(fig, "pax_by_nationality", output_dir)


# ═══════════════════════════════════════════════════════
# الجزء 8: رسوم بيانية — جدول زمني وتأشيرات ونوبات
# ═══════════════════════════════════════════════════════


def chart_timeline(df: pd.DataFrame, output_dir: str) -> str:
    """رسم #5 — الجدول الزمني — فقاعات scatter"""
    if "CHECKIN_TIME" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="Timeline (N/A)", **LAYOUT_DEFAULTS)
        return _save_chart(fig, "timeline", output_dir)

    fig = go.Figure(
        go.Scatter(
            x=df["CHECKIN_TIME"].astype(str),
            y=df["FLIGHT"],
            mode="markers",
            marker={
                "size": df["PAX"] / df["PAX"].max() * 40 + 10,
                "color": df["PAX"],
                "colorscale": [[0, BLUE], [1, GOLD]],
                "showscale": True,
                "colorbar": {"title": "PAX"},
            },
            text=df.apply(lambda r: f"{r['BUS_NUMBER']}: {r['PAX']} pax", axis=1),
            hoverinfo="text+x+y",
        ),
    )
    fig.update_layout(
        title="Check-in Timeline",
        xaxis_title="Check-in Time",
        yaxis_title="Flight",
        **LAYOUT_DEFAULTS,
    )
    return _save_chart(fig, "timeline", output_dir)


def chart_visa_type(df: pd.DataFrame, output_dir: str) -> str:
    """رسم #6 — توزيع أنواع التأشيرات — دائري مجوّف"""
    if "VISA_TYPE" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="Visa Types (N/A)", **LAYOUT_DEFAULTS)
        return _save_chart(fig, "visa_type", output_dir)

    data = df.groupby("VISA_TYPE")["PAX"].sum()
    # Kaleido يستخدم Chromium الذي يدعم العربية أصلاً — لا حاجة لمعالجة النص
    fig = go.Figure(
        go.Pie(
            labels=data.index.tolist(),
            values=data.values,
            hole=0.45,
            marker={"colors": [NAVY, TEAL, GOLD, BLUE, GRAY]},
            textinfo="label+percent",
            textposition="outside",
        ),
    )
    fig.update_layout(
        title="Visa Type Distribution",
        showlegend=False,
        **LAYOUT_DEFAULTS,
    )
    return _save_chart(fig, "visa_type", output_dir)


def chart_by_shift(df: pd.DataFrame, output_dir: str) -> str:
    """رسم #7 — مقارنة النوبات — أعمدة مجمّعة (حافلات + ركاب)"""
    shifts = sorted(df["SHIFT"].unique())
    buses = [df[df["SHIFT"] == s]["BUS_NUMBER"].nunique() for s in shifts]
    pax = [df[df["SHIFT"] == s]["PAX"].sum() for s in shifts]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Buses",
            x=shifts,
            y=buses,
            marker_color=BLUE,
            text=buses,
            textposition="outside",
        ),
    )
    fig.add_trace(
        go.Bar(
            name="Passengers",
            x=shifts,
            y=pax,
            marker_color=GOLD,
            text=pax,
            textposition="outside",
        ),
    )
    fig.update_layout(
        title="Shift Comparison",
        barmode="group",
        xaxis_title="Shift",
        yaxis_title="Count",
        **LAYOUT_DEFAULTS,
    )
    return _save_chart(fig, "by_shift", output_dir)


# ═══════════════════════════════════════════════════════
# الجزء 9-11: بناء PDF
# ═══════════════════════════════════════════════════════


def _build_styles() -> dict:
    """إعداد أنماط PDF"""
    base = getSampleStyleSheet()
    styles = {}

    # عنوان رئيسي عربي
    styles["title_ar"] = ParagraphStyle(
        "title_ar",
        parent=base["Normal"],
        fontName="ArabicBold",
        fontSize=18,
        alignment=TA_CENTER,
        textColor=colors.HexColor(NAVY),
        spaceAfter=2 * mm,
    )
    # عنوان إنجليزي
    styles["title_en"] = ParagraphStyle(
        "title_en",
        parent=base["Normal"],
        fontName="ArabicBold",
        fontSize=14,
        alignment=TA_CENTER,
        textColor=colors.HexColor(BLUE),
        spaceAfter=4 * mm,
    )
    # نص عادي عربي
    styles["body_ar"] = ParagraphStyle(
        "body_ar",
        parent=base["Normal"],
        fontName="Arabic",
        fontSize=10,
        alignment=TA_RIGHT,
        textColor=colors.HexColor(NAVY),
    )
    # نص مركزي
    styles["center"] = ParagraphStyle(
        "center",
        parent=base["Normal"],
        fontName="Arabic",
        fontSize=10,
        alignment=TA_CENTER,
        textColor=colors.HexColor(GRAY),
    )
    # بطاقة — رقم كبير
    styles["card_number"] = ParagraphStyle(
        "card_number",
        parent=base["Normal"],
        fontName="ArabicBold",
        fontSize=22,
        alignment=TA_CENTER,
        textColor=colors.HexColor(NAVY),
    )
    # بطاقة — تسمية — حجم مقروء ولون واضح
    styles["card_label"] = ParagraphStyle(
        "card_label",
        parent=base["Normal"],
        fontName="ArabicBold",
        fontSize=12,
        alignment=TA_CENTER,
        textColor=colors.HexColor(BLUE),
    )
    return styles


def _build_header(styles: dict, df: pd.DataFrame, filter_desc: str) -> list:
    """بناء رأس التقرير"""
    elements = []
    elements.append(Paragraph(arabic("تقرير النقل البري — الحج والعمرة"), styles["title_ar"]))
    elements.append(Paragraph("JEDCO Ground Transport Report", styles["title_en"]))

    # التاريخ والفلتر
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    info = f"Generated: {date_str}"
    if filter_desc:
        info += f"  |  Filter: {filter_desc}"
    elements.append(Paragraph(info, styles["center"]))
    elements.append(Spacer(1, 6 * mm))
    return elements


def _build_summary_cards(styles: dict, df: pd.DataFrame) -> list:
    """بناء 4 بطاقات ملخص"""
    total_buses = df["BUS_NUMBER"].nunique()
    total_pax = int(df["PAX"].sum())
    total_flights = df["FLIGHT"].nunique()
    avg_pax = round(total_pax / max(total_buses, 1), 1)

    cards_data = [
        (str(total_buses), arabic("حافلات")),
        (str(total_pax), arabic("ركاب")),
        (str(total_flights), arabic("رحلات")),
        (str(avg_pax), arabic("متوسط الركاب")),
    ]

    # صفان منفصلان — الأرقام في صف والتسميات في صف — لمنع التداخل
    number_row = [Paragraph(v, styles["card_number"]) for v, _ in cards_data]
    label_row = [Paragraph(lbl, styles["card_label"]) for _, lbl in cards_data]

    card_table = Table(
        [number_row, label_row],
        colWidths=[6 * cm] * 4,
        rowHeights=[1.8 * cm, 1.2 * cm],
    )
    card_table.setStyle(
        TableStyle(
            [
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(GRAY)),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor(GRAY)),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(LIGHT_BG)),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ],
        ),
    )

    return [card_table, Spacer(1, 8 * mm)]


def _build_charts_section(chart_paths: list[str]) -> list:
    """إدراج الرسوم البيانية في PDF — شبكة 2×2 ثم الباقي"""
    elements = []

    # تقسيم الصور لأزواج
    pairs = []
    for i in range(0, len(chart_paths), 2):
        pair = chart_paths[i : i + 2]
        pairs.append(pair)

    img_width = 12 * cm
    img_height = 7.2 * cm

    for pair in pairs:
        row = []
        for p in pair:
            if os.path.exists(p):
                row.append(Image(p, width=img_width, height=img_height))
            else:
                row.append(Paragraph("Chart not available", ParagraphStyle("x")))

        if len(row) == 1:
            # رسم بعرض كامل
            full_img = Image(pair[0], width=20 * cm, height=12 * cm)
            elements.append(full_img)
        else:
            t = Table([row], colWidths=[13 * cm, 13 * cm])
            t.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ],
                ),
            )
            elements.append(t)
        elements.append(Spacer(1, 4 * mm))

    return elements


def _build_shift_bar(styles: dict, df: pd.DataFrame) -> list:
    """شريط النوبات الملوّن"""
    elements = []
    shifts = sorted(df["SHIFT"].unique())
    row_data = []
    for s in shifts:
        count = df[df["SHIFT"] == s]["BUS_NUMBER"].nunique()
        pax = int(df[df["SHIFT"] == s]["PAX"].sum())
        cell_text = f"Shift {s}: {count} buses / {pax} pax"
        row_data.append(Paragraph(cell_text, styles["center"]))

    if row_data:
        shift_table = Table(
            [row_data],
            colWidths=[8 * cm] * len(row_data),
        )
        style_cmds = [
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]
        # تلوين كل خلية حسب النوبة
        for i, s in enumerate(shifts):
            bg = SHIFT_COLORS.get(s, GRAY)
            style_cmds.append(("BACKGROUND", (i, 0), (i, 0), colors.HexColor(bg)))
            style_cmds.append(("TEXTCOLOR", (i, 0), (i, 0), colors.white))

        shift_table.setStyle(TableStyle(style_cmds))
        elements.append(shift_table)
        elements.append(Spacer(1, 6 * mm))

    return elements


def _build_detail_table(styles: dict, df: pd.DataFrame) -> list:
    """جدول البيانات التفصيلي"""
    elements = []

    # اختيار الأعمدة المتوفرة للعرض
    display_cols = ["SHIFT", "FLIGHT", "BUS_NUMBER", "PAX", "AIRLINE", "STATUS"]
    optional_cols = ["CHECKIN_TIME", "STD", "DESTINATION", "VISA_TYPE", "NATIONALITY"]
    for c in optional_cols:
        if c in df.columns:
            display_cols.append(c)

    # رأس الجدول
    header = display_cols[:]
    rows = [header]
    for _, row in df.iterrows():
        # تمرير كل خلية عربية عبر معالج العربية لتظهر بشكل صحيح في PDF
        r = [_arabic_if_needed(row.get(c, "")) for c in display_cols]
        rows.append(r)

    # حساب عرض الأعمدة — توزيع متساوي
    page_width = landscape(A4)[0] - 3 * cm
    col_width = page_width / len(display_cols)
    col_widths = [col_width] * len(display_cols)

    detail_table = Table(rows, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        # رأس كحلي
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(NAVY)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "ArabicBold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTNAME", (0, 1), (-1, -1), "Arabic"),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor(GRAY)),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(NAVY)),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]
    # صفوف متبادلة الألوان
    for i in range(1, len(rows)):
        if i % 2 == 0:
            style_cmds.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor(LIGHT_BG)))

    detail_table.setStyle(TableStyle(style_cmds))
    elements.append(Paragraph(arabic("البيانات التفصيلية"), styles["title_ar"]))
    elements.append(Spacer(1, 3 * mm))
    elements.append(detail_table)
    elements.append(Spacer(1, 6 * mm))
    return elements


def _build_summary_tables(styles: dict, df: pd.DataFrame) -> list:
    """جدول ملخص الرحلات + ملخص الناقلين"""
    elements = []

    # ملخص الرحلات
    flight_summary = (
        df.groupby("FLIGHT")
        .agg(
            BUSES=("BUS_NUMBER", "nunique"),
            PAX=("PAX", "sum"),
            AIRLINE=("AIRLINE", "first"),
        )
        .reset_index()
        .sort_values("PAX", ascending=False)
    )
    f_header = ["FLIGHT", "AIRLINE", "BUSES", "PAX"]
    f_rows = [f_header]
    for _, row in flight_summary.iterrows():
        f_rows.append(
            [
                str(row["FLIGHT"]),
                str(row["AIRLINE"]),
                str(row["BUSES"]),
                str(int(row["PAX"])),
            ],
        )

    f_table = Table(f_rows, colWidths=[4 * cm, 5 * cm, 3 * cm, 3 * cm], repeatRows=1)
    f_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(NAVY)),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "ArabicBold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("FONTNAME", (0, 1), (-1, -1), "Arabic"),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor(GRAY)),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(NAVY)),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ],
        ),
    )
    elements.append(Paragraph(arabic("ملخص الرحلات"), styles["title_ar"]))
    elements.append(Spacer(1, 2 * mm))
    elements.append(f_table)
    elements.append(Spacer(1, 8 * mm))

    # ملخص الناقلين
    airline_summary = (
        df.groupby("AIRLINE")
        .agg(
            FLIGHTS=("FLIGHT", "nunique"),
            BUSES=("BUS_NUMBER", "nunique"),
            PAX=("PAX", "sum"),
        )
        .reset_index()
        .sort_values("PAX", ascending=False)
    )
    a_header = ["AIRLINE", "FLIGHTS", "BUSES", "PAX"]
    a_rows = [a_header]
    for _, row in airline_summary.iterrows():
        a_rows.append(
            [
                str(row["AIRLINE"]),
                str(row["FLIGHTS"]),
                str(row["BUSES"]),
                str(int(row["PAX"])),
            ],
        )

    a_table = Table(a_rows, colWidths=[5 * cm, 3 * cm, 3 * cm, 3 * cm], repeatRows=1)
    a_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(TEAL)),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "ArabicBold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("FONTNAME", (0, 1), (-1, -1), "Arabic"),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor(GRAY)),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(TEAL)),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ],
        ),
    )
    elements.append(Paragraph(arabic("ملخص الناقلين"), styles["title_ar"]))
    elements.append(Spacer(1, 2 * mm))
    elements.append(a_table)
    elements.append(Spacer(1, 6 * mm))

    return elements


def _build_footer(styles: dict) -> list:
    """تذييل التقرير"""
    footer_text = f"JEDCO Ground Transport System | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    return [
        Spacer(1, 10 * mm),
        Paragraph(footer_text, styles["center"]),
    ]


def generate_pdf(
    df: pd.DataFrame,
    output_path: str,
    filter_desc: str = "",
) -> str:
    """توليد تقرير PDF كامل"""
    # مجلد مؤقت للرسوم
    chart_dir = tempfile.mkdtemp(prefix="jedco_charts_")

    print("جارٍ توليد الرسوم البيانية...")
    # توليد 7 رسوم
    chart_paths = [
        chart_buses_by_airline(df, chart_dir),
        chart_pax_by_airline(df, chart_dir),
        chart_buses_per_flight(df, chart_dir),
        chart_pax_by_nationality(df, chart_dir),
        chart_timeline(df, chart_dir),
        chart_visa_type(df, chart_dir),
        chart_by_shift(df, chart_dir),
    ]

    print("جارٍ بناء PDF...")
    doc = SimpleDocTemplate(
        output_path,
        pagesize=landscape(A4),
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
    )

    styles = _build_styles()
    elements = []

    # الرأس
    elements.extend(_build_header(styles, df, filter_desc))
    # بطاقات الملخص
    elements.extend(_build_summary_cards(styles, df))
    # شريط النوبات
    elements.extend(_build_shift_bar(styles, df))
    # الرسوم البيانية
    elements.extend(_build_charts_section(chart_paths))
    # الجدول التفصيلي
    elements.extend(_build_detail_table(styles, df))
    # جداول الملخص
    elements.extend(_build_summary_tables(styles, df))
    # التذييل
    elements.extend(_build_footer(styles))

    doc.build(elements)

    # تنظيف الصور المؤقتة
    for p in chart_paths:
        if os.path.exists(p):
            os.remove(p)
    with contextlib.suppress(OSError):
        os.rmdir(chart_dir)

    return output_path


# ═══════════════════════════════════════════════════════
# الجزء 12: القائمة التفاعلية + argparse
# ═══════════════════════════════════════════════════════


def _get_output_path() -> str:
    """تحديد مسار ملف PDF الناتج"""
    desktop = Path.home() / "Desktop"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return str(desktop / f"JEDCO_Report_{timestamp}.pdf")


def interactive_menu(df: pd.DataFrame) -> None:
    """القائمة التفاعلية — 7 خيارات + خروج"""
    while True:
        print("\n" + "=" * 50)
        print(arabic("نظام تقارير JEDCO"))
        print("=" * 50)
        print("1.", arabic("تقرير كامل"))
        print("2.", arabic("تصفية حسب النوبة"))
        print("3.", arabic("تصفية حسب الحالة"))
        print("4.", arabic("تصفية حسب الناقل"))
        print("5.", arabic("تصفية حسب الجنسية"))
        print("6.", arabic("تصفية حسب التاريخ"))
        print("7.", arabic("رسوم بيانية فقط"))
        print("0.", arabic("خروج"))
        print("=" * 50)

        choice = input(arabic("اختر: ") + " ").strip()

        filtered = df.copy()
        filter_desc = ""

        if choice == "0":
            print(arabic("وداعاً"))
            break
        if choice == "1":
            filter_desc = "Full Report"
        elif choice == "2":
            shift = input("Shift (A/B/C): ").strip().upper()
            filtered = filter_by_shift(df, shift)
            filter_desc = f"Shift {shift}"
        elif choice == "3":
            status = input(arabic("الحالة: ") + " ").strip()
            filtered = filter_by_status(df, status)
            filter_desc = f"Status: {status}"
        elif choice == "4":
            airline = input("Airline: ").strip()
            filtered = filter_by_airline(df, airline)
            filter_desc = f"Airline: {airline}"
        elif choice == "5":
            nat = input(arabic("الجنسية: ") + " ").strip()
            filtered = filter_by_nationality(df, nat)
            filter_desc = f"Nationality: {nat}"
        elif choice == "6":
            date_str = input("Date (YYYY-MM-DD): ").strip()
            filtered = filter_by_date(df, date_str)
            filter_desc = f"Date: {date_str}"
        elif choice == "7":
            # رسوم فقط بدون PDF
            chart_dir = tempfile.mkdtemp(prefix="jedco_charts_")
            print(arabic("جارٍ توليد الرسوم..."))
            paths = [
                chart_buses_by_airline(df, chart_dir),
                chart_pax_by_airline(df, chart_dir),
                chart_buses_per_flight(df, chart_dir),
                chart_pax_by_nationality(df, chart_dir),
                chart_timeline(df, chart_dir),
                chart_visa_type(df, chart_dir),
                chart_by_shift(df, chart_dir),
            ]
            print(arabic("تم حفظ الرسوم في:"))
            print(chart_dir)
            for p in paths:
                print(f"  {os.path.basename(p)}")
            continue
        else:
            print(arabic("خيار غير صحيح"))
            continue

        if filtered.empty:
            print(arabic("لا توجد بيانات بعد التصفية"))
            continue

        output = _get_output_path()
        result = generate_pdf(filtered, output, filter_desc)
        print(f"\n{arabic('تم إنشاء التقرير:')}")
        print(result)

        # فتح الملف تلقائياً
        if sys.platform == "win32":
            os.startfile(result)
        elif sys.platform == "darwin":
            os.system(f'open "{result}"')
        else:
            os.system(f'xdg-open "{result}"')


def main() -> None:
    """نقطة الدخول الرئيسية"""
    parser = argparse.ArgumentParser(description="JEDCO Ground Transport Report Generator")
    parser.add_argument("--demo", action="store_true", help=arabic("استخدام بيانات تجريبية"))
    parser.add_argument("--file", type=str, help="Excel file path")
    parser.add_argument("--status", type=str, help="Filter by status")
    parser.add_argument("--shift", type=str, help="Filter by shift (A/B/C)")
    parser.add_argument("--date", type=str, help="Filter by date (YYYY-MM-DD)")
    parser.add_argument("--airline", type=str, help="Filter by airline")
    parser.add_argument("--nationality", type=str, help="Filter by nationality")
    parser.add_argument("--all", action="store_true", help="Generate without interactive menu")

    args = parser.parse_args()

    # تحديد مصدر البيانات
    if args.demo:
        print(arabic("وضع البيانات التجريبية"))
        df = get_demo_data()
    elif args.file:
        df = read_excel_data(args.file)
    else:
        # المسار الافتراضي
        default_path = r"C:\Users\ps5mk\Desktop\HT_SC_\JEDCO_HT.xlsx"
        if os.path.exists(default_path):
            df = read_excel_data(default_path)
        else:
            print(arabic("ملف البيانات غير موجود — استخدم --demo للبيانات التجريبية"))
            print(f"  --demo   {arabic('بيانات تجريبية')}")
            print(f"  --file   {arabic('مسار ملف إكسل')}")
            sys.exit(1)

    # تطبيق الفلاتر من سطر الأوامر
    filter_desc_parts = []
    if args.status:
        df = filter_by_status(df, args.status)
        filter_desc_parts.append(f"Status: {args.status}")
    if args.shift:
        df = filter_by_shift(df, args.shift)
        filter_desc_parts.append(f"Shift: {args.shift}")
    if args.date:
        df = filter_by_date(df, args.date)
        filter_desc_parts.append(f"Date: {args.date}")
    if args.airline:
        df = filter_by_airline(df, args.airline)
        filter_desc_parts.append(f"Airline: {args.airline}")
    if args.nationality:
        df = filter_by_nationality(df, args.nationality)
        filter_desc_parts.append(f"Nationality: {args.nationality}")

    filter_desc = " | ".join(filter_desc_parts)

    if df.empty:
        print(arabic("لا توجد بيانات بعد التصفية"))
        sys.exit(1)

    # وضع التوليد المباشر أو القائمة التفاعلية
    if args.all or any([args.status, args.shift, args.date, args.airline, args.nationality]):
        output = _get_output_path()
        result = generate_pdf(df, output, filter_desc)
        print(f"\n{arabic('تم إنشاء التقرير:')}")
        print(result)
        if sys.platform == "win32":
            os.startfile(result)
    elif args.demo and not any([args.status, args.shift, args.date]):
        # وضع demo بدون فلاتر — توليد مباشر
        output = _get_output_path()
        result = generate_pdf(df, output, "Demo Data")
        print(f"\n{arabic('تم إنشاء التقرير:')}")
        print(result)
        if sys.platform == "win32":
            os.startfile(result)
    else:
        interactive_menu(df)


if __name__ == "__main__":
    main()
