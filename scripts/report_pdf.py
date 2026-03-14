"""بناء مستند التقرير — تصميم موحد للمشروع.

يُستخدم من report_builder_new وأي سكربت آخر يحتاج توليد تقرير.
"""

__all__ = ["build_pdf"]

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from common import print_err, print_warn


def build_pdf(
    df: pd.DataFrame,
    chart_paths: list[str],
    selections: list[tuple[str, str]],
    file_name: str,
    summaries: list[tuple[str, str]] | None = None,
    chosen_columns: list[str] | None = None,
    header_info: dict | None = None,
) -> str | None:
    """بناء ملف PDF يحتوي الرسوم والبيانات."""
    try:
        import arabic_reshaper
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
    except ImportError as e:
        print_err(f"مكتبة ناقصة: {e}")
        print_warn("ثبّت المكتبات المطلوبة أولاً")
        return None

    # تسجيل الخطوط
    font_path = r"C:\Windows\Fonts\tahoma.ttf"
    font_bold = r"C:\Windows\Fonts\tahomabd.ttf"
    amiri_reg = r"C:\Windows\Fonts\Amiri-Regular.ttf"
    amiri_bold = r"C:\Windows\Fonts\Amiri-Bold.ttf"
    if os.path.exists(font_path):
        try:
            pdfmetrics.getFont("Arabic")
        except KeyError:
            pdfmetrics.registerFont(TTFont("Arabic", font_path))
            pdfmetrics.registerFont(TTFont("ArabicBold", font_bold))
    if os.path.exists(amiri_reg):
        try:
            pdfmetrics.getFont("Amiri")
        except KeyError:
            pdfmetrics.registerFont(TTFont("Amiri", amiri_reg))
            pdfmetrics.registerFont(TTFont("AmiriBold", amiri_bold))

    def ar(text):
        """معالجة العربية للعرض الصحيح في PDF."""
        if not text:
            return ""
        reshaped = arabic_reshaper.reshape(str(text))
        return get_display(reshaped)

    def is_arabic(text) -> bool:
        """كشف النص العربي."""
        return any("\u0600" <= ch <= "ۿ" or "ﭐ" <= ch <= "\ufdff" for ch in str(text))

    def ar_if(text):
        """معالجة مشروطة — فقط إذا يحتوي عربي."""
        val = str(text)
        return ar(val) if is_arabic(val) else val

    def ar_para(text, max_chars=45):
        """معالجة عربية للنصوص الطويلة داخل خلايا الجدول."""
        val = str(text).strip()
        if not val or not is_arabic(val):
            return val
        if len(val) <= max_chars:
            return ar(val)
        words = val.split()
        lines = []
        current = ""
        for w in words:
            if current and len(current) + 1 + len(w) > max_chars:
                lines.append(current)
                current = w
            else:
                current = f"{current} {w}" if current else w
        if current:
            lines.append(current)
        return "<br/>".join(ar(line) for line in lines)

    # ألوان
    NAVY = "#1B2A4A"
    TEAL = "#3AAFA9"
    GRAY = "#6C757D"
    LIGHT_BG = "#F8F9FA"

    # أنماط
    base = getSampleStyleSheet()
    info_style = ParagraphStyle(
        "info",
        parent=base["Normal"],
        fontName="Arabic",
        fontSize=10,
        alignment=TA_CENTER,
        textColor=colors.HexColor(GRAY),
    )
    section_style = ParagraphStyle(
        "section",
        parent=base["Normal"],
        fontName="ArabicBold",
        fontSize=14,
        alignment=TA_CENTER,
        textColor=colors.HexColor(NAVY),
        spaceAfter=3 * mm,
    )

    # مسار الناتج
    desktop = Path.home() / "Desktop"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = str(desktop / f"Report_{timestamp}.pdf")

    _hdr = header_info or {}
    hdr_h = 55 + 5
    top_margin = hdr_h + 12

    doc = SimpleDocTemplate(
        output_path,
        pagesize=landscape(A4),
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=top_margin,
        bottomMargin=1.5 * cm,
    )

    page_w, page_h = landscape(A4)

    def _draw_header(canvas_obj, doc_obj) -> None:
        """رسم الهيدر الاحترافي مباشرة على canvas."""
        canvas_obj.saveState()
        w = page_w
        h = page_h
        bar_h = 55

        canvas_obj.setFillColor(colors.HexColor(NAVY))
        canvas_obj.rect(0, h - bar_h, w, bar_h, fill=1, stroke=0)

        canvas_obj.setStrokeColor(colors.HexColor("#D4A843"))
        canvas_obj.setLineWidth(2)
        canvas_obj.line(0, h - bar_h, w, h - bar_h)

        canvas_obj.setStrokeColor(colors.HexColor(TEAL))
        canvas_obj.setLineWidth(0.8)
        canvas_obj.line(0, h - bar_h - 3, w, h - bar_h - 3)

        x_right = w - 30
        x_left = 30

        y1 = h - 20
        ar_title_font = "AmiriBold" if _font_exists("AmiriBold") else "ArabicBold"
        canvas_obj.setFont(ar_title_font, 14)
        canvas_obj.setFillColor(colors.white)
        canvas_obj.drawRightString(x_right, y1, ar("تقرير تحليل البيانات"))
        canvas_obj.setFont("Helvetica-Bold", 11)
        canvas_obj.setFillColor(colors.HexColor("#D4A843"))
        canvas_obj.drawString(x_left, y1, "Data Analysis Report")

        y2 = h - 36
        ar_sub_font = "Amiri" if _font_exists("Amiri") else "Arabic"
        canvas_obj.setFont(ar_sub_font, 8)
        canvas_obj.setFillColor(colors.HexColor("#B0C4DE"))
        canvas_obj.drawRightString(
            x_right,
            y2,
            ar("عمليات الساحات الخارجية والمواصلات - مجمع صالات الحج والعمرة"),
        )
        canvas_obj.setFont("Helvetica-Bold", 10)
        canvas_obj.setFillColor(colors.HexColor("#D4A843"))
        date_display = _hdr.get("date") or datetime.now().strftime("%d/%m/%Y")
        canvas_obj.drawString(x_left, y2, date_display)

        y3 = h - bar_h + 4
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.setFillColor(colors.HexColor("#8899AA"))
        canvas_obj.drawString(x_left, y3, f"Page {doc_obj.page}")

        canvas_obj.restoreState()

    def _font_exists(name) -> bool | None:
        """فحص وجود خط مسجل."""
        try:
            pdfmetrics.getFont(name)
            return True
        except KeyError:
            return False

    elements = []
    elements.append(Spacer(1, 4 * mm))

    # === الرسوم البيانية ===
    if chart_paths:
        elements.append(Paragraph(ar("الرسوم البيانية"), section_style))
        elements.append(Spacer(1, 3 * mm))

        img_w = 12 * cm
        img_h = 7.2 * cm

        pairs = []
        for i in range(0, len(chart_paths), 2):
            pairs.append(chart_paths[i : i + 2])

        for pair in pairs:
            row = []
            for p in pair:
                if os.path.exists(p):
                    row.append(Image(p, width=img_w, height=img_h))
            if len(row) == 1:
                elements.append(Image(pair[0], width=20 * cm, height=12 * cm))
            elif len(row) == 2:
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

    # === جدول البيانات ===
    max_rows = 30
    _chosen = chosen_columns or list(df.columns)
    display_cols = [c for c in _chosen if c in df.columns]
    display_df = df[display_cols].head(max_rows)

    elements.append(Paragraph(ar("تحليل البيانات التفصيلي"), section_style))
    if len(df) > max_rows:
        elements.append(
            Paragraph(
                f"Showing {max_rows} of {len(df)} records",
                info_style,
            ),
        )
    elements.append(Spacer(1, 3 * mm))

    cell_hdr_style = ParagraphStyle(
        "cell_hdr",
        parent=base["Normal"],
        fontName="ArabicBold",
        fontSize=7,
        alignment=TA_CENTER,
        textColor=colors.white,
        leading=9,
        wordWrap="CJK",
    )
    cell_data_style = ParagraphStyle(
        "cell_data",
        parent=base["Normal"],
        fontName="Arabic",
        fontSize=6.5,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#1B2A4A"),
        leading=8,
        wordWrap="CJK",
    )

    header = [Paragraph(ar_if(c), cell_hdr_style) for c in display_cols]
    rows = [header]
    for _, row in display_df.iterrows():
        r = [Paragraph(ar_para(row.get(c, "")), cell_data_style) for c in display_cols]
        rows.append(r)

    page_w_table = landscape(A4)[0] - 3 * cm

    col_max_len = []
    for c in display_cols:
        hdr_len = len(str(c))
        data_vals = display_df[c].dropna().astype(str).head(50)
        max_val_len = data_vals.str.len().max() if not data_vals.empty else 0
        effective = min(max(hdr_len, max_val_len, 3), 40)
        col_max_len.append(effective)

    total_len = sum(col_max_len)
    col_widths = [(l / total_len) * page_w_table for l in col_max_len]

    data_table = Table(rows, colWidths=col_widths, repeatRows=1)
    data_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(NAVY)),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor(GRAY)),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(NAVY)),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ("LEFTPADDING", (0, 0), (-1, -1), 2),
                ("RIGHTPADDING", (0, 0), (-1, -1), 2),
            ],
        ),
    )
    for i in range(2, len(rows), 2):
        data_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, i), (-1, i), colors.HexColor(LIGHT_BG)),
                ],
            ),
        )
    elements.append(data_table)
    elements.append(Spacer(1, 6 * mm))

    # === الملخصات التلقائية ===
    _summaries = summaries or []
    if _summaries:
        elements.append(Paragraph(ar("الملخصات التحليلية"), section_style))
        elements.append(Spacer(1, 3 * mm))

    s_hdr_style = ParagraphStyle(
        "s_hdr",
        parent=base["Normal"],
        fontName="ArabicBold",
        fontSize=7,
        alignment=TA_CENTER,
        textColor=colors.white,
        leading=9,
        wordWrap="CJK",
    )
    s_cell_style = ParagraphStyle(
        "s_cell",
        parent=base["Normal"],
        fontName="Arabic",
        fontSize=6.5,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#1B2A4A"),
        leading=8,
        wordWrap="CJK",
    )
    s_total_style = ParagraphStyle(
        "s_total",
        parent=base["Normal"],
        fontName="ArabicBold",
        fontSize=7,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#1B2A4A"),
        leading=9,
        wordWrap="CJK",
    )

    for summary in _summaries:
        grp_col = summary["group_col"]
        tbl_df = summary["table_df"]
        if tbl_df.empty:
            continue

        elements.append(Paragraph(ar(f"حسب {grp_col}"), section_style))
        elements.append(Spacer(1, 2 * mm))

        tbl_cols = list(tbl_df.columns)
        s_header = [Paragraph(ar_if(c), s_hdr_style) for c in tbl_cols]
        s_rows = [s_header]

        data_rows = tbl_df.iloc[:-1]
        total_row = tbl_df.iloc[-1]

        for _, row in data_rows.iterrows():
            r = []
            for c in tbl_cols:
                val = row[c]
                if isinstance(val, (int, float)) and c != grp_col:
                    r.append(
                        Paragraph(
                            f"{int(val):,}" if val == int(val) else f"{val:,.1f}",
                            s_cell_style,
                        ),
                    )
                else:
                    r.append(Paragraph(ar_if(val), s_cell_style))
            s_rows.append(r)

        total_r = []
        for c in tbl_cols:
            val = total_row[c]
            if isinstance(val, (int, float)) and c != grp_col:
                total_r.append(
                    Paragraph(
                        f"{int(val):,}" if val == int(val) else f"{val:,.1f}",
                        s_total_style,
                    ),
                )
            else:
                total_r.append(Paragraph(ar_if(val), s_total_style))
        s_rows.append(total_r)

        s_page_w = landscape(A4)[0] - 3 * cm
        n = len(tbl_cols)
        first_w = s_page_w * 0.35
        rest_w = (s_page_w - first_w) / max(n - 1, 1)
        s_col_widths = [first_w] + [rest_w] * (n - 1)

        s_table = Table(s_rows, colWidths=s_col_widths, repeatRows=1)
        s_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(TEAL)),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor(GRAY)),
                    ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor(TEAL)),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                    ("LEFTPADDING", (0, 0), (-1, -1), 2),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 2),
                    ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#E8F4F8")),
                    ("LINEABOVE", (0, -1), (-1, -1), 1, colors.HexColor(NAVY)),
                ],
            ),
        )
        for i in range(2, len(s_rows) - 1, 2):
            s_table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, i), (-1, i), colors.HexColor(LIGHT_BG)),
                    ],
                ),
            )
        elements.append(s_table)
        elements.append(Spacer(1, 6 * mm))

    # === التذييل ===
    footer = f"Report Builder | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    elements.append(Spacer(1, 10 * mm))
    elements.append(Paragraph(footer, info_style))

    # بناء PDF
    try:
        doc.build(elements, onFirstPage=_draw_header, onLaterPages=_draw_header)
    except Exception as e:
        print_err(f"خطأ في بناء PDF: {e}")
        return None

    return output_path
