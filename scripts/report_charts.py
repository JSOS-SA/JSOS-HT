"""دوال الرسوم البيانية والإحصائيات — وحدة مستقلة قابلة لإعادة الاستخدام.

تُستخدم من report_builder_new وأي سكربت آخر يحتاج رسوماً أو ملخصات.
"""

__all__ = [
    "CHART_COLORS",
    "auto_summaries",
    "detect_col_type",
    "generate_chart",
    "inject_dispatch_columns",
    "normalize_code_columns",
]

import os
from datetime import time as dt_time

import pandas as pd
import plotly.graph_objects as go

from common import print_err

# === ألوان الرسوم البيانية — لوحة موحدة للمشروع ===
CHART_COLORS = [
    "#1B2A4A",
    "#2E5090",
    "#3AAFA9",
    "#D4A843",
    "#6C757D",
    "#E74C3C",
    "#8E44AD",
    "#27AE60",
    "#F39C12",
    "#2C3E50",
    "#1ABC9C",
    "#E67E22",
    "#9B59B6",
    "#34495E",
    "#16A085",
]


def detect_col_type(series: pd.Series | pd.DataFrame) -> str:
    """كشف نوع العمود الفعلي — يفحص القيم بدل الاعتماد على dtype.

    لأن pandas يقرأ الأعمدة ذات القيم الفارغة كـ float64 حتى لو كانت نصية.
    إذا وصل DataFrame (أعمدة مكررة) يأخذ أول عمود فقط.
    """
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    non_null = series.dropna()
    if non_null.empty:
        return "فارغ"
    if series.dtype == object:
        return "نصي"
    try:
        if all(float(v) == int(float(v)) for v in non_null):
            return "رقمي"
    except (ValueError, TypeError, OverflowError):
        pass
    return "رقمي"


def auto_summaries(
    df: pd.DataFrame,
    columns: list[str],
) -> list[dict]:
    """توليد ملخصات تلقائية — لكل عمود نصي: تجميع + عدد + مجاميع رقمية.

    يعيد قائمة قواميس كل واحد يحتوي: group_col, table_df
    """
    text_cols = []
    num_cols = []
    for c in columns:
        if c not in df.columns:
            continue
        col_data = df[c].dropna()
        if col_data.empty:
            continue
        if pd.api.types.is_numeric_dtype(col_data):
            num_cols.append(c)
        else:
            try:
                pd.to_numeric(col_data, errors="raise")
                num_cols.append(c)
            except (ValueError, TypeError):
                text_cols.append(c)

    if not text_cols:
        return []

    results = []
    for grp_col in text_cols:
        tmp = df.copy()
        sample = tmp[grp_col].dropna().head(1)
        if not sample.empty and isinstance(sample.iloc[0], str):
            tmp[grp_col] = tmp[grp_col].str.upper().str.strip()
        vc = tmp[grp_col].dropna().value_counts()
        if vc.empty or vc.max() <= 1:
            continue
        grouped = (
            tmp.groupby(grp_col, dropna=True)
            .agg(
                **{
                    "عدد السجلات": pd.NamedAgg(column=grp_col, aggfunc="count"),
                    **{nc: pd.NamedAgg(column=nc, aggfunc="sum") for nc in num_cols},
                },
            )
            .reset_index()
        )
        grouped = grouped.sort_values("عدد السجلات", ascending=False)

        total_row = {grp_col: "الإجمالي"}
        total_row["عدد السجلات"] = grouped["عدد السجلات"].sum()
        for nc in num_cols:
            total_row[nc] = grouped[nc].sum()
        total_df = pd.DataFrame([total_row])
        grouped = pd.concat([grouped, total_df], ignore_index=True)

        results.append(
            {
                "group_col": grp_col,
                "table_df": grouped,
            },
        )

    return results


def generate_chart(
    df: pd.DataFrame,
    col: str,
    chart_type: str,
    output_dir: str,
    index: int,
) -> str | None:
    """توليد رسم بياني واحد وحفظه كصورة."""
    data = df[col].value_counts().sort_values(ascending=False)
    if data.empty:
        return None

    labels = data.index.tolist()
    values = data.values.tolist()

    fig = go.Figure()

    if chart_type == "bar_v":
        fig.add_trace(
            go.Bar(
                x=labels,
                y=values,
                marker_color=CHART_COLORS[: len(labels)],
                text=values,
                textposition="outside",
            ),
        )
        fig.update_layout(title=f"{col}", xaxis_title="", yaxis_title="العدد")

    elif chart_type == "bar_h":
        fig.add_trace(
            go.Bar(
                x=values[::-1],
                y=labels[::-1],
                orientation="h",
                marker_color=CHART_COLORS[: len(labels)],
                text=values[::-1],
                textposition="outside",
            ),
        )
        fig.update_layout(title=f"{col}", xaxis_title="العدد", yaxis_title="")

    elif chart_type == "bar_stack":
        for idx, (lbl, val) in enumerate(zip(labels, values, strict=False)):
            fig.add_trace(
                go.Bar(
                    x=[col],
                    y=[val],
                    name=str(lbl),
                    marker_color=CHART_COLORS[idx % len(CHART_COLORS)],
                    text=[val],
                    textposition="inside",
                ),
            )
        fig.update_layout(
            title=f"{col}",
            barmode="stack",
            yaxis_title="العدد",
            showlegend=True,
        )

    elif chart_type == "pie":
        fig = go.Figure(
            go.Pie(
                labels=labels,
                values=values,
                marker={"colors": CHART_COLORS[: len(labels)]},
                textinfo="label+value",
                textposition="outside",
            ),
        )
        fig.update_layout(title=f"{col}", showlegend=False)

    elif chart_type == "donut":
        total = sum(values)
        fig = go.Figure(
            go.Pie(
                labels=labels,
                values=values,
                hole=0.45,
                marker={"colors": CHART_COLORS[: len(labels)]},
                textinfo="label+percent",
                textposition="outside",
            ),
        )
        fig.update_layout(
            title=f"{col}",
            showlegend=False,
            annotations=[{"text": str(total), "x": 0.5, "y": 0.5, "font_size": 20, "showarrow": False}],
        )

    elif chart_type == "percent":
        fig = go.Figure(
            go.Pie(
                labels=labels,
                values=values,
                hole=0.5,
                marker={"colors": CHART_COLORS[: len(labels)]},
                textinfo="percent+label",
                textposition="inside",
                insidetextorientation="radial",
            ),
        )
        fig.update_layout(title=f"{col}", showlegend=True)

    elif chart_type == "treemap":
        fig = go.Figure(
            go.Treemap(
                labels=labels,
                values=values,
                parents=[""] * len(labels),
                marker={"colors": CHART_COLORS[: len(labels)]},
                textinfo="label+value+percent root",
            ),
        )
        fig.update_layout(title=f"{col}")

    elif chart_type == "funnel":
        fig = go.Figure(
            go.Funnel(
                y=labels,
                x=values,
                marker={"color": CHART_COLORS[: len(labels)]},
                textinfo="value+percent initial",
            ),
        )
        fig.update_layout(title=f"{col}")

    elif chart_type == "waterfall":
        measures = ["relative"] * len(labels)
        fig = go.Figure(
            go.Waterfall(
                x=labels,
                y=values,
                measure=measures,
                connector={"line": {"color": "#1B2A4A"}},
                increasing={"marker": {"color": "#3AAFA9"}},
                decreasing={"marker": {"color": "#E74C3C"}},
                totals={"marker": {"color": "#2E5090"}},
                text=values,
                textposition="outside",
            ),
        )
        fig.update_layout(title=f"{col}", yaxis_title="العدد")

    elif chart_type == "gauge":
        top_val = values[0]
        total = sum(values)
        pct = (top_val / total * 100) if total else 0
        fig = go.Figure(
            go.Indicator(
                mode="gauge+number+delta",
                value=top_val,
                title={"text": f"{col}: {labels[0]}"},
                delta={"reference": total, "relative": False, "prefix": "من أصل "},
                gauge={
                    "axis": {"range": [0, total]},
                    "bar": {"color": "#2E5090"},
                    "bgcolor": "#F8F9FA",
                    "steps": [
                        {"range": [0, total * 0.5], "color": "#E8F5E9"},
                        {"range": [total * 0.5, total * 0.8], "color": "#FFF3E0"},
                        {"range": [total * 0.8, total], "color": "#FFEBEE"},
                    ],
                    "threshold": {
                        "line": {"color": "#E74C3C", "width": 3},
                        "thickness": 0.8,
                        "value": total * 0.8,
                    },
                },
            ),
        )
        fig.add_annotation(
            x=0.5,
            y=-0.15,
            xref="paper",
            yref="paper",
            text=f"{pct:.1f}% من الإجمالي ({total})",
            showarrow=False,
            font={"size": 14, "color": "#6C757D"},
        )

    elif chart_type == "kpi_cards":
        total = sum(values)
        count = len(labels)
        top_label = str(labels[0])
        top_val = values[0]
        bottom_label = str(labels[-1])
        bottom_val = values[-1]

        fig = go.Figure()
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=total,
                title={"text": "الإجمالي"},
                domain={"x": [0, 0.45], "y": [0.55, 1]},
            ),
        )
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=count,
                title={"text": "عدد الفئات"},
                domain={"x": [0.55, 1], "y": [0.55, 1]},
            ),
        )
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=top_val,
                title={"text": f"الأعلى: {top_label}"},
                domain={"x": [0, 0.45], "y": [0, 0.45]},
            ),
        )
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=bottom_val,
                title={"text": f"الأدنى: {bottom_label}"},
                domain={"x": [0.55, 1], "y": [0, 0.45]},
            ),
        )
        fig.update_layout(title=f"{col}")

    elif chart_type == "sunburst":
        fig = go.Figure(
            go.Sunburst(
                labels=labels,
                values=values,
                parents=[""] * len(labels),
                marker={"colors": CHART_COLORS[: len(labels)]},
                branchvalues="total",
                textinfo="label+percent root",
            ),
        )
        fig.update_layout(title=f"{col}")

    # إعدادات مشتركة
    fig.update_layout(
        font={"family": "Tahoma, Arial", "size": 13},
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
        margin={"l": 40, "r": 40, "t": 60, "b": 40},
    )

    # حفظ كصورة
    path = os.path.join(output_dir, f"chart_{index:02d}_{chart_type}.png")
    try:
        fig.write_image(path, width=700, height=420, scale=2)
        return path
    except Exception as e:
        print_err(f"خطأ في حفظ الرسم: {e}")
        return None


def normalize_code_columns(df: pd.DataFrame) -> pd.DataFrame:
    """توحيد أحرف الأكواد القصيرة (رحلات، لوحات) إلى أحرف كبيرة.

    لا يمس النصوص الطويلة مثل أسماء الشركات أو الإجراءات.
    """
    for col in df.columns:
        sample = df[col].dropna().head(1)
        if sample.empty or not isinstance(sample.iloc[0], str):
            continue
        vals = df[col].dropna()
        avg_len = vals.str.len().mean()
        has_spaces = vals.str.contains(" ", na=False).mean()
        if avg_len < 12 and has_spaces < 0.3:
            df[col] = df[col].str.upper().str.strip()
    return df


def inject_dispatch_columns(
    df: pd.DataFrame,
    chosen: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    """إذا اختار المستخدم عمودي وقت الوصول ووقت الإقلاع معاً.

    يُضاف تلقائياً: الوقت المتبقي + حالة التفويج.
    """
    arrival_col = None
    dep_col = None
    for c in chosen:
        if "وصول" in c and "حافل" in c:
            arrival_col = c
        elif "إقلاع" in c or "قلاع" in c:
            if "متبقي" not in c:
                dep_col = c
    if not arrival_col or not dep_col:
        return df, chosen
    col_remaining = "الوقت المتبقي"
    col_status = "حالة التفويج"
    if col_remaining in df.columns:
        return df, chosen
    remaining_vals = []
    status_vals = []
    for _, row in df.iterrows():
        arr = row[arrival_col]
        dep = row[dep_col]
        if isinstance(arr, dt_time) or hasattr(arr, "hour"):
            arr_minutes = arr.hour * 60 + arr.minute
        else:
            remaining_vals.append(None)
            status_vals.append(None)
            continue
        if isinstance(dep, dt_time) or hasattr(dep, "hour"):
            dep_minutes = dep.hour * 60 + dep.minute
        else:
            remaining_vals.append(None)
            status_vals.append(None)
            continue
        diff = dep_minutes - arr_minutes
        if diff < 0:
            diff += 24 * 60
        hours = diff // 60
        mins = diff % 60
        remaining_vals.append(f"{hours}:{mins:02d}")
        if diff >= 8 * 60:
            status_vals.append("مبكر")
        elif diff < 4 * 60:
            status_vals.append("متأخر")
        else:
            status_vals.append("في الموعد")
    df[col_remaining] = remaining_vals
    df[col_status] = status_vals
    new_chosen = []
    for c in chosen:
        new_chosen.append(c)
        if c == dep_col:
            new_chosen.append(col_remaining)
            new_chosen.append(col_status)
    return df, new_chosen
