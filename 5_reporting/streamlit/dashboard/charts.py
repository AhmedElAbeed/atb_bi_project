"""Plotly chart helpers."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.config import ATB_PRIMARY, ATB_SILVER, CHART_COLORS, CHART_TEMPLATE, RISK_COLORS, RISK_ORDER


def safe_size_column(df: pd.DataFrame, column: str, fallback: float = 1.0) -> pd.Series:
    """Plotly marker size must be >= 0; clip negatives (overdraft balances)."""
    if column not in df.columns:
        return pd.Series([fallback] * len(df), index=df.index)
    sizes = pd.to_numeric(df[column], errors="coerce").fillna(0).clip(lower=0)
    if sizes.max() <= 0:
        return pd.Series([fallback] * len(df), index=df.index)
    return sizes


def apply_theme(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        template=CHART_TEMPLATE,
        height=height,
        margin=dict(t=44, b=48, l=52, r=28),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=ATB_SILVER, size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        title=dict(font=dict(size=15, color=ATB_SILVER)),
    )
    fig.update_xaxes(gridcolor="rgba(158,27,50,0.12)", zerolinecolor="rgba(192,192,192,0.15)")
    fig.update_yaxes(gridcolor="rgba(158,27,50,0.12)", zerolinecolor="rgba(192,192,192,0.15)")
    return fig


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str = "",
    orientation: str = "v",
    color: str | None = None,
    top_n: int | None = None,
    height: int = 380,
) -> go.Figure:
    data = df.copy()
    if top_n:
        data = data.nlargest(top_n, y) if orientation == "v" else data.nlargest(top_n, x)
    if color:
        fig = px.bar(data, x=x, y=y, color=color, orientation=orientation, title=title)
    else:
        fig = px.bar(
            data, x=x, y=y, orientation=orientation, title=title,
            color_discrete_sequence=CHART_COLORS,
        )
    return apply_theme(fig, height=height)


def pie_chart(
    df: pd.DataFrame,
    names: str,
    values: str,
    title: str = "",
    hole: float = 0.45,
    height: int = 380,
) -> go.Figure:
    fig = px.pie(
        df, names=names, values=values, title=title, hole=hole,
        color_discrete_sequence=CHART_COLORS,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return apply_theme(fig, height=height)


def risk_pie(df: pd.DataFrame, names: str = "risk_tier", values: str = "count") -> go.Figure:
    ordered = df.copy()
    if names in ordered.columns:
        ordered[names] = pd.Categorical(ordered[names], categories=RISK_ORDER, ordered=True)
        ordered = ordered.sort_values(names)
    fig = px.pie(
        ordered,
        names=names,
        values=values,
        color=names,
        color_discrete_map=RISK_COLORS,
        hole=0.45,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return apply_theme(fig)


def heatmap_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    z: str,
    title: str = "",
    height: int = 460,
) -> go.Figure:
    pivot = df.pivot_table(index=y, columns=x, values=z, aggfunc="mean")
    fig = px.imshow(
        pivot,
        aspect="auto",
        color_continuous_scale="RdYlGn_r",
        title=title,
        labels=dict(color=z.replace("_", " ").title()),
    )
    return apply_theme(fig, height=height)


def line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str | None = None,
    title: str = "",
    height: int = 380,
) -> go.Figure:
    if color:
        fig = px.line(df, x=x, y=y, color=color, markers=True, title=title)
    else:
        fig = px.line(df, x=x, y=y, markers=True, title=title, color_discrete_sequence=CHART_COLORS)
    return apply_theme(fig, height=height)


def horizontal_bar(
    df: pd.DataFrame,
    y: str,
    x: str,
    color: str | None = None,
    title: str = "",
    top_n: int | None = None,
    height: int = 420,
) -> go.Figure:
    data = df.nlargest(top_n, x) if top_n else df
    if color:
        fig = px.bar(data, y=y, x=x, orientation="h", color=color, title=title)
    else:
        fig = px.bar(
            data, y=y, x=x, orientation="h", title=title,
            color_discrete_sequence=CHART_COLORS,
        )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    return apply_theme(fig, height=height)
