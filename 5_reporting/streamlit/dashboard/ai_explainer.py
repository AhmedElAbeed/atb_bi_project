"""Gemini-powered chart explanations for BI students."""
from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request

import pandas as pd
import streamlit as st

from dashboard.config import DEFAULT_GEMINI_API_KEY

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent"
)


def get_api_key() -> str:
    if st.session_state.get("gemini_api_key"):
        return str(st.session_state["gemini_api_key"]).strip()
    key = os.getenv("GEMINI_API_KEY", os.getenv("ATB_GEMINI_API_KEY", DEFAULT_GEMINI_API_KEY)).strip()
    if key:
        return key
    try:
        return str(st.secrets.get("GEMINI_API_KEY", DEFAULT_GEMINI_API_KEY)).strip()
    except Exception:
        return DEFAULT_GEMINI_API_KEY


def panels_enabled() -> bool:
    return st.session_state.get("ai_explain_enabled", True)


def _data_fingerprint(title: str, context: str, df: pd.DataFrame | None) -> str:
    parts = [title, context]
    if df is not None and not df.empty:
        parts.append(df.head(8).to_csv(index=False))
        parts.append(str(len(df)))
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def _summarize_df(df: pd.DataFrame | None, max_rows: int = 6) -> str:
    if df is None or df.empty:
        return "No tabular data attached."
    try:
        summary = df.describe(include="all").transpose().head(8).to_string()
    except Exception:
        summary = ""
    sample = df.head(max_rows).to_string(index=False)
    return f"Rows: {len(df)}\nSample:\n{sample}\n\nStats:\n{summary}"


def _call_gemini(prompt: str, api_key: str) -> str:
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.25, "maxOutputTokens": 220},
    }
    req = urllib.request.Request(
        f"{GEMINI_URL}?key={api_key}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    return body["candidates"][0]["content"]["parts"][0]["text"].strip()


@st.cache_data(ttl=3600, show_spinner=False)
def explain_chart(
    fingerprint: str,
    title: str,
    business_context: str,
    data_summary: str,
    api_key: str,
) -> str:
    prompt = f"""You are a BI tutor helping a banking student understand a dashboard chart.

Chart title: {title}
Business context: {business_context}
Data summary:
{data_summary}

Write a SHORT explanation (2-4 sentences, max 80 words) in plain English:
1. What the chart shows
2. One key insight the student should notice
3. Why it matters for ATB bank risk/portfolio analysis

No bullet points. No markdown. Be specific using the numbers if present."""

    return _call_gemini(prompt, api_key)


def _fallback_explanation(title: str, business_context: str) -> str:
    return (
        f"This chart shows {title.lower()}. {business_context} "
        "Look for outliers and concentration — they guide compliance and portfolio decisions."
    )


def render_ai_panel(
    chart_key: str,
    title: str,
    business_context: str,
    df: pd.DataFrame | None = None,
) -> None:
    """Always-visible robot panel beside charts."""
    st.markdown(
        """
        <div style="
            background:linear-gradient(145deg,rgba(99,102,241,0.15),rgba(56,189,248,0.08));
            border:1px solid rgba(129,140,248,0.45);
            border-radius:12px;padding:0.55rem 0.65rem;text-align:center;
        ">
            <div style="font-size:1.75rem;line-height:1;">🤖</div>
            <div style="font-size:0.7rem;color:#a5b4fc;font-weight:700;
                text-transform:uppercase;letter-spacing:0.08em;">AI Analyst</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    api_key = get_api_key()
    if not api_key:
        st.warning("No API key")
        return

    auto = st.session_state.get("ai_auto_explain", False)
    btn_key = f"ai_btn_{chart_key}"
    triggered = auto or st.session_state.get(btn_key, False)

    if not triggered:
        if st.button("Explain chart", key=btn_key, use_container_width=True, type="primary"):
            st.session_state[btn_key] = True
            st.rerun()
        return

    fingerprint = _data_fingerprint(title, business_context, df)
    data_summary = _summarize_df(df)
    try:
        with st.spinner("Analyzing…"):
            text = explain_chart(fingerprint, title, business_context, data_summary, api_key)
    except Exception:
        text = _fallback_explanation(title, business_context)

    st.markdown(f'<div class="ai-explain-box">{text}</div>', unsafe_allow_html=True)
