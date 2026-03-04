# =============================================================================
# SCHEDULE INTELLIGENCE — AI-Powered Meeting & Workload Agent
# =============================================================================
# Built on Snowflake Cortex Analyst + Streamlit in Snowflake
# Author: Zuhayr Adams
#
# HOW THIS APP WORKS (three-stage pipeline per question):
#
#   STAGE 1 — PRONOUN RESOLUTION
#       Before anything reaches the SQL engine, Cortex Complete (mistral-large2)
#       rewrites the user's question to be fully explicit.
#       e.g. "is she busy that day?" → "Is Palesa busy on 2025-11-11?"
#
#   STAGE 2 — SQL GENERATION
#       The rewritten question is sent to Cortex Analyst, which reads the
#       semantic model YAML and translates the question into SQL automatically.
#
#   STAGE 3 — DISPLAY & ANALYSIS
#       The SQL is executed against Snowflake, results are displayed as a table
#       and auto-generated charts, then Cortex Complete writes a plain-English
#       summary of the key findings.
# =============================================================================

import streamlit as st          # Streamlit — the web app framework
import json                     # For parsing API response payloads
import _snowflake               # Internal Snowflake module for REST API calls
import pandas as pd             # For working with query result DataFrames
import plotly.express as px     # For rendering charts automatically
from snowflake.snowpark.context import get_active_session  # Snowflake DB session


# =============================================================================
# APP CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Schedule Intelligence",
    page_icon="📅",
    layout="wide",                      # Use the full browser width
    initial_sidebar_state="expanded"    # Open the sidebar panel on load
)

# Active Snowflake session — used to run SQL queries against the database
session = get_active_session()

# Path to the semantic model YAML stored in a Snowflake stage.
# This YAML file describes the database table to Cortex Analyst —
# what each column means, column types, and how to aggregate measures.
SEMANTIC_MODEL_PATH = "@SCHEDULE_DB.PUBLIC.MY_AI_MODELS/Employee_Schedule.yaml"

# The Cortex Analyst REST API endpoint.
# Called via _snowflake.send_snow_api_request() — NOT as a SQL function.
API_ENDPOINT = "/api/v2/cortex/analyst/message"


# =============================================================================
# CUSTOM STYLING
# =============================================================================
# All visual styling is injected as CSS via st.markdown().
# Dark theme: navy/slate palette with a blue (#63b3ed) accent colour.
# Fonts: DM Serif Display (headings) and DM Sans (body) from Google Fonts.

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap');

/* ── Global base styles ── */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #0e0f13;
    color: #e8e6e0;
}

/* Hide Streamlit's default top menu, footer, and header chrome */
#MainMenu, footer, header { visibility: hidden; }

/* Remove default page padding so content fills the screen */
.block-container {
    padding: 1.5rem 2rem 2rem 2rem !important;
    max-width: 100% !important;
}

/* ── Sidebar: force always visible ──
   Streamlit in Snowflake can collapse the sidebar unexpectedly.
   These rules pin it open regardless of the aria-expanded state. */
[data-testid="stSidebar"] {
    background: #13151c !important;
    border-right: 1px solid #1e2130 !important;
    transform: translateX(0) !important;
    visibility: visible !important;
    display: block !important;
    min-width: 280px !important;
}
[data-testid="stSidebar"][aria-expanded="false"] {
    transform: translateX(0) !important;
    margin-left: 0 !important;
    min-width: 280px !important;
}
/* Hide the native collapse arrow button entirely */
[data-testid="stSidebarCollapsedControl"] { display: none !important; }

[data-testid="stSidebar"] > div:first-child { padding: 1rem 0.8rem !important; }
[data-testid="stSidebar"] * { color: #e8e6e0 !important; }

/* Sidebar button styling */
[data-testid="stSidebar"] .stButton > button {
    background: #1a1d28 !important;
    border: 1px solid #2d3348 !important;
    color: #e8e6e0 !important;
    border-radius: 8px !important;
    font-size: 0.78rem !important;
    width: 100% !important;
    margin-bottom: 0.2rem !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    border-color: #63b3ed !important;
    color: #63b3ed !important;
}

/* Sidebar metric card styling */
[data-testid="stSidebar"] [data-testid="stMetric"] {
    background: #1a1d28 !important;
    border: 1px solid #1e2130 !important;
    border-radius: 8px !important;
    padding: 0.6rem 0.7rem !important;
}
[data-testid="stSidebar"] [data-testid="stMetricValue"] {
    font-size: 1.2rem !important;
    color: #63b3ed !important;
}
[data-testid="stSidebar"] [data-testid="stMetricLabel"] {
    font-size: 0.65rem !important;
    color: #6b7280 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
}

/* ── Hero banner ── */
.hero {
    background: linear-gradient(135deg, #13151c 0%, #1a1d28 50%, #13151c 100%);
    border: 1px solid #1e2130;
    border-radius: 16px;
    padding: 1.5rem 2rem;
    margin-bottom: 1.2rem;
    position: relative;
    overflow: hidden;
}
/* Decorative radial glow in the top-right corner */
.hero::before {
    content: '';
    position: absolute;
    top: -50%; right: -10%;
    width: 400px; height: 400px;
    background: radial-gradient(circle, rgba(99,179,237,0.06) 0%, transparent 70%);
    pointer-events: none;
}
.hero-title {
    font-family: 'DM Serif Display', serif;
    font-size: 2rem; color: #f0ede6;
    margin: 0; letter-spacing: -0.02em;
}
.hero-subtitle {
    font-size: 0.78rem; color: #6b7280;
    margin-top: 0.3rem; text-transform: uppercase; letter-spacing: 0.06em;
}
.hero-accent { color: #63b3ed; }

/* ── Result metric cards ── */
[data-testid="stMetric"] {
    background: #1a1d28;
    border: 1px solid #1e2130;
    border-radius: 10px;
    padding: 0.8rem 1rem !important;
}
[data-testid="stMetricLabel"] {
    color: #6b7280 !important; font-size: 0.7rem !important;
    text-transform: uppercase; letter-spacing: 0.08em;
}
[data-testid="stMetricValue"] {
    color: #63b3ed !important;
    font-family: 'DM Serif Display', serif;
    font-size: 1.6rem !important;
}

/* ── General buttons ── */
.stButton > button {
    background: #1a1d28 !important;
    color: #e8e6e0 !important;
    border: 1px solid #2d3348 !important;
    border-radius: 8px !important;
    font-size: 0.82rem !important;
    transition: all 0.2s ease !important;
}
.stButton > button:hover { border-color: #63b3ed !important; color: #63b3ed !important; }

/* ── Chat UI elements ── */
[data-testid="stChatMessage"] { background: transparent !important; border: none !important; padding: 0.4rem 0 !important; }
[data-testid="stChatInput"]   { border: 1px solid #2d3348 !important; border-radius: 12px !important; background: #1a1d28 !important; }
[data-testid="stChatInput"]:focus-within { border-color: #63b3ed !important; box-shadow: 0 0 0 3px rgba(99,179,237,0.1) !important; }
[data-testid="stDataFrame"]   { border: 1px solid #1e2130 !important; border-radius: 12px !important; overflow: hidden; }
[data-testid="stExpander"]    { background: #1a1d28 !important; border: 1px solid #1e2130 !important; border-radius: 10px !important; }

/* Blue callout: shows Cortex Analyst's interpretation of the question */
.interpretation {
    background: rgba(99,179,237,0.05); border-left: 3px solid #63b3ed;
    border-radius: 0 8px 8px 0; padding: 0.6rem 1rem;
    margin-bottom: 0.6rem; font-size: 0.85rem; color: #9ca3af; font-style: italic;
}

/* Green callout: shown when a question was auto-rewritten to resolve pronouns */
.rewritten-q {
    background: rgba(104,211,145,0.05); border-left: 3px solid #68d391;
    border-radius: 0 8px 8px 0; padding: 0.5rem 0.9rem;
    margin-bottom: 0.6rem; font-size: 0.8rem; color: #68d391;
}

/* Small uppercase section label used in the sidebar */
.panel-label {
    font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.12em;
    color: #4b5563; font-weight: 600; margin: 1rem 0 0.4rem 0; display: block;
}

/* Animated green dot showing live Snowflake connection */
.status-dot {
    display: inline-block; width: 6px; height: 6px;
    background: #34d399; border-radius: 50%; margin-right: 0.4rem;
    animation: pulse 2s infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# CHART CONFIGURATION
# =============================================================================
# Shared colour palette used across all charts for visual consistency.

COLOR_SEQ = ["#63b3ed", "#f6ad55", "#68d391", "#fc8181", "#b794f4", "#76e4f7"]
BG   = "#13151c"   # Chart background — matches the app dark theme
GRID = "#1e2130"   # Chart gridline colour


def style_fig(fig, title=""):
    """
    Apply the app's dark theme to a Plotly chart.

    Every chart created in render_charts() passes through this function
    to ensure consistent colours, fonts, and grid styling across all chart types.

    Args:
        fig   : A Plotly figure object (bar, line, pie, etc.)
        title : The chart title string — supports emoji e.g. "⏱ Total Minutes"

    Returns:
        The same figure object with the dark theme applied
    """
    fig.update_layout(
        title=dict(text=title, font=dict(family="DM Serif Display", size=15, color="#e8e6e0")),
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        font=dict(color="#9ca3af", family="DM Sans"),
        margin=dict(t=45, b=25, l=25, r=25),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=GRID),
        xaxis=dict(gridcolor=GRID, linecolor=GRID, zerolinecolor=GRID),
        yaxis=dict(gridcolor=GRID, linecolor=GRID, zerolinecolor=GRID),
    )
    return fig


def render_charts(df):
    """
    Automatically detect and render the most relevant charts for a query result.

    Instead of hardcoding charts, this function inspects which columns exist
    in the returned DataFrame and renders only the charts that make sense
    for that data — no manual configuration needed.

    Supported chart types (triggered by column combinations):
        EMPLOYEE_NAME + TOTAL_MEETING_MINUTES → Employee comparison bar chart
        SCHEDULE_DATE + MEETING_TITLE         → Meetings per day bar chart
        MEETING_TITLE + TOTAL_MINUTES         → Top meetings by duration (horizontal)
        TIME_OF_DAY                           → Time of day donut chart
        WORKLOAD_BAND                         → Workload distribution donut
        SCHEDULE_DATE + LOAD_PERCENTAGE       → Load % over time line chart
        DAY_OF_WEEK                           → Meetings by day of week bar chart
        MONTH_NAME + TOTAL_MINUTES            → Monthly trend line chart

    Args:
        df : pandas DataFrame returned from executing the Cortex Analyst SQL
    """
    # Normalise all column names to uppercase for consistent lookups
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    # ── Employee comparison: total meeting minutes per person ──
    if "EMPLOYEE_NAME" in df.columns and "TOTAL_MEETING_MINUTES" in df.columns:
        c1, c2 = st.columns(2)
        with c1:
            fig = px.bar(df, x="EMPLOYEE_NAME", y="TOTAL_MEETING_MINUTES",
                         color="EMPLOYEE_NAME", color_discrete_sequence=COLOR_SEQ)
            st.plotly_chart(style_fig(fig, "⏱ Total Meeting Minutes"), use_container_width=True)
        # If avg load % is also present, show it alongside in the second column
        if "AVG_LOAD_PERCENTAGE" in df.columns:
            with c2:
                fig = px.bar(df, x="EMPLOYEE_NAME", y="AVG_LOAD_PERCENTAGE",
                             color="EMPLOYEE_NAME", color_discrete_sequence=COLOR_SEQ)
                st.plotly_chart(style_fig(fig, "💼 Avg Load %"), use_container_width=True)

    # ── Meetings per day: count how many meetings occurred on each date ──
    if "SCHEDULE_DATE" in df.columns and "MEETING_TITLE" in df.columns:
        daily = df.groupby("SCHEDULE_DATE")["MEETING_TITLE"].count().reset_index()
        daily.columns = ["Date", "Meetings"]
        fig = px.bar(daily, x="Date", y="Meetings", color_discrete_sequence=["#63b3ed"])
        st.plotly_chart(style_fig(fig, "📅 Meetings Per Day"), use_container_width=True)

    # ── Top 10 meetings by total duration: horizontal bar ──
    if "MEETING_TITLE" in df.columns and "TOTAL_MINUTES" in df.columns:
        top = df.groupby("MEETING_TITLE")["TOTAL_MINUTES"].sum().reset_index()
        top = top.sort_values("TOTAL_MINUTES", ascending=True).tail(10)
        top.columns = ["Meeting", "Minutes"]
        fig = px.bar(top, x="Minutes", y="Meeting", orientation="h",
                     color="Minutes", color_continuous_scale=["#1a2744", "#63b3ed"])
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(style_fig(fig, "⏱ Top Meetings by Duration"), use_container_width=True)

    # ── Time of day + workload band: two donut charts side by side ──
    if "TIME_OF_DAY" in df.columns:
        c1, c2 = st.columns(2)
        with c1:
            tod = df["TIME_OF_DAY"].value_counts().reset_index()
            tod.columns = ["Time", "Count"]
            fig = px.pie(tod, names="Time", values="Count",
                         color_discrete_sequence=COLOR_SEQ, hole=0.5)
            fig.update_traces(textfont_color="#e8e6e0")
            st.plotly_chart(style_fig(fig, "🕐 Time of Day"), use_container_width=True)
        if "WORKLOAD_BAND" in df.columns:
            with c2:
                wb = df["WORKLOAD_BAND"].value_counts().reset_index()
                wb.columns = ["Band", "Count"]
                fig = px.pie(wb, names="Band", values="Count",
                             color_discrete_sequence=COLOR_SEQ, hole=0.5)
                fig.update_traces(textfont_color="#e8e6e0")
                st.plotly_chart(style_fig(fig, "💼 Workload"), use_container_width=True)

    # ── Load % over time: line chart, one line per employee if multiple ──
    if "SCHEDULE_DATE" in df.columns and "LOAD_PERCENTAGE" in df.columns:
        color_col = "EMPLOYEE_NAME" if "EMPLOYEE_NAME" in df.columns else None
        fig = px.line(df.sort_values("SCHEDULE_DATE"), x="SCHEDULE_DATE", y="LOAD_PERCENTAGE",
                      color=color_col, color_discrete_sequence=COLOR_SEQ, markers=True)
        fig.update_traces(line_width=2)
        st.plotly_chart(style_fig(fig, "📈 Load % Over Time"), use_container_width=True)

    # ── Meetings by day of week: enforces Mon→Sun order (not alphabetical) ──
    if "DAY_OF_WEEK" in df.columns:
        order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        dow = df["DAY_OF_WEEK"].value_counts().reindex(order).dropna().reset_index()
        dow.columns = ["Day", "Count"]
        fig = px.bar(dow, x="Day", y="Count",
                     color="Count", color_continuous_scale=["#1a2744", "#63b3ed"])
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(style_fig(fig, "📆 Day of Week"), use_container_width=True)

    # ── Monthly trend: total meeting minutes per month in calendar order ──
    if "MONTH_NAME" in df.columns and "TOTAL_MINUTES" in df.columns:
        month_order = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        color_col = "EMPLOYEE_NAME" if "EMPLOYEE_NAME" in df.columns else None
        grp = ["MONTH_NAME"] + ([color_col] if color_col else [])
        monthly = df.groupby(grp)["TOTAL_MINUTES"].sum().reset_index()
        # Use pd.Categorical to ensure months sort in calendar order, not alphabetically
        monthly["MONTH_NAME"] = pd.Categorical(monthly["MONTH_NAME"],
                                               categories=month_order, ordered=True)
        fig = px.line(monthly.sort_values("MONTH_NAME"), x="MONTH_NAME", y="TOTAL_MINUTES",
                      color=color_col, color_discrete_sequence=COLOR_SEQ, markers=True)
        st.plotly_chart(style_fig(fig, "📊 Monthly Minutes"), use_container_width=True)


# =============================================================================
# CORE AI FUNCTIONS
# =============================================================================

def resolve_pronouns(user_prompt, history):
    """
    Rewrite a vague or pronoun-containing question into a fully explicit one.

    Cortex Analyst is a SQL engine — it cannot resolve conversational references
    like "she", "that day", or "the same person" on its own. This function
    pre-processes every question using Cortex Complete (mistral-large2) BEFORE
    it reaches Cortex Analyst, making the question fully self-contained.

    Example:
        Previous message : "Show me Palesa's meetings"
        User now types   : "Is she busy that day?"
        Rewritten to     : "Is Palesa busy on 2025-11-15?"

    Args:
        user_prompt : The raw question the user typed
        history     : Full conversation history from st.session_state.messages

    Returns:
        Tuple of (rewritten_question: str, was_changed: bool)
        Returns the original prompt unchanged if no rewriting was needed.
    """
    # No history means no context to resolve pronouns against — skip
    if not history:
        return user_prompt, False

    # Build a readable text summary of the last 8 messages for the LLM
    history_text = ""
    for m in history[-8:]:
        if m["role"] == "user":
            history_text += f"User: {m['content']}\n"
        elif m["role"] == "assistant":
            # Prefer the Cortex Analyst interpretation summary over the full reply
            ref = m.get("interpretation") or m.get("content", "")
            if ref and ref not in ("See results below.", ""):
                history_text += f"Assistant: {ref}\n"

    # Prompt instructs the LLM to rewrite only if truly necessary,
    # and to return just the final question — no preamble or explanation
    rewrite_prompt = f"""You are a question rewriter for a scheduling analytics assistant.
Conversation so far:
{history_text}
New question: "{user_prompt}"
If the question contains pronouns (she, he, they, her, him, it) or vague references \
(that day, same person, that meeting, this week), rewrite it as fully explicit using \
names and details from the conversation.
If already clear, return unchanged.
Return ONLY the final question. No explanation. No quotes."""

    # Escape single quotes to prevent breaking the SQL string wrapper
    safe = rewrite_prompt.replace("'", "''")
    try:
        result = session.sql(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{safe}') AS Q"
        ).collect()
        rewritten = result[0]["Q"].strip().strip('"').strip("'")
        # Only flag as changed if the rewrite is meaningfully different
        changed = rewritten.lower().strip() != user_prompt.lower().strip()
        return rewritten, changed
    except Exception:
        # If the LLM call fails for any reason, use the original question as fallback
        return user_prompt, False


def get_nl_summary(question, df):
    """
    Generate a plain-English narrative analysis of the query results.

    After SQL is executed and data is fetched, this function sends the original
    question and the first 50 rows to mistral-large2 and asks it to write a
    concise, conversational 3-5 sentence summary of what the data shows.

    This gives the agent an "analyst voice" — instead of just showing a table,
    it tells the user what the data means, highlights patterns, and calls out
    specific numbers or outliers.

    Args:
        question : The resolved question that was sent to Cortex Analyst
        df       : pandas DataFrame of results from the executed SQL

    Returns:
        A plain-English summary string, or None if the LLM call fails
    """
    try:
        # Pass up to 50 rows as CSV so the LLM can read and interpret the data
        csv_preview = df.head(50).to_csv(index=False)

        summary_prompt = f"""You are an analyst for a team scheduling tool.
User asked: "{question}"
Data returned:
{csv_preview}
Write a 3-5 sentence natural language summary of key findings. \
Use specific names and numbers. Be conversational. No bullet points. No preamble."""

        safe = summary_prompt.replace("'", "''")
        result = session.sql(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{safe}') AS S"
        ).collect()
        return result[0]["S"].strip()
    except Exception:
        # Summary is enhancement-only — silently skip if it fails
        return None


def render_ai_analysis(text):
    """
    Display the AI-generated summary in a styled card below the data table.

    Renders the output of get_nl_summary() inside a dark card clearly labelled
    as 'AI Analysis' so users understand it is an LLM interpretation,
    not raw data from the database.

    Args:
        text : The natural language summary string to display
    """
    st.markdown(
        '<div style="background:rgba(99,179,237,0.04);border:1px solid #1e2130;'
        'border-radius:12px;padding:1rem 1.2rem;margin-top:1rem;">'
        '<div style="font-size:0.62rem;text-transform:uppercase;letter-spacing:0.1em;'
        'color:#4b5563;font-weight:600;margin-bottom:0.5rem;">✦ AI Analysis</div>'
        '<div style="color:#c9c5bc;font-size:0.9rem;line-height:1.7;">'
        + text +
        '</div></div>',
        unsafe_allow_html=True
    )


# =============================================================================
# SESSION STATE INITIALISATION
# =============================================================================
# Streamlit rerenders the entire script on every user interaction.
# st.session_state persists values across rerenders within the same browser session.
# Keys are initialised once with defaults — existing values are never overwritten.

SESSION_DEFAULTS = {
    "messages":      [],   # Full conversation history (user + assistant turns)
    "total_queries": 0,    # Number of questions asked this session
    "total_rows":    0,    # Total database rows fetched this session
    "charts_shown":  0,    # Total charts rendered this session
}
for key, default in SESSION_DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = default


# =============================================================================
# SIDEBAR
# =============================================================================
# Always-visible side panel containing: connection status, semantic model info,
# live session stats, quick-prompt shortcut buttons, and a clear chat button.

with st.sidebar:

    # ── Live connection indicator ──
    st.markdown(
        '<span class="status-dot"></span>'
        '<span style="font-size:0.78rem;color:#6b7280;">Connected to Snowflake</span>',
        unsafe_allow_html=True
    )

    # ── Semantic model reference ──
    # Tells users which YAML file is powering the agent
    st.markdown('<span class="panel-label">Semantic Model</span>', unsafe_allow_html=True)
    st.markdown(
        '<div style="background:#1a1d28;border:1px solid #1e2130;border-radius:8px;'
        'padding:0.6rem 0.8rem;font-size:0.78rem;color:#9ca3af;">'
        '📄 Employee_Schedule.yaml<br>'
        '<span style="color:#4b5563;font-size:0.68rem;">SCHEDULE_DB · PUBLIC</span>'
        '</div>',
        unsafe_allow_html=True
    )

    # ── Session statistics ──
    # Live counts that update as the user asks more questions
    st.markdown('<span class="panel-label">Session Stats</span>', unsafe_allow_html=True)
    s1, s2 = st.columns(2)
    s1.metric("Queries",  st.session_state.total_queries)
    s2.metric("Messages", len(st.session_state.messages))
    s1.metric("Rows",     f"{st.session_state.total_rows:,}")
    s2.metric("Charts",   st.session_state.charts_shown)

    # ── Quick prompt buttons ──
    # Clicking a button saves the prompt text to session state under "injected_prompt".
    # The chat input section below picks this up and treats it as a typed message.
    st.markdown('<span class="panel-label">Quick Prompts</span>', unsafe_allow_html=True)
    QUICK_PROMPTS = [
        "Compare all employees workload",
        "Who has the most meetings?",
        "Show Andrew's busiest days",
        "Meetings by day of week",
        "Monthly summary for all staff",
        "Show all High load days",
        "Average meeting duration by person",
        "Morning vs afternoon meetings",
    ]
    for s in QUICK_PROMPTS:
        if st.button(s, key=f"sug_{s}", use_container_width=True):
            st.session_state["injected_prompt"] = s
            st.rerun()

    # ── Clear chat button ──
    st.markdown(
        "<hr style='border:none;border-top:1px solid #1e2130;margin:0.8rem 0'>",
        unsafe_allow_html=True
    )
    if st.button("🔄 Clear Chat", use_container_width=True):
        # Reset all session tracking back to defaults
        for k in ["messages", "total_queries", "total_rows", "charts_shown"]:
            st.session_state[k] = [] if k == "messages" else 0
        st.rerun()


# =============================================================================
# HERO BANNER
# =============================================================================

st.markdown("""
<div class="hero">
    <div class="hero-title">Schedule <span class="hero-accent">Intelligence</span></div>
    <div class="hero-subtitle">Snowflake Cortex Analyst · Slipstream Data</div>
</div>
""", unsafe_allow_html=True)


# =============================================================================
# EMPTY STATE
# =============================================================================
# Shown only when no conversation has started yet.
# Displays a welcome message and example prompt buttons to help users get started.

if not st.session_state.messages:
    st.markdown("""
    <div style="text-align:center;padding:2rem 1rem 1.5rem 1rem;">
        <div style="font-size:3rem;margin-bottom:0.8rem;">📅</div>
        <div style="font-family:'DM Serif Display',serif;font-size:1.5rem;
                    color:#e8e6e0;margin-bottom:0.4rem;">
            Ask anything about your team's schedule
        </div>
        <div style="color:#6b7280;font-size:0.88rem;max-width:460px;
                    margin:0 auto 1.5rem auto;">
            Analyse meetings, compare workloads, spot patterns — just ask in plain English.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Six example prompt chips in a 3-column grid
    EXAMPLE_PROMPTS = [
        ("📋", "Show Palesa's meetings this month"),
        ("⚖️", "Compare Robin and Andrew's workload"),
        ("🔥", "Who has the busiest Mondays?"),
        ("📊", "Monthly meeting hours per employee"),
        ("🕐", "What time of day are most meetings?"),
        ("💼", "Show all High workload days"),
    ]
    cols = st.columns(3)
    for i, (icon, label) in enumerate(EXAMPLE_PROMPTS):
        with cols[i % 3]:
            if st.button(f"{icon} {label}", key=f"ex_{i}", use_container_width=True):
                st.session_state["injected_prompt"] = label
                st.rerun()


# =============================================================================
# CHAT HISTORY
# =============================================================================
# Streamlit rerenders the full page on every interaction, so we replay all
# previous messages from session state to rebuild the visible conversation thread.
# Each message is re-rendered with its saved SQL, data table, charts, and summary.

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):

        # Green banner: shown when this question was rewritten to resolve pronouns
        if msg.get("rewritten_q"):
            st.markdown(
                f'<div class="rewritten-q">🔁 Interpreted as: '
                f'<em>{msg["rewritten_q"]}</em></div>',
                unsafe_allow_html=True
            )

        # Blue banner: Cortex Analyst's own interpretation of the question
        if msg.get("interpretation"):
            st.markdown(
                f'<div class="interpretation">🎯 {msg["interpretation"]}</div>',
                unsafe_allow_html=True
            )

        # Main message text body
        st.markdown(msg["content"])

        # Collapsible SQL block (assistant messages that executed a query only)
        if "sql" in msg:
            with st.expander("🔍 View Generated SQL"):
                st.code(msg["sql"], language="sql")

        # Re-render data table and auto-charts from the saved DataFrame
        if "dataframe" in msg:
            df = pd.DataFrame(msg["dataframe"])
            st.dataframe(df, use_container_width=True, hide_index=True)
            render_charts(df)

        # Re-render the AI analysis summary card
        if "summary" in msg:
            render_ai_analysis(msg["summary"])


# =============================================================================
# CHAT INPUT
# =============================================================================
# The chat input box sits at the bottom of the page.
# Sidebar buttons and example chips write to st.session_state["injected_prompt"],
# which is picked up here and treated identically to a typed message.

prompt = st.chat_input("Ask about meetings, workload, schedules...")

# Override with an injected prompt if a button was clicked
if "injected_prompt" in st.session_state:
    prompt = st.session_state.pop("injected_prompt")


# =============================================================================
# MAIN AGENT PIPELINE
# =============================================================================
# Runs whenever the user submits a question (typed or via a button).
#
# STAGE 1 → resolve_pronouns()   — rewrite vague question to be explicit
# STAGE 2 → Cortex Analyst API   — translate question to SQL, get interpretation
# STAGE 3 → Execute + display    — run SQL, show table, charts, and LLM summary

if prompt:

    # Display the user message and save it to conversation history
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.session_state.total_queries += 1

    with st.spinner("Thinking..."):

        # ── STAGE 1: Resolve pronouns ──────────────────────────────────────
        # Pass conversation history (excluding the just-added message) to
        # resolve any vague references before querying Cortex Analyst.
        history = st.session_state.messages[:-1]
        resolved_prompt, was_rewritten = resolve_pronouns(prompt, history)

        # ── STAGE 2: Call Cortex Analyst ───────────────────────────────────
        # Send the explicit, resolved question to the Cortex Analyst REST API.
        # Cortex Analyst reads the semantic model YAML and returns generated SQL
        # plus a plain-English interpretation of how it understood the question.
        #
        # NOTE: Cortex Analyst only accepts "user" as the message role.
        # It does NOT support multi-turn "assistant" messages — attempting to
        # include assistant history causes a 400 error. Context is handled
        # entirely by Stage 1's pronoun resolution instead.
        request_body = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": resolved_prompt}]
                }
            ],
            "semantic_model_file": SEMANTIC_MODEL_PATH
        }

        try:
            # Call the Cortex Analyst API using Snowflake's internal HTTP client.
            # Authentication is handled automatically by the active Snowpark session.
            resp = _snowflake.send_snow_api_request(
                "POST",        # HTTP method
                API_ENDPOINT,  # /api/v2/cortex/analyst/message
                {},            # Extra headers — none required
                {},            # Query string parameters — none required
                request_body,  # JSON body with the user question
                None,          # No streaming
                10000          # Timeout: 10 seconds
            )

            # ── Handle API error responses ──
            if resp["status"] != 200:
                err = json.loads(resp.get("content", "{}"))
                st.error(f"⚠️ {err.get('message', 'Unknown error')}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": err.get("message", "Error")
                })

            else:
                # ── Parse the response content blocks ──
                # The response is a list of blocks, each with a "type" field.
                # type "text" → Cortex Analyst's interpretation of the question
                # type "sql"  → The generated SQL statement ready to execute
                blocks = json.loads(resp["content"]).get("message", {}).get("content", [])
                reply_text     = ""
                generated_sql  = None
                interpretation = None

                for block in blocks:
                    if block["type"] == "text":
                        t = block["text"]
                        # Extract and clean the interpretation sentence if present
                        if "This is our interpretation" in t:
                            lines = t.split("\n", 1)
                            interpretation = lines[0].replace(
                                "This is our interpretation of your question:", ""
                            ).strip()
                            reply_text += lines[1].strip() if len(lines) > 1 else ""
                        else:
                            reply_text += t
                    elif block["type"] == "sql":
                        generated_sql = block["statement"]

                # Prepare the dict that will be saved to conversation history
                saved_entry = {
                    "role":           "assistant",
                    "content":        reply_text or "See results below.",
                    "interpretation": interpretation,
                    "rewritten_q":    resolved_prompt if was_rewritten else None
                }

                # ── Render the assistant response in the chat ──
                with st.chat_message("assistant"):

                    # Show green banner if the question was rewritten
                    if was_rewritten:
                        st.markdown(
                            f'<div class="rewritten-q">🔁 Interpreted as: '
                            f'<em>{resolved_prompt}</em></div>',
                            unsafe_allow_html=True
                        )

                    # Show blue banner with Cortex Analyst's interpretation
                    if interpretation:
                        st.markdown(
                            f'<div class="interpretation">🎯 {interpretation}</div>',
                            unsafe_allow_html=True
                        )

                    # Show any plain text included in the response
                    if reply_text:
                        st.markdown(reply_text)

                    # ── STAGE 3: Execute SQL and display results ───────────
                    if generated_sql:

                        # Show generated SQL in a collapsible block for transparency
                        with st.expander("🔍 View Generated SQL"):
                            st.code(generated_sql, language="sql")

                        try:
                            # Run the SQL against Snowflake
                            df    = session.sql(generated_sql).to_pandas()
                            df_up = df.copy()
                            df_up.columns = [c.upper() for c in df.columns]

                            # Update the session row count stat in the sidebar
                            st.session_state.total_rows += len(df)

                            # ── Summary metric cards ──
                            # Show key numbers above the table.
                            # Which metrics appear depends on the columns returned.
                            mc = st.columns(4)
                            mc[0].metric("Rows", f"{len(df):,}")

                            if "TOTAL_MINUTES" in df_up.columns:
                                # Shown for detail queries (e.g. "show Andrew's meetings")
                                tm = df_up["TOTAL_MINUTES"].sum()
                                mc[1].metric("Total Minutes", f"{tm:,.0f}")
                                mc[2].metric("Avg Duration",  f"{df_up['TOTAL_MINUTES'].mean():.0f} min")
                                mc[3].metric("Total Hours",   f"{tm / 60:.1f} hrs")

                            elif "TOTAL_MEETING_MINUTES" in df_up.columns:
                                # Shown for comparison queries (e.g. "compare Robin and Andrew")
                                tm = df_up["TOTAL_MEETING_MINUTES"].sum()
                                mc[1].metric("Total Minutes", f"{tm:,.0f}")
                                mc[2].metric("Total Hours",   f"{tm / 60:.1f} hrs")
                                if "AVG_LOAD_PERCENTAGE" in df_up.columns:
                                    mc[3].metric("Avg Load %",
                                                 f"{df_up['AVG_LOAD_PERCENTAGE'].mean() * 100:.0f}%")

                            # ── Data table ──
                            st.dataframe(df, use_container_width=True, hide_index=True)

                            # ── Auto charts ──
                            # render_charts() inspects the column names in df and
                            # renders whichever chart types are relevant for that data
                            render_charts(df)
                            st.session_state.charts_shown += 1

                            # ── Natural language summary ──
                            # Send the question + data to mistral-large2 for a
                            # plain-English analysis of the findings (Stage 3 final step)
                            summary = get_nl_summary(resolved_prompt, df)
                            if summary:
                                render_ai_analysis(summary)
                                saved_entry["summary"] = summary

                            # Persist SQL and DataFrame data to session state
                            # so they can be replayed when the page rerenders
                            saved_entry["sql"]       = generated_sql
                            saved_entry["dataframe"] = df.to_dict()

                        except Exception as e:
                            st.error(f"SQL error: {e}")

                # Save the complete assistant response to conversation history
                st.session_state.messages.append(saved_entry)

        except Exception as e:
            st.error(f"Request failed: {e}")