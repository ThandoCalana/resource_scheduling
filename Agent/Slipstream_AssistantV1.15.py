import streamlit as st
import json
import _snowflake
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

session = get_active_session()

SEMANTIC_MODEL_PATH = "@SCHEDULE_DB.PUBLIC.MY_AI_MODELS/Employee_Schedule.yaml"
API_ENDPOINT = "/api/v2/cortex/analyst/message"

# ── Define function FIRST before anything else ──
def render_charts(df):
    """Automatically render relevant charts based on dataframe content."""
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    if "SCHEDULE_DATE" in df.columns and "MEETING_TITLE" in df.columns:
        daily_counts = df.groupby("SCHEDULE_DATE")["MEETING_TITLE"].count().reset_index()
        daily_counts.columns = ["Date", "Meeting Count"]
        st.plotly_chart(
            px.bar(daily_counts, x="Date", y="Meeting Count", title="📊 Meetings Per Day"),
            use_container_width=True
        )

    if "MEETING_TITLE" in df.columns and "TOTAL_MINUTES" in df.columns:
        top = df.groupby("MEETING_TITLE")["TOTAL_MINUTES"].sum().reset_index()
        top.columns = ["Meeting", "Total Minutes"]
        top = top.sort_values("Total Minutes", ascending=False).head(10)
        st.plotly_chart(
            px.bar(top, x="Total Minutes", y="Meeting", orientation="h",
                   title="⏱️ Top Meetings by Duration"),
            use_container_width=True
        )

    if "TIME_OF_DAY" in df.columns:
        tod = df["TIME_OF_DAY"].value_counts().reset_index()
        tod.columns = ["Time of Day", "Count"]
        st.plotly_chart(
            px.pie(tod, names="Time of Day", values="Count",
                   title="🕐 Meetings by Time of Day"),
            use_container_width=True
        )

    if "WORKLOAD_BAND" in df.columns:
        wb = df["WORKLOAD_BAND"].value_counts().reset_index()
        wb.columns = ["Workload Band", "Count"]
        st.plotly_chart(
            px.pie(wb, names="Workload Band", values="Count",
                   title="💼 Workload Distribution"),
            use_container_width=True
        )

    if "SCHEDULE_DATE" in df.columns and "LOAD_PERCENTAGE" in df.columns:
        st.plotly_chart(
            px.line(df.sort_values("SCHEDULE_DATE"),
                    x="SCHEDULE_DATE", y="LOAD_PERCENTAGE",
                    title="📈 Load Percentage Over Time"),
            use_container_width=True
        )

    if "DAY_OF_WEEK" in df.columns:
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        dow = df["DAY_OF_WEEK"].value_counts().reindex(dow_order).dropna().reset_index()
        dow.columns = ["Day", "Count"]
        st.plotly_chart(
            px.bar(dow, x="Day", y="Count", title="📅 Meetings by Day of Week"),
            use_container_width=True
        )

    if "EMPLOYEE_NAME" in df.columns and "TOTAL_MEETING_MINUTES" in df.columns:
        st.plotly_chart(
            px.bar(df, x="EMPLOYEE_NAME", y="TOTAL_MEETING_MINUTES",
                   title="👥 Total Meeting Minutes by Employee",
                   color="EMPLOYEE_NAME"),
            use_container_width=True
        )

    if "EMPLOYEE_NAME" in df.columns and "AVG_LOAD_PERCENTAGE" in df.columns:
        st.plotly_chart(
            px.bar(df, x="EMPLOYEE_NAME", y="AVG_LOAD_PERCENTAGE",
                   title="💼 Average Load Percentage by Employee",
                   color="EMPLOYEE_NAME"),
            use_container_width=True
        )

# ── App starts here ──
st.title("📅 Employee Schedule Agent")

if "messages" not in st.session_state:
    st.session_state.messages = []

if st.button("🔄 Reset Chat"):
    st.session_state.messages = []
    st.rerun()

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if "sql" in msg:
            with st.expander("Generated SQL"):
                st.code(msg["sql"], language="sql")
        if "dataframe" in msg:
            df = pd.DataFrame(msg["dataframe"])
            st.dataframe(df, use_container_width=True)
            render_charts(df)

prompt = st.chat_input("Ask something about the employee schedule...")

if prompt:
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    analyst_messages = [
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    ]

    request_body = {
        "messages": analyst_messages,
        "semantic_model_file": SEMANTIC_MODEL_PATH
    }

    with st.spinner("Thinking..."):
        try:
            resp = _snowflake.send_snow_api_request(
                "POST", API_ENDPOINT, {}, {}, request_body, None, 10000
            )

            if resp["status"] != 200:
                st.error(f"API Error {resp['status']}: {resp.get('content', 'Unknown error')}")
            else:
                response_dict = json.loads(resp["content"])
                content_blocks = response_dict.get("message", {}).get("content", [])

                reply_text = ""
                generated_sql = None

                for block in content_blocks:
                    if block["type"] == "text":
                        reply_text += block["text"]
                    elif block["type"] == "sql":
                        generated_sql = block["statement"]

                with st.chat_message("assistant"):
                    if reply_text:
                        st.markdown(reply_text)

                    if generated_sql:
                        with st.expander("Generated SQL"):
                            st.code(generated_sql, language="sql")
                        try:
                            df = session.sql(generated_sql).to_pandas()

                            cols = st.columns(3)
                            df_upper = df.copy()
                            df_upper.columns = [c.upper() for c in df.columns]
                            if "TOTAL_MINUTES" in df_upper.columns:
                                cols[0].metric("Total Meetings", len(df))
                                cols[1].metric("Total Minutes", f"{df_upper['TOTAL_MINUTES'].sum():.0f}")
                                cols[2].metric("Avg Duration", f"{df_upper['TOTAL_MINUTES'].mean():.0f} min")
                            else:
                                cols[0].metric("Total Rows", len(df))

                            st.dataframe(df, use_container_width=True)
                            render_charts(df)

                            saved_entry = {
                                "role": "assistant",
                                "content": reply_text or "See results above.",
                                "sql": generated_sql,
                                "dataframe": df.to_dict()
                            }
                        except Exception as e:
                            st.error(f"SQL execution error: {e}")
                            saved_entry = {"role": "assistant", "content": reply_text or "Error running SQL."}
                    else:
                        saved_entry = {"role": "assistant", "content": reply_text or "No response returned."}

                st.session_state.messages.append(saved_entry)

        except Exception as e:
            st.error(f"Request failed: {e}")