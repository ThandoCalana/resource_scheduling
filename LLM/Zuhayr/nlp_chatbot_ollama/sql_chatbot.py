import os
import re
import datetime as dt
import pandas as pd
import urllib.parse
import ollama

from dotenv import loaddotenv
from typing import Optional, Tuple, List, Dict, Any
from sqlalchemy import create_engine, text
loaddotenv()

# CONFIG 
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
TABLE_NAME = "meetings"
LLAMA_MODEL = "llama3.2"
DEFAULT_LIMIT = 200
MAX_CONTEXT_ROWS = 100
CONVERSATION_HISTORY_LIMIT = 10

# Building SQLAlchemy engine
def build_sql_engine():
    if not SQL_SERVER or not SQL_DATABASE:
        raise ValueError("Set SQL_SERVER and SQL_DATABASE")
    
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
    params = urllib.parse.quote_plus(conn_str)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
    return engine

# Enhanced query detection 
WEEKDAY_MAP = {
    'monday': 'Monday', 'tuesday': 'Tuesday', 'wednesday': 'Wednesday',
    'thursday': 'Thursday', 'friday': 'Friday', 'saturday': 'Saturday', 'sunday': 'Sunday'
}

def extract_query_intent(user_text: str, conversation_history: List[Dict]) -> Dict[str, Any]:
    """
    Analyze user intent and extract relevant filters.
    Returns dict with: intent, filters, needs_context
    """
    text = user_text.strip().lower()
    
    # Check for conversational/chitchat patterns
    chitchat_patterns = [
        r'^(hi|hello|hey|howdy)\b',
        r'^(thanks|thank you|thx)',
        r'^(bye|goodbye|see you)',
        r'^(how are you|what\'s up)',
        r'^(ok|okay|alright|cool|great|nice)',
    ]
    for pattern in chitchat_patterns:
        if re.search(pattern, text):
            return {"intent": "chitchat", "needs_context": False}
    
    # Checking for follow-up/clarification (uses pronouns or references)
    follow_up_patterns = [
        r'\b(what about|and for|how about)\b',
        r'\b(him|her|them|that|those|this|these)\b',
        r'\b(also|additionally|moreover)\b',
        r'^(and|but|so|then)\b',
    ]
    is_follow_up = any(re.search(p, text) for p in follow_up_patterns)
    
    intent = {
        "intent": "query",
        "is_follow_up": is_follow_up,
        "needs_context": True,
        "filters": {}
    }
    
    # Detecting specific query types FIRST (before extracting filters)
    if re.search(r'\b(who|which person|whose)\b.*\b(busy|busiest|most|load|overload|stress|heavy|full)', text):
        intent["query_type"] = "find_busiest"
        intent["filters"]["aggregate"] = True
        return intent
    elif re.search(r'\b(busy|load|overload|stress|heavy|full)', text):
        intent["query_type"] = "load_analysis"
        threshold = detect_high_load(user_text)
        if threshold:
            intent["filters"]["load_threshold"] = threshold
    elif re.search(r'\b(free|available|open|gap|break)', text):
        intent["query_type"] = "availability"
    elif re.search(r'\b(next|upcoming|future|schedule)', text):
        intent["query_type"] = "upcoming"
    elif re.search(r'\b(compare|versus|vs|difference)', text):
        intent["query_type"] = "comparison"
    elif re.search(r'\b(count|how many|number of)', text):
        intent["query_type"] = "count"
    else:
        intent["query_type"] = "general"
    
    # Only extracting name if not an aggregate query
    if not intent["filters"].get("aggregate"):
        name = extract_name(user_text, conversation_history if is_follow_up else [])
        if name:
            intent["filters"]["name"] = name
    
    # Extracting date/time
    date_info = extract_date_info(user_text)
    intent["filters"].update(date_info)
    
    return intent

def extract_name(user_text: str, conversation_history: List[Dict]) -> Optional[str]:
    """Extract name with context awareness"""
    text = user_text.strip()
    
    # Excluding weekday names and common words
    EXCLUDE_WORDS = {
        "I", "The", "Show", "Give", "What", "Which", "How", "Can", "Is", "Are", "Do", "Does",
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
        "January", "February", "March", "April", "May", "June", "July", "August",
        "September", "October", "November", "December", "Today", "Tomorrow", "Yesterday",
        "Next", "Last", "This", "All", "Get", "Find", "When"
    }
    
    # Checking for pronouns referring to previous context
    if re.search(r'\b(he|she|him|her|his|their|them)\b', text, re.IGNORECASE):
        # Looking for name in recent conversation
        for entry in reversed(conversation_history[-3:]):
            if "filters" in entry and "name" in entry["filters"]:
                return entry["filters"]["name"]
    
    # Directing name patterns
    m = re.search(r"\bfor\s+([A-Z][a-z]+)\b", text)
    if m and m.group(1) not in EXCLUDE_WORDS:
        return m.group(1)
    
    m = re.search(r"\b([A-Z][a-z]+)'s\b", text)
    if m and m.group(1) not in EXCLUDE_WORDS:
        return m.group(1)
    
    # Finding capitalized words (excluding common words and weekdays)
    tokens = re.findall(r"\b[A-Z][a-z]{1,20}\b", text)
    tokens = [t for t in tokens if t not in EXCLUDE_WORDS]
    
    return tokens[0] if tokens else None

def extract_date_info(user_text: str) -> Dict[str, Any]:
    """Extract date, weekday, and time range information"""
    text = user_text.lower()
    info = {}
    
    # Explicit ISO date
    m = re.search(r"(\d{4}-\d{2}-\d{2})", user_text)
    if m:
        info["date_iso"] = m.group(1)
        return info
    
    # dd/mm/yyyy format
    m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", user_text)
    if m:
        try:
            parsed = pd.to_datetime(m.group(1), dayfirst=True, errors='coerce')
            if not pd.isna(parsed):
                info["date_iso"] = parsed.strftime("%Y-%m-%d")
                return info
        except Exception:
            pass
    
    # Relative dates
    if "today" in text:
        info["date_iso"] = dt.date.today().isoformat()
    elif "tomorrow" in text:
        info["date_iso"] = (dt.date.today() + dt.timedelta(days=1)).isoformat()
    elif "yesterday" in text:
        info["date_iso"] = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    elif m := re.search(r"next\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)", text):
        target = m.group(1).capitalize()
        today = dt.date.today()
        days_ahead = (list(WEEKDAY_MAP.values()).index(target) - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        info["date_iso"] = (today + dt.timedelta(days=days_ahead)).isoformat()
    elif m := re.search(r"last\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)", text):
        target = m.group(1).capitalize()
        today = dt.date.today()
        days_back = (today.weekday() - list(WEEKDAY_MAP.values()).index(target)) % 7
        if days_back == 0:
            days_back = 7
        info["date_iso"] = (today - dt.timedelta(days=days_back)).isoformat()
    
    # Weekday without specific date
    if "date_iso" not in info:
        for w in WEEKDAY_MAP:
            if re.search(rf"\b{w}\b", text):
                info["weekday"] = WEEKDAY_MAP[w]
                break
    
    # Date range
    if re.search(r'\b(this week|week)\b', text):
        info["date_range"] = "this_week"
    elif re.search(r'\b(next week)\b', text):
        info["date_range"] = "next_week"
    elif re.search(r'\b(this month|month)\b', text):
        info["date_range"] = "this_month"
    
    return info

def detect_high_load(user_text: str) -> Optional[float]:
    """Detect load threshold from query"""
    t = user_text.lower()
    m = re.search(r"over\s+(\d{1,3})\s*%?", t)
    if m:
        return float(m.group(1))
    if re.search(r'\bhigh load\b|\bvery busy\b', t):
        return 80.0
    if "busy" in t:
        return 70.0
    return None

# Enhanced SQL queries 
def build_query(filters: Dict[str, Any], limit: int = DEFAULT_LIMIT) -> Tuple[str, Dict]:
    """Build SQL query from filters"""
    sql = f"SELECT TOP({limit}) * FROM {TABLE_NAME} WHERE 1=1"
    params = {}
    
    if "name" in filters:
        sql += " AND first_name = :first_name"
        params["first_name"] = filters["name"]
    
    if "date_iso" in filters:
        sql += " AND CAST([date] AS DATE) = :date_iso"
        params["date_iso"] = filters["date_iso"]
    elif "weekday" in filters:
        sql += " AND weekday = :weekday"
        params["weekday"] = filters["weekday"]
    elif "date_range" in filters:
        today = dt.date.today()
        if filters["date_range"] == "this_week":
            start = today - dt.timedelta(days=today.weekday())
            end = start + dt.timedelta(days=6)
            sql += " AND CAST([date] AS DATE) BETWEEN :start_date AND :end_date"
            params.update({"start_date": start.isoformat(), "end_date": end.isoformat()})
        elif filters["date_range"] == "next_week":
            start = today + dt.timedelta(days=(7 - today.weekday()))
            end = start + dt.timedelta(days=6)
            sql += " AND CAST([date] AS DATE) BETWEEN :start_date AND :end_date"
            params.update({"start_date": start.isoformat(), "end_date": end.isoformat()})
        elif filters["date_range"] == "this_month":
            start = today.replace(day=1)
            next_month = (start + dt.timedelta(days=32)).replace(day=1)
            end = next_month - dt.timedelta(days=1)
            sql += " AND CAST([date] AS DATE) BETWEEN :start_date AND :end_date"
            params.update({"start_date": start.isoformat(), "end_date": end.isoformat()})
    
    if "load_threshold" in filters:
        sql += " AND TRY_CAST(load_percentage AS FLOAT) >= :threshold"
        params["threshold"] = filters["load_threshold"]
    
    sql += " ORDER BY CAST([date] AS DATE) ASC, start_time ASC"
    return sql, params

def execute_query(engine, filters: Dict[str, Any], limit: int = 1000) -> pd.DataFrame:
    """Execute query with given filters"""
    sql, params = build_query(filters, limit)
    print(f"\n[DEBUG] SQL: {sql}")
    print(f"[DEBUG] Params: {params}")
    with engine.connect() as conn:
        if params:
            df = pd.read_sql_query(text(sql), conn, params=params)
        else:
            df = pd.read_sql_query(text(sql), conn)
    print(f"[DEBUG] Returned {len(df)} rows")
    return df

# Context formatting 
def dataframe_to_context(df: pd.DataFrame, max_rows: int = MAX_CONTEXT_ROWS) -> str:
    """Format dataframe for LLM context"""
    if df is None or df.empty:
        return "No meeting data found."
    
    df = df.head(max_rows)
    lines = []
    for _, r in df.iterrows():
        date = r.get("date", "")
        subj = r.get("meeting_subject", "") or r.get("subject", "")
        start = r.get("start_time", "")
        end = r.get("end_time", "")
        name = r.get("first_name", "") or r.get("user_email", "")
        load = r.get("load_percentage", "")
        summary = r.get("summary_sentence", "")
        lines.append(f"• {name} | {date} {start}-{end} | {subj} | Load: {load}% | {summary}")
    
    return "\n".join(lines)

def format_conversation_history(history: List[Dict], max_entries: int = 5) -> str:
    """Format recent conversation for context"""
    if not history:
        return ""
    
    recent = history[-max_entries:]
    lines = ["Previous conversation:"]
    for entry in recent:
        lines.append(f"User: {entry['user']}")
        lines.append(f"Assistant: {entry['assistant']}")
    return "\n".join(lines)

# LLM integration 
def ask_llama(
    user_question: str,
    meeting_context: str,
    conversation_history: List[Dict],
    intent: Dict
) -> str:
    """
    Enhanced LLM call with conversation memory and intent awareness
    """
    history_context = format_conversation_history(conversation_history, max_entries=3)
    
    # Building a  system prompt based on intent
    system_prompt = """You are Slipstream's Meeting Assistant. You help users understand and manage their meeting schedules.

Key behaviors:
- Answer naturally and conversationally
- Reference previous conversation when relevant
- Be specific with dates, times, and names
- If data is missing, acknowledge it clearly
- Don't apologize for being an AI
- Keep responses concise but complete"""

    if intent.get("query_type") == "load_analysis":
        system_prompt += "\n- Focus on workload and busy periods"
    elif intent.get("query_type") == "availability":
        system_prompt += "\n- Identify free time slots and gaps"
    elif intent.get("query_type") == "comparison":
        system_prompt += "\n- Compare schedules or patterns"
    
    # Building the user prompt
    user_prompt = f"""Context from database:
{meeting_context}

{history_context}

Current question: {user_question}

Provide a helpful, natural response based on the context above."""

    try:
        response = ollama.chat(
            model=LLAMA_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        return response.get("message", {}).get("content", "").strip()
    except Exception as e:
        return f"Error calling Ollama: {e}"

#Main handler with memory
class ConversationalAssistant:
    def __init__(self, engine):
        self.engine = engine
        self.conversation_history: List[Dict] = []
        self.last_query_data: Optional[pd.DataFrame] = None
        self.last_intent: Optional[Dict] = None
    
    def process_query(self, user_text: str) -> str:
        """Process user query with full context and memory"""
        
        # Analyzes intent
        intent = extract_query_intent(user_text, self.conversation_history)
        print(f"[DEBUG] Intent: {intent}")
        
        # Handles chitchat
        if intent["intent"] == "chitchat":
            response = self.handle_chitchat(user_text)
            self.add_to_history(user_text, response, intent)
            return response
        
        # Executing database query if needed
        meeting_context = ""
        if intent["needs_context"]:
            filters = intent.get("filters", {})
            
            # If no filters and it's a follow-up, use previous filters
            if not filters and intent.get("is_follow_up") and self.last_intent:
                filters = self.last_intent.get("filters", {})
            
            print(f"[DEBUG] Final filters: {filters}")
            
            if filters:
                df = execute_query(self.engine, filters)
                self.last_query_data = df
                meeting_context = dataframe_to_context(df)
            elif self.last_query_data is not None:
                # Use cached data for follow-ups
                meeting_context = dataframe_to_context(self.last_query_data)
            else:
                # No filters and no cached data - get recent meetings
                print("[DEBUG] No filters, getting recent meetings")
                sql = f"SELECT TOP(50) * FROM {TABLE_NAME} ORDER BY CAST([date] AS DATE) DESC"
                with self.engine.connect() as conn:
                    df = pd.read_sql_query(text(sql), conn)
                print(f"[DEBUG] Recent meetings: {len(df)} rows")
                meeting_context = dataframe_to_context(df)
        
        print(f"[DEBUG] Meeting context length: {len(meeting_context)}")
        
        # Getting LLM response
        response = ask_llama(user_text, meeting_context, self.conversation_history, intent)
        
        # Saves to history
        self.add_to_history(user_text, response, intent)
        self.last_intent = intent
        
        return response
    
    def handle_chitchat(self, user_text: str) -> str:
        """Handle conversational responses"""
        text = user_text.lower()
        
        if re.search(r'^(hi|hello|hey)', text):
            return "Hello! I'm your meeting assistant. I can help you find information about schedules, meetings, and availability. What would you like to know?"
        elif re.search(r'^(thanks|thank you)', text):
            return "You're welcome! Let me know if you need anything else."
        elif re.search(r'^(bye|goodbye)', text):
            return "Goodbye! Feel free to come back anytime you need help with meetings."
        elif re.search(r'^(how are you|what\'s up)', text):
            return "I'm doing great! Ready to help you with any meeting-related questions."
        else:
            return "I'm here to help with meeting information. What would you like to know?"
    
    def add_to_history(self, user_msg: str, assistant_msg: str, intent: Dict):
        """Add exchange to conversation history"""
        self.conversation_history.append({
            "user": user_msg,
            "assistant": assistant_msg,
            "intent": intent,
            "filters": intent.get("filters", {}),
            "timestamp": dt.datetime.now().isoformat()
        })
        
        # Keep only recent history
        if len(self.conversation_history) > CONVERSATION_HISTORY_LIMIT:
            self.conversation_history = self.conversation_history[-CONVERSATION_HISTORY_LIMIT:]
    
    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []
        self.last_query_data = None
        self.last_intent = None
        return "Conversation history cleared."

# Interactive CLI 
def chat_loop():
    engine = build_sql_engine()
    assistant = ConversationalAssistant(engine)
    
    print("=" * 60)
    print("Slipstream Data Virtual Assistant")
    print("=" * 60)
    print("Connected to SQL Server:", SQL_DATABASE)
    print("\nI can help you with:")
    print("  • Finding meetings for specific people or dates")
    print("  • Analyzing workload and busy periods")
    print("  • Checking availability")
    print("  • Comparing schedules")
    print("\nCommands: 'exit' to quit, 'clear' to reset conversation")
    print("=" * 60)
    
    while True:
        try:
            q = input("\n🗣️  You: ").strip()
            
            if q.lower() in ("exit", "quit", "q"):
                print("\n👋 Goodbye!")
                break
            
            if q.lower() == "clear":
                assistant.clear_history()
                print("\n✅ Conversation history cleared")
                continue
            
            if not q:
                continue
            
            answer = assistant.process_query(q)
            print(f"\n🤖 Assistant: {answer}")
            print("-" * 60)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            print("Please try again.")

if __name__ == "__main__":
    chat_loop()