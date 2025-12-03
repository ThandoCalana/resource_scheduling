# 📘 My Slipstream SQL + LLM Chatbot Assistant

A Technical Overview & Architecture Breakdown

This project is a conversational assistant that connects directly to SQL Server, reads meeting and workload data, and then uses a Large Language Model (LLM), specifically the Llama model running locally via Ollama is to transform raw SQL query results into natural, human-like conversational answers.

The goal is to allow natural questions like:
- "What meetings does Andrew have on Wednesday?"
- "Is he free from 9 to 10 tomorrow?"
- "Who is the busiest this week?"
- "Compare Robin's meetings to Palesa."
- "What about him?" (follow-up across context)

The assistant then:
- Understands the intent behind the query
- Extracts names, dates, times, and workload details
- Pulls structured data from SQL
- Formats the data contextually
- Passes curated context to the LLM
- Returns a friendly, natural-language answer

This document explains how each subsystem works.

## 🔧 1. System Setup & Configuration

At the top of the script, the setup includes:
- OS operations & environment variables
- Regular expressions for NLP-style intent extraction
- Pandas for DataFrame handling
- SQLAlchemy for DB connectivity
- Ollama for talking to the local LLM
- dotenv for secure credentials

This loads the key config values:
- SQL_SERVER
- SQL_DATABASE
- SQL_DRIVER
- LLM_MODEL

These tell the system how to connect to SQL Server and what model to use for AI responses.

## 🛢️ 2. Connecting to SQL Server

A helper function, `build_sql_engine()`, constructs a SQLAlchemy engine.

It:
- Reads environment variables
- Builds a trusted Windows Authentication connection string
- Validates the configuration
- Returns a ready-to-query SQL engine

If any required variables are missing, the script stops with a clear error.

## 🧠 3. Understanding User Intent

Before running SQL, the system interprets what the user wants in `extract_query_intent()`.

It detects:

### ✔ Greetings / Chitchat
- Short, friendly messages are answered without running SQL.

### ✔ Follow-up Questions
- Detects context-based references like:
    - "What about him?"
    - "And on that day?"
    - "What about Tuesday?"
- The system uses conversation memory to interpret these.

### ✔ Query Types
- The assistant classifies questions into categories:
    - Availability
    - Meeting lookup
    - Workload / load percentage
    - Weekly summaries
    - Comparison between people
    - Busiest employee
    - High-load detection

### ✔ Extracted Filters
- Names
- Dates / weekdays
- Date ranges
- Time ranges
- Workload thresholds

These later translate into SQL WHERE clauses.

## 👤 4. Extracting Names

Using `extract_name()`, the system can detect:
- Personal names
- Avoid calendar words (Monday, June, etc.)
- Understand pronouns referring to previous people

If no name is found, queries still work based on other context.

## 📅 5. Extracting Dates, Weeks & Times

The function `extract_date_info()` processes natural language date descriptions:
- ISO dates: `2025-11-10`
- Local formats: `12/02/2025`
- Relative dates: tomorrow, next Friday, this week, next month

All are converted into SQL-compatible ranges, allowing smooth, human-style expressions instead of strict date formats.

## 📊 6. Detecting Workload / Busy-Level Queries

Workload parsing detects terms like:
- "over 80%"
- "high load"
- "very busy"

These trigger load-based SQL filters.

## 🧩 7. Dynamic SQL Query Builder

`build_query()` takes the detected filters and assembles real SQL:

Examples:
- If name is provided → `WHERE first_name = 'Andrew'`
- If date = specific day → `CAST(date AS DATE) = '2025-11-10'`
- If range = week → Automatically generates Monday–Sunday filters

This allows flexible, intent-aware SQL without predefined templates.

## 🟦 8. Executing SQL Queries

`execute_query()` sends the query to SQL Server and returns a DataFrame.

It also logs:
- SQL text
- Bound parameters
- Number of rows returned

Great for debugging and transparency.

## 🧾 9. Converting Rows into LLM-Friendly Context

LLMs can't read DataFrames, so the assistant formats results like:
- `• Andrew | 2025-11-10 | 09:00–10:00 | Daily Stand-Up | Load: 70%`

This structured but human-readable context is optimal for LLM reasoning.

## 💬 10. Short-Term Conversation Memory

The assistant keeps a memory buffer containing:
- Previous questions
- Extracted intents
- Last known name/date/time values

This enables multi-turn conversations such as:
- "Show his meetings for Monday."
- "Is he free after 2pm?" (No name repeated — system remembers)

The memory size is capped to avoid context bloating.

## 🧠 11. Talking to the LLM (Ollama + Llama)

`ask_llama()` sends the following to the model:
- A system prompt describing its job
- Meeting context converted into bullet form
- Conversation memory
- Current user question

The Llama model then:
- Interprets context
- Summarizes meetings
- Answers availability questions
- Generates natural, conversational explanations

It never returns SQL — only human-friendly responses.

## 🤖 12. The ConversationalAssistant Class

This class orchestrates the entire system handling:
- Intent extraction
- SQL query creation
- SQL execution
- Result formatting
- LLM answering
- Conversation memory storage

The main pipeline:
1. `process_query()` →
2. Determine if it's chitchat
3. Extract intent
4. Build SQL (if required)
5. Run SQL
6. Compile meeting context
7. Ask the LLM
8. Save to conversation memory
9. Return natural-language output

This creates a smooth conversational UX.

## 🖥️ 13. The Terminal Chat Loop

The `chat_loop()` function powers the full interactive experience.

On script launch, it:
- Connects to SQL Server
- Initializes the assistant
- Displays a welcome header
- Enters an infinite conversation loop

Commands:
- `exit` → Quit
- `clear` → Reset assistant memory

This turns your Python script into a fully functional local chatbot.

