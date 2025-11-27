# rag_chain.py
from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from extract_to_rag import retriever  # your RAG-ready vector store retriever

# --- Initialize the LLM ---
llm = OllamaLLM(model="llama3.2")

# --- Define prompt template ---
template = """
You are an expert in answering questions related to the team's schedule availability and technical capabilities.

Here is the information about the team's availability: {schedule}

Here is the question to answer: {question}
"""

prompt = ChatPromptTemplate.from_template(template)

# --- Helper to retrieve documents (handles private method with run_manager) ---
def retrieve_docs(query: str):
    return retriever._get_relevant_documents(query, run_manager=None)

def answer_question(question: str) -> str:
    """
    Perform a RAG-style query:
    - Retrieve relevant documents from vector store
    - Generate an answer using the LLM
    """
    # --- Retrieve relevant summaries ---
    relevant_docs = retrieve_docs(question)
    if not relevant_docs:
        return "No relevant schedule information found."

    schedule_text = "\n\n".join([doc.page_content for doc in relevant_docs])

    # --- Fill prompt template ---
    prompt_text = prompt.format(schedule=schedule_text, question=question)

    # --- Invoke the LLM ---
    response = llm.invoke(prompt_text)
    return response

# --- Interactive CLI ---
if __name__ == "__main__":
    print("RAG question-answering ready. Type 'q' to quit.")
    while True:
        question = input("\nEnter your question: ").strip()
        if question.lower() == "q":
            break
        answer = answer_question(question)
        print("\nAnswer:\n", answer)
