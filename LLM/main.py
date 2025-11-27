from langchain_ollama.llms import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from vector_csv import retriever


model = OllamaLLM(model="llama3.2")

template = """
You are an expert in answering questions related to the team's schedule availability as well as their technical capabilities.
Do not hallucinate answers.
Keep answers clean and direct.

Here is the information about the team's availability: {schedule}

Here is the question to answer: {question}
"""

prompt = ChatPromptTemplate.from_template(template)
chain = prompt | model 

while True:
    print("\n------------------------------------")
    question = input("How can I help you today? (q to quit): ")
    print("\n")
    if question.lower().strip() == "q":
        break

    schedule = retriever.invoke(question)
    result = chain.invoke({"schedule": schedule, "question": question})
    print(result)