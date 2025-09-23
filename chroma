# rag_faq_bot.py

# Install before running:
# pip install chromadb sentence-transformers openai

import chromadb
from sentence_transformers import SentenceTransformer
from openai import OpenAI

# -----------------------
# Step 1: FAQ dataset
# -----------------------
faqs = [
    "How can I reset my online banking password?",
    "How do I check my account balance?",
    "What should I do if my debit card is lost?",
    "How do I activate international transactions on my credit card?",
    "How can I open a new savings account?",
    "What is the minimum balance required?",
    "How do I update my registered mobile number?",
    "How can I apply for a home loan?",
    "What is the process for closing my bank account?",
    "How do I check my loan EMI schedule?",
    "How can I download my account statement?",
    "What is the daily withdrawal limit from an ATM?",
    "How do I enable UPI payments?",
    "Can I increase my credit card limit?",
    "What is the process to block a stolen credit card?",
    "How can I register for mobile banking?",
    "How do I apply for a personal loan?",
    "What is the penalty for not maintaining minimum balance?",
    "How can I dispute a wrong transaction?",
    "What are the bank's working hours?"
]

# -----------------------
# Step 2: Setup Vector DB
# -----------------------
chroma_client = chromadb.Client()
collection = chroma_client.create_collection("banking_faqs")

# Load embeddings model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Generate embeddings for all FAQs
embeddings = model.encode(faqs)

# Add FAQs into ChromaDB
collection.add(
    documents=faqs,
    embeddings=embeddings.tolist(),
    ids=[str(i) for i in range(len(faqs))]
)

# -----------------------
# Step 3: Retrieval + RAG
# -----------------------

def rag_answer(user_query: str):
    # Encode user query
    query_embedding = model.encode([user_query])

    # Search top 3 relevant FAQs
    results = collection.query(
        query_embeddings=query_embedding.tolist(),
        n_results=3
    )

    # Extract retrieved FAQs
    retrieved_faqs = results['documents'][0]

    # Build context
    context = "\n".join(retrieved_faqs)

    # Build prompt for LLM
    prompt = f"""
    You are a helpful banking assistant.
    Here are some relevant FAQs from the knowledge base:
    {context}

    User question: {user_query}
    Answer conversationally using the FAQs above.
    """

    # Call OpenAI LLM
    client = OpenAI()   # Make sure OPENAI_API_KEY is set in your environment
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content


# -----------------------
# Step 4: Run example
# -----------------------
if __name__ == "__main__":
    user_query = "I forgot my online banking password. What should I do?"
    answer = rag_answer(user_query)
    print("User Query:", user_query)
    print("AI Answer:", answer)
 
