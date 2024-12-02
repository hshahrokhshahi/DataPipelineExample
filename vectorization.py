import openai
import numpy as np

openai.api_key = 'your-api-key'


# for this part I like Langchain way better as it is
def vectorize_data_openai(data, text_field="comments", openai_model="text-embedding-ada-002"):
    """
    Vectorizes unstructured text using OpenAI's embedding API.

    Parameters:
        data (list[dict]): The dataset (list of dictionaries).
        text_field (str): The key containing the unstructured text to vectorize.
        openai_model (str): The OpenAI model for embeddings.

    Returns:
        list[dict]: List of records with embeddings added to each record.
    """
    # Add embeddings to each record
    for record in data:
        if text_field in record and record[text_field]:
            response = openai.Embedding.create(input=record[text_field], model=openai_model)
            record["embedding"] = response["data"][0]["embedding"]
    return data

# Example: Convert comments to embeddings



# using Langchain
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS

def vectorize_data_langchain(data, text_field="comments"):
    """
    Vectorizes unstructured text using LangChain and stores the embeddings in FAISS.

    Parameters:
        data (list[dict]): The dataset (list of dictionaries).
        text_field (str): The key containing the unstructured text to vectorize.

    Returns:
        FAISS: The FAISS vector store containing the embeddings.
    """
    # Initialize OpenAI embeddings
    embeddings = OpenAIEmbeddings()

    # Extract text data for vectorization
    texts = [record[text_field] for record in data if record.get(text_field)]

    # Create FAISS vector store
    vector_store = FAISS.from_texts(texts, embedding=embeddings)

    return vector_store


def vectorize_data(data, logger):
    try:
        logger.info("Starting vectorization with OpenAI.")
        # Step 1: OpenAI Vectorization
        openai_vectorized = vectorize_data_openai(data, text_field="comments")
        logger.info(f"OpenAI vectorization completed. Processed {len(openai_vectorized)} records.")

        logger.info("Starting vectorization with LangChain.")
        # Step 2: LangChain Vectorization
        langchain_store = vectorize_data_langchain(data, text_field="comments")
        logger.info("LangChain vectorization completed.")

        return openai_vectorized, langchain_store

    except Exception as e:
        logger.error(f"Vectorization step failed: {str(e)}", exc_info=True)
        raise
