def query_data_langchain(query, vector_store, top_k=5):
    """
    Queries the vector store using LangChain to find the most similar embeddings.
    for default I used 5 nearerst neighbor to 

    Parameters:
        query (str): The input query text.
        vector_store (FAISS): The FAISS vector store containing the embeddings.
        top_k (int): Number of top similar results to retrieve.

    Returns:
        list: Top-k similar records.
    """
    # Perform similarity search
    results = vector_store.similarity_search(query, k=top_k)

    return results


from sklearn.metrics.pairwise import cosine_similarity
import openai
import numpy as np

def query_data_openai(query, data, openai_model="text-embedding-ada-002", top_k=5):
    """
    Queries the dataset using OpenAI embeddings and retrieves the most similar records.

    Parameters:
        query (str): The input query text.
        data (list[dict]): The dataset with embeddings.
        openai_model (str): The OpenAI model for embeddings.
        top_k (int): Number of top similar results to retrieve.

    Returns:
        list[dict]: Top-k similar records based on cosine similarity.
    """
    # Get the embedding for the query
    response = openai.Embedding.create(input=query, model=openai_model)
    query_embedding = np.array(response["data"][0]["embedding"])

    # Calculate cosine similarity with all embeddings in the dataset
    similarities = []
    for record in data:
        if "embedding" in record:
            similarity = cosine_similarity([query_embedding], [record["embedding"]])[0][0]
            similarities.append((record, similarity))

    # Sort by similarity and return top-k results
    similarities = sorted(similarities, key=lambda x: x[1], reverse=True)
    return [item[0] for item in similarities[:top_k]]
