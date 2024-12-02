# main_pipeline.py
import time
from followupfromjessicadisneypositions import logging
from typing import List, Dict, Tuple

# Import custom modules
from data_ingestion import ingest_data
from data_preprocessing import preprocess_data
from vectorization import vectorize_data
from QueryandRetrive import query_data_langchain, query_data_openai
from RAG import generate_summary
from logging_utils import setup_logging


def monitor_time(func):
    """
    Decorator to time function execution.
    """

    def wrapper(*args, **kwargs):
        logger = kwargs.get('logger')
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        if logger:
            logger.info(f"{func.__name__} completed in {end_time - start_time:.2f} seconds.")

        return result

    return wrapper


@monitor_time
def data_ingestion_step(file_path: str, logger: logging.Logger) -> List[Dict]:
    return ingest_data(file_path, logger=logger)


@monitor_time
def data_preprocessing_step(data: List[Dict], logger: logging.Logger) -> List[Dict]:
    return preprocess_data(data, logger=logger)


@monitor_time
def vectorization_step(
        data: List[Dict],
        logger: logging.Logger
) -> Tuple[List[Dict], object]:
    return vectorize_data(data, logger=logger)


@monitor_time
def query_and_retrieve_step(
        query: str,
        langchain_store,
        openai_vectors: List[Dict],
        logger: logging.Logger,
        top_k: int = 5
) -> Tuple[List[Dict], List[Dict]]:
    langchain_results = query_data_langchain(query, langchain_store, top_k, logger)
    openai_results = query_data_openai(query, openai_vectors, top_k=top_k, logger=logger)

    return langchain_results, openai_results


@monitor_time
def rag_step(
        query: str,
        results: List[Dict],
        logger: logging.Logger
) -> str:
    return generate_summary(query, results, logger=logger)


def main():
    """
    Main pipeline execution function.
    """
    # Setup logging
    logger = setup_logging("data_pipeline.log")

    try:
        # Configuration
        file_path = "structured_unstructured_data.json"
        query = "Find customer purchases related to specific product categories"

        # Step 1: Data Ingestion
        ingested_data = data_ingestion_step(file_path, logger)

        # Step 2: Data Preprocessing
        preprocessed_data = data_preprocessing_step(ingested_data, logger)

        # Step 3: Vectorization
        openai_vectorized, langchain_store = vectorization_step(preprocessed_data, logger)

        # Step 4: Query and Retrieve
        top_k = 5
        langchain_results, openai_results = query_and_retrieve_step(
            query,
            langchain_store,
            openai_vectorized,
            logger,
            top_k
        )

        # Step 5: RAG (Retriever-Augmented Generation)
        rag_summary = rag_step(query, langchain_results, logger)

        logger.info("Data pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()