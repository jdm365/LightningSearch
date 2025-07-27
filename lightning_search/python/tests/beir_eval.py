import os
import logging
import pathlib
import random
import polars as pl
from tqdm import tqdm

from beir import LoggingHandler, util
from beir.datasets.data_loader import GenericDataLoader
from beir.retrieval.evaluation import EvaluateRetrieval
from lightning_search import Index

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[LoggingHandler()],
)

class MyCustomSearcher:
    def __init__(self, model):
        self.model = model

    def search(self, corpus: dict, queries: dict, top_k: int, score_function: str, **kwargs) -> dict:
        """
        BEIR's EvaluateRetrieval calls this method with 5 arguments:
        corpus, queries, top_k, score_function, **kwargs
        """
        results = {}
        
        for query_id, query_text in tqdm(queries.items()):
            search_results = self.model.query(
                query_map={
                    "title": query_text,
                    "text": query_text,
                },
                boost_factors={
                    "title": 2.0,
                    "text": 1.0,
                },
                k=top_k,
            )
            
            # Convert to the format BEIR expects: {doc_id: score}
            results[query_id] = {
                res["_ID"]: float(res["SCORE"]) for res in search_results
            }
        
        return results


if __name__ == "__main__":
    ## dataset = "scifact"
    ## dataset = "dbpedia-entity"
    dataset = "msmarco"
    url = f"https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{dataset}.zip"
    out_dir = os.path.join(pathlib.Path(__file__).parent.absolute(), "datasets")
    data_path = util.download_and_unzip(url, out_dir)

    corpus_jsonl_path = os.path.join(data_path, "corpus.jsonl")
    corpus_csv_path = os.path.join(data_path, "corpus.csv")

    if not os.path.exists(corpus_csv_path):
        logging.info("Converting corpus.jsonl to corpus.csv...")
        corpus_df = pl.read_ndjson(corpus_jsonl_path)
        corpus_df = corpus_df.drop("metadata")
        corpus_df.write_csv(corpus_csv_path)
        logging.info(f"corpus.csv created at: {corpus_csv_path}")

    index = Index()
    index.index_file(
        filename=corpus_csv_path,
        query_cols=["title", "text"],
    )

    ## corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split="test")
    corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split="dev")

    my_searcher = MyCustomSearcher(model=index)
    retriever   = EvaluateRetrieval(my_searcher)

    results = retriever.retrieve(corpus, queries)

    logging.info(f"Retriever evaluation for k in: {retriever.k_values}")
    ndcg, _map, recall, precision = retriever.evaluate(qrels, results, retriever.k_values)

    '''
    top_k = 10
    query_id, ranking_scores = random.choice(list(results.items()))
    scores_sorted = sorted(ranking_scores.items(), key=lambda item: item[1], reverse=True)
    logging.info(f"Query : {queries[query_id]}\n")

    for rank in range(top_k):
        doc_id = scores_sorted[rank][0]
        doc_text = corpus.get(doc_id, {}).get("text", "Text not available in corpus")
        logging.info(f"Rank {rank + 1}: {doc_id} [Score: {scores_sorted[rank][1]:.4f}] - {doc_text}\n")
    '''
