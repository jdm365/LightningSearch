from lightning_search import Index 
import json
import sys
from time import perf_counter
from tqdm import tqdm
import random
import os

from nltk.corpus import words
import nltk; nltk.download('words')

NLTK_WORDS = words.words()




if __name__ == '__main__':
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(
            CURRENT_DIR,
            "../../../data",
            "enwiki_small.csv",
            )

    index = Index()
    index.index_file(
            filename=filename,
            query_cols=[
                "text"
                ],
            )

    '''
    print(json.dumps(index.query(
            query_map={
                "text": "griffith observatory"
            },
            boost_factors={
                "text": 1.0
            },
            k=5,
            ), indent=2))
    '''
    query_maps = []
    boost_factor = {
            "text": 1.0
            }

    N = 10_000
    M_min = 1
    M_max = 4
    for i in range(N):
        num_words = random.randint(M_min, M_max)
        query_maps.append({
            "text": " ".join(random.sample(NLTK_WORDS, num_words))
        })

    init = perf_counter()
    for query_map in tqdm(query_maps):
        index.query(
            query_map=query_map,
            boost_factors=boost_factor,
            k=5,
        )
    end = perf_counter()


    print(f"Time taken for {N} queries: {end - init:.2f} seconds")
    print(f"QPS: {N / (end - init):.2f} queries per second")
