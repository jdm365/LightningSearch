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
NLTK_STOPWORDS = set([
"i",
"me",
"my",
"myself",
"we",
"our",
"ours",
"ourselves",
"you",
"your",
"yours",
"yourself",
"yourselves",
"he",
"him",
"his",
"himself",
"she",
"her",
"hers",
"herself",
"it",
"its",
"itself",
"they",
"them",
"their",
"theirs",
"themselves",
"what",
"which",
"who",
"whom",
"this",
"that",
"these",
"those",
"am",
"is",
"are",
"was",
"were",
"be",
"been",
"being",
"have",
"has",
"had",
"having",
"do",
"does",
"did",
"doing",
"a",
"an",
"the",
"and",
"but",
"if",
"or",
"because",
"as",
"until",
"while",
"of",
"at",
"by",
"for",
"with",
"about",
"against",
"between",
"into",
"through",
"during",
"before",
"after",
"above",
"below",
"to",
"from",
"up",
"down",
"in",
"out",
"on",
"off",
"over",
"under",
"again",
"further",
"then",
"once",
"here",
"there",
"when",
"where",
"why",
"how",
"all",
"any",
"both",
"each",
"few",
"more",
"most",
"other",
"some",
"such",
"no",
"nor",
"not",
"only",
"own",
"same",
"so",
"than",
"too",
"very",
"s",
"t",
"can",
"will",
"just",
"don",
"should",
"now",
])



if __name__ == '__main__':
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(
            CURRENT_DIR,
            "../../../data",
            "enwiki.csv",
            ## "mb_small.csv",
            )
    index_dir = os.path.join(
            CURRENT_DIR,
            "../../ls_data/96a03617fbfc74b054e835a1e001514a",
            )

    index = Index()
    index.index_file(
            filename=filename,
            query_cols=[
                "text"
                ## "title"
                ],
            )
    """
    index = Index()
    index.load(index_dir)
    """

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
            ## "text": 1.0
            "title": 1.0
            }

    N = 10_000
    M_min = 1
    M_max = 4
    for i in tqdm(range(N), desc="Preparing test queries"):
        num_words = random.randint(M_min, M_max)
        query_maps.append({
            "text": " ".join(random.sample(NLTK_WORDS, num_words))
            ## "title": " ".join(random.sample(NLTK_WORDS, num_words))
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
