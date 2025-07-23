from lightning_search import Index 
import json
import sys
import os




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

    print(index.query(
            query_map={
                "text": "Booty"
            },
            boost_factors={
                "text": 1.0
            },
            k=5,
            ))
    sys.stdout.flush()
