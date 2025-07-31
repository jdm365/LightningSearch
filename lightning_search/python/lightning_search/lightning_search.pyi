from typing import List, Dict, Tuple, Any
import numpy as np

class Index:
    def __init__(self) -> None: ...
    def load(self, index_dir: str) -> None:
        """
        Load an index from the specified directory.

        Parameters
        ----------
        index_dir : str
            The directory where the index files are stored.

        Returns
        -------
        None
        """
        ...

    def index_file(
        self, 
        filename: str, 
        query_cols: List[str], 
    ) -> None: 
        """
        Index a file with the given query columns as searchable.

        Parameters
        ----------
        filename : str
            The path to the file to index.
        query_cols : list
            A list of column names to index for search queries.

        Returns
        -------
        None
        """ 
        ...

    def index_from_polars(self, df: object, query_cols: List[str]) -> None: 
        """
        Index a Polars DataFrame with the given query columns as searchable.
        NOTE: This currently just writes a csv to disk, indexes it, then
        deletes the csv file. This is a temporary solution until arrow
        arrays and IPC are implemented.

        Parameters
        ----------
        df : object (polars.DataFrame)
            The Polars DataFrame to index.
        query_cols : list
            A list of column names to index for search queries.

        Returns
        -------
        None
        """
        ...

    def query(
        self, 
        query_map: Dict[str, str], 
        boost_factors: Dict[str, float], 
        k: int
    ) -> List[Dict[str, Any]]: 
        """
        Query the index with the specified query map and boost factors.
        Return the matching records with their relevance scores.

        Parameters
        ----------
        query_map : dict
            A dictionary mapping column names to query strings.
        boost_factors : dict
            A dictionary mapping column names to boost factors for relevance scoring.
        k : int
            The number of search results to return.

        Returns
        -------
        List[Dict[str, Any]]
            A list of search result dicts with scores.
        """
        ...

    def query_ids(
        self, 
        query_map: Dict[str, str], 
        boost_factors: Dict[str, float], 
        k: int
    ) -> Tuple[np.ndarray, np.ndarray]: 
        """
        Query the index with the specified query map and boost factors,
        Return the idxs and scores of the matching records, but not the records themselves.

        Parameters
        ----------
        query_map : dict
            A dictionary mapping column names to query strings.
        boost_factors : dict
            A dictionary mapping column names to boost factors for relevance scoring.
        k : int

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            A tuple containing two numpy arrays: the idxs of the matching records and their scores.
        """
        ...
