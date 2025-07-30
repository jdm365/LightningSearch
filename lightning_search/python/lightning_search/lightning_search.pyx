# cython: language_level=3

cimport cython

from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from cpython.unicode cimport PyUnicode_DecodeUTF8

cimport numpy as np
import numpy as np
import json
import os
from random import randint
from typing import List
np.import_array()

from time import perf_counter


cdef extern from "ffi.h":
    ctypedef struct IndexManager 
    IndexManager* create_index()
    void destroy_index(IndexManager* idx_ptr)
    void index_file(
            IndexManager* idx_ptr,
            uint8_t* _filename,
            uint8_t** query_cols,
            uint32_t num_query_cols,
            ) nogil

    void c_query(
		IndexManager* idx_ptr,
		uint32_t* search_col_idxs,
		uint8_t** queries,
		float* boost_factors,
		uint32_t num_query_cols,
		uint32_t k,

		uint32_t* num_matched_records,
		uint8_t** result_json_str_buf,
        uint64_t* result_json_str_buf_len
		)
    void c_query_no_fetch(
		IndexManager* idx_ptr,
		uint32_t* search_col_idxs,
		uint8_t** queries,
		float* boost_factors,
		uint32_t num_query_cols,
		uint32_t k,

		uint32_t* num_matched_records,
		uint32_t** result_doc_id_buf,
		float** scores_buf 
		)
    void get_num_cols(
            IndexManager* idx_ptr, 
            uint32_t* num_cols,
            uint32_t* num_search_cols
            )
    void get_columns(
            IndexManager* idx_ptr,
            uint32_t num_cols,
            uint8_t*** column_names, 
            uint64_t** column_name_lengths
            )
    uint32_t get_search_col_idxs(
            IndexManager* idx_ptr, 
            uint32_t** search_col_idxs
            )
    void load(IndexManager* idx_ptr, uint8_t* _dir)

cdef class Index:
    cdef IndexManager* idx_ptr 
    cdef list columns 
    cdef dict query_col_map
    cdef uint32_t num_cols

    def __init__(self):
        self.idx_ptr = create_index()
        self.columns = []
        self.query_col_map = {}
        self.num_cols = 0

    cpdef void load(self, str dir_name):
        dname = (dir_name + '\0').encode('utf-8')
        cdef uint8_t* c_dirname = <uint8_t*>dname
        load(self.idx_ptr, c_dirname)
        self.get_cols()

    def __del__(self):
        if self.idx_ptr:
            destroy_index(self.idx_ptr)
            self.idx_ptr = NULL

    cpdef void index_file(self, str filename, list query_cols):
        fname = (filename + '\0').encode('utf-8')

        cdef uint32_t num_query_cols = len(query_cols)
        cdef uint8_t* c_filename = <uint8_t*>fname
        cdef uint8_t** c_query_cols = <uint8_t**>malloc(
                num_query_cols * sizeof(uint8_t*)
                )

        query_cols = [(query_cols[i] + '\0').encode('utf-8') for i in range(num_query_cols)]

        cdef uint32_t i
        for i in range(num_query_cols):
            c_query_cols[i] = <uint8_t*>query_cols[i]

        with nogil:
            index_file(
                    self.idx_ptr,
                    c_filename,
                    c_query_cols,
                    num_query_cols
                    )

        free(c_query_cols)
        self.get_cols()

    def index_from_polars(self, df: pl.DataFrame, query_cols: list):
        """
        Arrow reading is a WIP. For now just dump to disk first.
        """
        hash_str = hash(randint()).hex()
        dst_path = f"/tmp/index_{hash_str}.csv"
        df.write_csv(dst_path)

        self.index_file(dst_path, query_cols)

        if os.path.exists(dst_path):
            os.remove(dst_path)


    cdef void get_cols(self):
        cdef uint32_t num_search_cols
        get_num_cols(self.idx_ptr, &self.num_cols, &num_search_cols)

        cdef uint8_t** column_names = <uint8_t**>malloc(
                self.num_cols * sizeof(uint8_t*)
                )
        cdef uint64_t* column_name_lengths = <uint64_t*>malloc(
                self.num_cols * sizeof(uint64_t)
                )
        get_columns(
                self.idx_ptr,
                self.num_cols,
                &column_names,
                &column_name_lengths
                )

        cdef uint32_t* search_col_idxs
        get_search_col_idxs(self.idx_ptr, &search_col_idxs)

        cdef uint64_t sc_idx = 0
        self.columns = []
        for i in range(self.num_cols):
            col_name = PyUnicode_DecodeUTF8(
                <char*>column_names[i],
                column_name_lengths[i],
                NULL
            ).upper()
            self.columns.append(col_name)

            if i == search_col_idxs[sc_idx]:
                self.query_col_map[col_name] = sc_idx
                sc_idx += 1

        free(column_names)
        free(column_name_lengths)

    cpdef list query(
            self, 
            dict query_map,
            dict boost_factors,
            uint32_t k,
            ):

        cdef uint32_t num_query_cols = len(query_map)
        cdef uint32_t* search_col_idxs = <uint32_t*>malloc(
                num_query_cols * sizeof(uint32_t)
                )
        cdef uint8_t** queries = <uint8_t**>malloc(
                num_query_cols * sizeof(uint8_t*)
                )
        cdef float* boost_factors_arr = <float*>malloc(
                num_query_cols * sizeof(float)
                )

        cdef uint8_t* result_json_str_buf = NULL
        cdef uint64_t result_json_str_buf_len = 0
        cdef uint32_t num_matched_records = 0

        for i, (query_col, query_value) in enumerate(query_map.items()):
            qc_upper = query_col.upper()

            if qc_upper not in self.query_col_map:
                print(self.query_col_map)
                raise Warning(f"Query column '{qc_upper}' not indexed.")

            idx = self.query_col_map[qc_upper]

            search_col_idxs[i] = idx
            q_copy = (query_value + '\0').encode('utf-8')
            queries[i] = <uint8_t*>q_copy
            boost_factors_arr[i] = boost_factors.get(qc_upper, 1.0)

        c_query(
            self.idx_ptr,
            search_col_idxs,
            queries,
            boost_factors_arr,
            num_query_cols,
            k,

            &num_matched_records,
            &result_json_str_buf,
            &result_json_str_buf_len,
        )

        if num_matched_records == 0:
            free(search_col_idxs)
            free(queries)
            free(boost_factors_arr)
            return []

        if result_json_str_buf is NULL or result_json_str_buf_len == 0:
            free(search_col_idxs)
            free(queries)
            free(boost_factors_arr)
            raise ValueError("Results are empty or malformed.")

        cdef str result_json_str = PyUnicode_DecodeUTF8(
            <char*>result_json_str_buf,
            result_json_str_buf_len,
            NULL
        )

        results = json.loads(result_json_str)["results"]

        ## TODO: Make members. Avoid repeated allocations.
        free(search_col_idxs)
        free(queries)
        free(boost_factors_arr)

        return results 

    cpdef tuple query_ids(
            self, 
            dict query_map,
            dict boost_factors,
            uint32_t k,
            ):

        cdef uint32_t num_query_cols = len(query_map)
        cdef uint32_t* search_col_idxs = <uint32_t*>malloc(
                num_query_cols * sizeof(uint32_t)
                )
        cdef uint8_t** queries = <uint8_t**>malloc(
                num_query_cols * sizeof(uint8_t*)
                )
        cdef float* boost_factors_arr = <float*>malloc(
                num_query_cols * sizeof(float)
                )

        cdef np.ndarray doc_ids_buf = np.zeros(k, dtype=np.uint32)
        cdef np.ndarray scores_buf  = np.zeros(k, dtype=np.float32)

        cdef uint32_t* doc_ids_ptr = <uint32_t*>doc_ids_buf.data
        cdef float* scores_ptr     = <float*>scores_buf.data

        cdef uint32_t num_matched_records = 0

        for i, (query_col, query_value) in enumerate(query_map.items()):
            qc_upper = query_col.upper()

            if qc_upper not in self.query_col_map:
                print(self.query_col_map)
                raise Warning(f"Query column '{qc_upper}' not indexed.")

            idx = self.query_col_map[qc_upper]

            search_col_idxs[i] = idx
            q_copy = (query_value + '\0').encode('utf-8')
            queries[i] = <uint8_t*>q_copy
            boost_factors_arr[i] = boost_factors.get(qc_upper, 1.0)

        c_query_no_fetch(
            self.idx_ptr,
            search_col_idxs,
            queries,
            boost_factors_arr,
            num_query_cols,
            k,

            &num_matched_records,
            &doc_ids_ptr,
            &scores_ptr,
        )

        if num_matched_records == 0:
            free(search_col_idxs)
            free(queries)
            free(boost_factors_arr)
            return np.ndarray([]), np.ndarray([])

        if num_matched_records != k:
            doc_ids_buf.ndarray.resize(num_matched_records)
            scores_buf.ndarray.resize(num_matched_records)

        ## TODO: Make members. Avoid repeated allocations.
        free(search_col_idxs)
        free(queries)
        free(boost_factors_arr)

        return doc_ids_buf, scores_buf
