# cython: language_level=3

cimport cython

from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from cpython.unicode cimport PyUnicode_DecodeUTF8

cimport numpy as np
import numpy as np
import json
import os
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
		) nogil
    uint32_t get_num_cols(IndexManager* idx_ptr)


cdef class Index:
    cdef IndexManager* idx_ptr 
    cdef dict query_col_map
    cdef uint32_t num_cols

    def __init__(self):
        self.idx_ptr = create_index()
        self.query_col_map = {}
        self.num_cols = 0

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

        self.query_col_map = {query_cols[i]: idx for idx in range(num_query_cols)}
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

    cdef void get_num_cols(self):
        self.num_cols = get_num_cols(self.idx_ptr)

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

        for query_col, query_value in query_map.items():
            if query_col not in self.query_col_map:
                raise Warning(f"Query column '{query_col}' not indexed.")

            idx = self.query_col_map[query_col]

            search_col_idxs[idx] = idx
            q_copy = (query_value + '\0').encode('utf-8')
            queries[idx] = <uint8_t*>q_copy
            boost_factors_arr[idx] = boost_factors.get(query_col, 1.0)

        with nogil:
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
