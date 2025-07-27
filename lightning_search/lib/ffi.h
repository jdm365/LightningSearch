#include <stdint.h>

typedef struct IndexManager IndexManager;
typedef struct TermPos {
	uint32_t start_pos;
	uint32_t field_len;
} TermPos;

IndexManager* create_index();
void destroy_index(IndexManager* idx_ptr);
void index_file(
		IndexManager* idx_ptr,
		uint8_t* _filename,
		uint8_t** query_cols,
		uint32_t num_query_cols
		);
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
		);
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
		);
void get_num_cols(
		IndexManager const* idx_ptr, 
		uint32_t* num_cols,
		uint32_t* num_search_cols
		);
void get_columns(
		IndexManager const* idx_ptr,
		uint32_t num_cols,
		uint8_t*** column_names, 
		uint64_t** column_name_lengths
		);
uint32_t get_search_col_idxs(
		IndexManager const* idx_ptr, 
		uint32_t** search_col_idxs
		);
void load(IndexManager* idx_ptr, uint8_t* _dir);
