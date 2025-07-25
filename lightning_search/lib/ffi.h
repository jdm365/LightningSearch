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
uint32_t get_num_cols(IndexManager* idx_ptr);
void load(IndexManager* idx_ptr, uint8_t* _dir);
