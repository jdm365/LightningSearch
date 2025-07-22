#include <stdint.h>

typedef struct IndexManager IndexManager;

IndexManager* create_index();
void destroy_index(IndexManager* idx_ptr);
void index_file(
		IndexManager* idx_ptr,
		uint8_t* _filename,
		uint8_t** query_cols,
		uint32_t num_query_cols
		);
