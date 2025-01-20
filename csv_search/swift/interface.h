#include <stdint.h>
#include <stdbool.h>

typedef struct QueryHandlerLocal QueryHandlerLocal;

QueryHandlerLocal* getQueryHandlerLocal();
void readHeader(
		QueryHandlerLocal* query_handler,
		const char* filename
		);
void scanFile(QueryHandlerLocal* query_handler);
void indexFile(QueryHandlerLocal* query_handler);
void search(
		QueryHandlerLocal* query_handler, 
		const char* query_string, 
		uint32_t* result_count,
		uint32_t* start_positions,
		uint32_t* lengths,
		char** result_buffers 
		);
void addSearchCol(
		QueryHandlerLocal* query_handler, 
		const char* col_name
		);
void getColumnNames(
		QueryHandlerLocal const* query_handler, 
		char** column_names, 
		uint32_t* num_columns 
		);
void getSearchColumns(
		QueryHandlerLocal const* query_handler, 
		uint8_t* col_mask
		);
void init_allocators();
void deinit_allocators();
