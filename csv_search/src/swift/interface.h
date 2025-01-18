#include <stdint.h>

typedef struct QueryHandlerLocal QueryHandlerLocal;

QueryHandlerLocal* get_query_handler_local();
void search(
		QueryHandlerLocal* query_handler, 
		const char* query_string, 
		uint32_t* result_count,
		uint32_t* start_positions,
		uint32_t* lengths,
		char** result_buffers 
		);
void get_column_names(
		QueryHandlerLocal const* query_handler, 
		char** column_names, 
		uint32_t* num_columns 
		);
void init_allocators();
void deinit_allocators();
