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
