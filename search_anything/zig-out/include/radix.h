#include <stddef.h>
#include <stdbool.h>

// Opaque pointer type for the trie
typedef struct RadixTrie* RadixTrieHandle;
RadixTrieHandle radix_trie_create();
void radix_trie_destroy(RadixTrieHandle handle);
bool radix_trie_insert(RadixTrieHandle handle, const char* key, size_t key_len, void* value);
bool radix_trie_find(RadixTrieHandle handle, const char* key, size_t key_len, void** value);
size_t radix_trie_get_memory_usage(RadixTrieHandle handle);
