# Import required C types
from libc.stdint cimport uint32_t
from libc.stddef cimport size_t
from libc.string cimport const_char
from cpython.bytes cimport PyBytes_AsString, PyBytes_Size
from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF

# Declare the C functions from radix.h
cdef extern from "radix.h":
    ctypedef void* RadixTrieHandle
    
    RadixTrieHandle radix_trie_create()
    void radix_trie_destroy(RadixTrieHandle handle)
    bint radix_trie_insert(RadixTrieHandle handle, const char* key, size_t key_len, void* value)
    bint radix_trie_find(RadixTrieHandle handle, const char* key, size_t key_len, void** value)
    size_t radix_trie_get_memory_usage(RadixTrieHandle handle)

# Python wrapper class
cdef class RadixTrie:
    cdef RadixTrieHandle _handle

    def __cinit__(self):
        self._handle = radix_trie_create()
        if self._handle is NULL:
            raise MemoryError("Failed to create RadixTrie")

    def __dealloc__(self):
        if self._handle is not NULL:
            radix_trie_destroy(self._handle)

    def insert(self, key, value):
        """Insert a key-value pair into the trie.
        
        Args:
            key: Bytes or string key
            value: Integer value to associate with the key
            
        Returns:
            bool: True if insertion was successful, False otherwise
        """
        if isinstance(key, str):
            key = key.encode('utf-8')
        elif not isinstance(key, bytes):
            raise TypeError("Key must be string or bytes")

        cdef PyObject* py_obj = <PyObject*>value
        Py_INCREF(value)
            
        return radix_trie_insert(
            self._handle,
            PyBytes_AsString(key),
            PyBytes_Size(key),
            py_obj,
        )

    def find(self, key):
        """Find a value by key in the trie.
        
        Args:
            key: Bytes or string key
            
        Returns:
            int: The value associated with the key
            
        Raises:
            KeyError: If the key is not found
            TypeError: If key is not string or bytes
        """
        if isinstance(key, str):
            key = key.encode('utf-8')
        elif not isinstance(key, bytes):
            raise TypeError("Key must be string or bytes")
            
        cdef void* value_ptr
        if radix_trie_find(
            self._handle,
            PyBytes_AsString(key),
            PyBytes_Size(key),
            &value_ptr
        ):
            py_obj = <object>value_ptr
            return py_obj
        return None
        ## raise KeyError(key)

    @property
    def memory_usage(self):
        """Get the current memory usage of the trie in bytes."""
        return radix_trie_get_memory_usage(self._handle)
