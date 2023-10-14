#ifndef RO_H
#define RO_H
#include "db.h"

#define INT64 int64_t

typedef struct Page {   // page data structure
    UINT64 pid;
    INT* data;
} Page;

typedef struct Slot {   // buffer slot
    INT oid;
    INT64 pid;
    UINT64 pin;
    UINT64 usage;
    Page* page_ptr;
} Slot;

typedef struct File_Pointer {   // file limitation management
    FILE* fp;
    INT64 oid;
    UINT64 pin;
} File_Pointer;

// Inner functions
// buffer management
//UINT request_page(UINT64 pid, UINT oid);
//void release_page(UINT64 pid, UINT oid);

// open file management
//FILE* open_file(UINT oid);
//void close_file(UINT oid);

// read page from disk
//Page* read_page_from_file(UINT oid, UINT64 pid, UINT64 page_id_init);

void init();
void release();

// equality test for one attribute
// idx: index of the attribute for comparison, 0 <= idx < nattrs
// cond_val: the compared value
// table_name: table name
_Table* sel(const UINT idx, const INT cond_val, const char* table_name);

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name);
#endif