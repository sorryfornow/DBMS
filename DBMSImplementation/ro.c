#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include <string.h>
#include <assert.h>

static Conf* cf = NULL;
static Database* db = NULL;
Slot* buffer = NULL;
File_Pointer* opened_files = NULL;
UINT nvb = 0;
UINT idx_cmp_use_only = 0;

// cmp func for qsort
int compare_tuples(const void *a, const void *b) {
    Tuple* tup1 = (Tuple*)a;
    Tuple* tup2 = (Tuple*)b;
    Tuple t1 = *tup1;
    Tuple t2 = *tup2;
    return t1[idx_cmp_use_only] - t2[idx_cmp_use_only];
}

UINT request_page(UINT64 pid, UINT oid){    // clock sweep
    UINT i;
    for (i = 0; i < cf->buf_slots; i++){
        if (buffer[i].pid == pid && buffer[i].oid == oid){ // optional: buffer[i].page_ptr != NULL &&
            buffer[i].usage++;
            buffer[i].pin = 1;
            return i;
        }
    }

//    printf("request page pid %llu oid %ul not found in buffer\n", pid, oid);

    while (1){  // deadlock may exist

        if (buffer[nvb].pin == 0 && buffer[nvb].usage == 0){

//            printf("request page pid %llu oid %ul not found in buffer, replace with nvb %ul\n", pid, oid, nvb);

            UINT res_idx = nvb;
            // read page from disk to buffer[nvb]

            // release the previous existing page in the very buffer slot
            if (buffer[i].page_ptr == NULL) log_release_page(pid);  // log release page

            if (buffer[i].page_ptr != NULL && buffer[i].page_ptr->data != NULL)
                free(buffer[i].page_ptr->data);
            if (buffer[i].page_ptr != NULL) free(buffer[i].page_ptr);

            // need to read new page from disk
            // page_ptr will be assigned outside this function after successful read
            buffer[nvb].page_ptr = NULL;
            buffer[nvb].oid = oid;
            buffer[nvb].pid = pid;
            buffer[nvb].pin = 1;
            buffer[nvb].usage = 1;
            nvb = (nvb + 1) % cf->buf_slots;
            return res_idx;
        }
        else{
            if (buffer[nvb].usage > 0) buffer[nvb].usage--;
            nvb = (nvb + 1) % cf->buf_slots;
        }
    }
}

void release_page(UINT64 pid, UINT oid){    // pin set to 0
    UINT i;
    for (i = 0; i < cf->buf_slots; i++){
        if (buffer[i].pid == pid && buffer[i].oid == oid){
            buffer[i].pin = 0;
            return;
        }
    }
}

// file ptr management

void unpin_file(UINT oid){    // file open management
    UINT i;
    for (i = 0; i < cf->file_limit; ++i){
        if (opened_files[i].oid == oid){
            opened_files[i].pin = 0;
            return;
        }
    }
}

void close_file(UINT oid){    // file open management
    UINT i;
    for (i = 0; i < cf->file_limit; ++i){
        if (opened_files[i].oid == oid){

            if (opened_files[i].fp != NULL) fclose(opened_files[i].fp);
            opened_files[i].fp = NULL;
            opened_files[i].oid = -1;
            opened_files[i].pin = 0;
            log_close_file(oid);    // log close file
            return;
        }
    }
}

FILE* open_file(UINT oid){    // file open management
    // return file pointer if already opened
    UINT i;
    for (i = 0; i < cf->file_limit; ++i){
        if (opened_files[i].oid == oid && opened_files[i].fp != NULL){
            opened_files[i].pin = 1;
            return opened_files[i].fp;
        }
    }

    // find an empty file_ptr slot & open file, return file pointer
    for (i = 0; i < cf->file_limit; ++i){
        if (opened_files[i].oid == -1){
            char table_path[200];
            sprintf(table_path,"%s/%u",db->path,oid);

            opened_files[i].fp = fopen(table_path,"r");
            assert(opened_files[i].fp != NULL);
            opened_files[i].oid = oid;
            opened_files[i].pin = 1;

            log_open_file(oid); // log open file
            return opened_files[i].fp;
        }
    }

    // if all slots are used, check if any file is not pinned
    for (i = 0; i < cf->file_limit; ++i){

        if (opened_files[i].pin == 0){

            // close the old file
            if (opened_files[i].fp != NULL){
                fclose(opened_files[i].fp);
                log_close_file(oid);    // log close file
            }
            opened_files[i].fp = NULL;
            opened_files[i].oid = -1;
            opened_files[i].pin = 0;

            // reassign file pointer
            char table_path[200];
            sprintf(table_path,"%s/%u",db->path,oid);

            opened_files[i].fp = fopen(table_path,"r");
            assert(opened_files[i].fp != NULL);
            opened_files[i].oid = oid;
            opened_files[i].pin = 1;

            log_open_file(oid); // log open file
            return opened_files[i].fp;
        }

    }

    // reached the limitation, return NULL
    return NULL;
}


Page* read_page_from_file(UINT oid, UINT64 pid, UINT64 page_id_init){
    Page* page;
    FILE* table_fp = open_file(oid);
    assert(table_fp != NULL);

    page = malloc(sizeof(Page));
    page->data = malloc(cf->page_size-sizeof(UINT64));

    fseek(table_fp, (pid-page_id_init) * cf->page_size, SEEK_SET);
    fread(&page->pid, sizeof(UINT64), 1, table_fp);
    fread(page->data, sizeof(INT), (cf->page_size-sizeof(UINT64))/sizeof(INT), table_fp);
    unpin_file(oid);

    log_read_page(pid); // log read page
    return page;
}



void init(){
    // do some initialization here.
    UINT i;
    // example to get the Conf pointer
    cf = get_conf();

    // example to get the Database pointer
    db = get_db();

    if (db == NULL || cf == NULL){
        printf("Database is not initialized.\n");
        exit(-1);
    }

    // initialize buffer here.
    buffer = malloc(sizeof(Slot) * cf->buf_slots);
    for (i = 0; i < cf->buf_slots; i++){
        buffer[i].oid = -1;
        buffer[i].pid = -1;
        buffer[i].pin = 0;
        buffer[i].usage = 0;
        buffer[i].page_ptr = NULL;
    }

    // open file management
    opened_files = malloc(sizeof(File_Pointer) * cf->file_limit);
    for (i = 0; i < cf->file_limit; ++i) {
        opened_files[i].fp = NULL;
        opened_files[i].oid = -1;
        opened_files[i].pin = 0;
    }

    printf("init() is invoked.\n");
}


void release(){
    // optional
    // do some end tasks here.
    // free space to avoid memory leak

    UINT i;

    // release buffer & pages inside buffer
    if (buffer != NULL){
        for (i = 0; i < cf->buf_slots; ++i) {
            if (buffer[i].page_ptr != NULL) {
                if (buffer[i].page_ptr != NULL && buffer[i].page_ptr->data != NULL)
                    free(buffer[i].page_ptr->data);
                if (buffer[i].page_ptr != NULL) free(buffer[i].page_ptr);

                // reinitialize buffer slot (optional)
                buffer[i].page_ptr = NULL;
                buffer[i].pid = -1;
                buffer[i].pin = 0;
                buffer[i].usage = 0;
                buffer[i].oid = -1;

                log_release_page(buffer[i].pid);    // log release page
            }
        }
        free(buffer);
    }


    // release file_open management
    if (opened_files != NULL){
        for (i = 0; i < cf->file_limit; ++i) {
            if (opened_files[i].fp != NULL) {
                fclose(opened_files[i].fp);
                log_close_file(opened_files[i].oid);  // log close file

                // reinitialize file_open slot (optional)
                opened_files[i].fp = NULL;
                opened_files[i].oid = -1;
            }
        }
        free(opened_files);
    }

    printf("release() is invoked.\n");
}

int cmp_func (const void * a, const void * b) {
   return ( *(INT*)a - *(INT*)b );
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name){
    
    printf("sel() is invoked.\n");

    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    // testing
    // the following code constructs a synthetic _Table with 10 tuples and each tuple contains 4 attributes
    // examine log.txt to see the example outputs
    // replace all code with your implementation

    Table t;    // the chosen table
    UINT ntuples = 0;   // number of tuples in the chosen table
    UINT nattrs = 0;    // number of attributes in the chosen table
    UINT npages = 0;    // number of pages in the chosen table
    UINT ntuples_per_page = 0;  // number of tuples per page
    UINT ntuples_last_page = 0; // number of tuples in the last page

    // loop variables
    UINT i = 0;
    UINT j = 0;
    UINT k = 0;

    FILE* table_fp = NULL;  // file pointer to the chosen table
    UINT64 page_id_init = 0;    // first page id of the table
    INT table_found = 0;    // flag to check if table is found

    UINT ntuples_res = 0;   // number of tuples in result table
    Tuple* tuples_cur_table = NULL;  // selected tuples in current table
    _Table* result = NULL;  // result table

    // find the table
    for (i = 0; i < db->ntables; ++i){
        if (strcmp(db->tables[i].name, table_name) == 0){
            t = db->tables[i];
            ++table_found;
            break;
        }
    }
    assert(table_found != 0);

    // get the number of tuples and attributes
    ntuples_per_page = (cf->page_size-sizeof(UINT64))/sizeof(INT)/t.nattrs;
    ntuples_last_page = t.ntuples % ntuples_per_page;
    if (ntuples_last_page == 0) ntuples_last_page = ntuples_per_page;
    nattrs = t.nattrs;
    assert(idx < nattrs);   // check if idx is valid
    ntuples = t.ntuples;
    npages = (t.ntuples + ntuples_per_page - 1) / ntuples_per_page;

//    // check if all pages of the table are already in the buffer
//    // if so, there will be no need to load them by calling open_file() function
//    UINT count_page_in_buffer = 0;
//    for (i = 0; i < cf->buf_slots; ++i){
//        if (buffer[i].oid == t.oid){
//            ++count_page_in_buffer;
//        }
//    }
//    if (count_page_in_buffer == npages){
//        // all pages are in buffer
//        // no need to load them from file
//        // still need to get the first page_id (maybe not necessary)
//    }
//    else{
//        // load pages from file
//        // open file
//        table_fp = open_file(t.oid);
//        assert(table_fp != NULL);
//        fseek(table_fp, 0, SEEK_SET);
//        fread(&page_id_init,sizeof(UINT64),1,table_fp);
//    }

    // get the first page_id
    table_fp = open_file(t.oid);
    assert(table_fp != NULL);
    fseek(table_fp, 0, SEEK_SET);
    fread(&page_id_init,sizeof(UINT64),1,table_fp);
    unpin_file(t.oid);

    // allocate space for tuples in result table
    tuples_cur_table = malloc(ntuples*sizeof(Tuple));


    // loop through all pages
    for (i = 0; i < npages; ++i){
        // try reading page from buffer
        UINT ntuples_of_cur_page = 0;
        UINT cur_page_slot = request_page(page_id_init+i,t.oid);
        Page* cur_page = buffer[cur_page_slot].page_ptr;

        // if the page unloaded then read it from file
        if (cur_page==NULL){
            cur_page = read_page_from_file(t.oid, page_id_init+i, page_id_init);
            buffer[cur_page_slot].page_ptr = cur_page;
        }

        if (i == npages-1){
            ntuples_of_cur_page = ntuples_last_page;
        } else {
            ntuples_of_cur_page = ntuples_per_page;
        }

        // choose the tuples that satisfy the condition in current page
        for (j = 0; j < ntuples_of_cur_page*nattrs; j+=nattrs){
            if (cur_page->data[j+idx] == cond_val){
                // copy the tuple to result table
                Tuple tup = malloc(sizeof(INT)*nattrs);
                for (k = 0; k < nattrs; ++k){
                    tup[k] = cur_page->data[j+k];
                }
                tuples_cur_table[ntuples_res] = tup;
                ++ntuples_res;
            }
        }

        // release the page
        release_page( page_id_init+i,t.oid);
    }

    // close the file
    unpin_file(t.oid);

    result = malloc(sizeof(_Table)+ntuples_res*sizeof(Tuple));
    result->nattrs = nattrs;
    result->ntuples = ntuples_res;

//    puts("show result table:\n");

    // copy the tuples to result table
    for (i = 0; i < ntuples_res; ++i){
        result->tuples[i] = tuples_cur_table[i];


        //print
//        for (j = 0; j < nattrs; ++j){
//            printf("%d ", tuples_cur_table[i][j]);
//        }
//        printf("\n");


    }

//    puts("select() is finished.\n");

    if (tuples_cur_table != NULL) free(tuples_cur_table); // release the shell space
    // do not release the tuples in tuples_cur_table because they are using to result table


    return result;
}

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name){

    printf("join() is invoked.\n");
    // write your code to join two tables
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    Table r,s;    // the chosen table
    INT table_found_r = 0;    // flag to check if R table is found
    INT table_found_s = 0;    // flag to check if S table is found

    UINT ntuples_r = 0;
    UINT ntuples_s = 0;

    UINT nattrs_r = 0;
    UINT nattrs_s = 0;

    UINT npages_r = 0;
    UINT npages_s = 0;

    UINT ntuples_per_page_r = 0;
    UINT ntuples_per_page_s = 0;
    UINT ntuples_last_page_r = 0;
    UINT ntuples_last_page_s = 0;

    // loop variables
    UINT i = 0;
    UINT j = 0;
    UINT k = 0;
    UINT l = 0;
    UINT m = 0;

    FILE* table_fp_r = NULL;  // file pointer to the R table
    FILE* table_fp_s = NULL;  // file pointer to the S table

    UINT64 page_id_init_r = 0;    // first page id of the R table
    UINT64 page_id_init_s = 0;    // first page id of the S table

    // result table
    UINT nattrs_res = 0;    // number of attributes in result table
    UINT ntuples_res = 0;   // number of tuples in result table
    _Table* result = NULL;  // result table
    Tuple* tuples_cur_table = NULL;  // selected tuples in current table (release after copying to result table)

    // find the table
    for (i = 0; i < db->ntables; ++i){
        if (strcmp(db->tables[i].name, table1_name) == 0){
            r = db->tables[i];
            ++table_found_r;
        }
        if (strcmp(db->tables[i].name, table2_name) == 0){
            s = db->tables[i];
            ++table_found_s;
        }
    }
    assert(table_found_r != 0 && table_found_s != 0);

    // get the number of tuples and attributes of R table
    ntuples_r = r.ntuples;
    nattrs_r = r.nattrs;
    ntuples_per_page_r = (cf->page_size - sizeof(UINT64)) / (nattrs_r * sizeof(INT));
    ntuples_last_page_r = ntuples_r % ntuples_per_page_r;
    if (ntuples_last_page_r == 0) ntuples_last_page_r = ntuples_per_page_r;
    npages_r = (r.ntuples + ntuples_per_page_r - 1) / ntuples_per_page_r;

    // get the number of tuples and attributes of S table
    ntuples_s = s.ntuples;
    nattrs_s = s.nattrs;
    ntuples_per_page_s = (cf->page_size - sizeof(UINT64)) / (nattrs_s * sizeof(INT));
    ntuples_last_page_s = ntuples_s % ntuples_per_page_s;
    if (ntuples_last_page_s == 0) ntuples_last_page_s = ntuples_per_page_s;
    npages_s = (s.ntuples + ntuples_per_page_s - 1) / ntuples_per_page_s;

    printf("ntuples_r = %d, nattrs_r = %d, ntuples_per_page_r = %d, ntuples_last_page_r = %d, npages_r = %d\n",
           ntuples_r, nattrs_r, ntuples_per_page_r, ntuples_last_page_r, npages_r);
    printf("ntuples_s = %d, nattrs_s = %d, ntuples_per_page_s = %d, ntuples_last_page_s = %d, npages_s = %d\n",
           ntuples_s, nattrs_s, ntuples_per_page_s, ntuples_last_page_s, npages_s);

    assert(idx1 < nattrs_r && idx2 < nattrs_s);

    // get the first page_id of R table
    table_fp_r = open_file(r.oid);
    assert(table_fp_r != NULL);
    fseek(table_fp_r,0,SEEK_SET);
    fread(&page_id_init_r,sizeof(UINT64),1,table_fp_r);
    unpin_file(r.oid);

    // get the first page_id of S table
    table_fp_s = open_file(s.oid);
    assert(table_fp_s != NULL);
    fseek(table_fp_s,0,SEEK_SET);
    fread(&page_id_init_s,sizeof(UINT64),1,table_fp_s);
    unpin_file(s.oid);

    // initialize result table
    nattrs_res = nattrs_r + nattrs_s;
    tuples_cur_table = malloc(sizeof(Tuple) * (ntuples_r * ntuples_s)); // join at most ntuples_r * ntuples_s

//    puts("join() is init.\n");

    if (cf->buf_slots < npages_r + npages_s){

        //// nested loop join

        puts("join() is nested loop join.\n");


        // choose the smaller table as outer loop
        if (npages_r <= npages_s){   // set table r outside

            puts("join() is nested loop join, set table r outside.\n");

            // buffer assignment
            UINT buf_slot_outer = cf->buf_slots-1;
            if (buf_slot_outer > npages_r) buf_slot_outer = npages_r;
            UINT buf_slot_inner = cf->buf_slots - buf_slot_outer;

//            printf("buf_slot_outer = %d, buf_slot_inner = %d\n", buf_slot_outer, buf_slot_inner);

            UINT count_in_buffer = 0;   // at most == buf_slot_outer
            UINT count_out_buffer = 0;  // at most == buf_slot_inner

            UINT is_in_buffer[npages_r];
            for(i=0;i<npages_r;++i) is_in_buffer[i] = 0;
            UINT inner_in_buffer[npages_s];
            for(i=0;i<npages_s;++i) inner_in_buffer[i] = 0;


            // check if any page in R,S that are in buffer
            for (i=0;i<cf->buf_slots;++i) {

                if ( buffer[i].oid == r.oid && buffer[i].pid >= page_id_init_r && buffer[i].pid < page_id_init_r + npages_r && buffer[i].page_ptr != NULL) {
                    if (count_out_buffer<buf_slot_outer) {
                        is_in_buffer[buffer[i].pid - page_id_init_r] = 1;
                        buffer[i].pin = 1;
                        ++count_out_buffer;
//                        printf("exist out table:buffer[%d].oid = %d, buffer[%d].pid = %lld, buffer[%d].page_ptr = %p",i,buffer[i].oid,i,buffer[i].pid,i,buffer[i].page_ptr);
                    }   // reserve at lease one page for inner loop
                    else{
                        buffer[i].pin = 0;
                    }
                }

                if ( buffer[i].oid == s.oid && buffer[i].pid >= page_id_init_s && buffer[i].pid < page_id_init_s + npages_s && buffer[i].page_ptr != NULL) {
                    if (count_in_buffer<buf_slot_inner) {
                        inner_in_buffer[buffer[i].pid - page_id_init_s] = 1;
                        buffer[i].pin = 1;
                        ++count_in_buffer;
//                        printf("exist in table:buffer[%d].oid = %d, buffer[%d].pid = %lld, buffer[%d].page_ptr = %p",i,buffer[i].oid,i,buffer[i].pid,i,buffer[i].page_ptr);
                    }
                    else{
                        buffer[i].pin = 0;
                    }
                }

            }

//            printf("count_in_buffer = %d, count_out_buffer = %d\n",count_in_buffer,count_out_buffer);

            // full the outer buffer as much as possible
            i = 0;
            k = 0;
            while (i<buf_slot_outer-count_out_buffer){
                k = i;
                for (j = 0; j < npages_r; ++j) {
                    if (is_in_buffer[j] == 0) {
                        UINT cur_page_slot_r = request_page(page_id_init_r + j,r.oid);
                        Page *cur_page_r = buffer[cur_page_slot_r].page_ptr;
                        if (cur_page_r == NULL) {
                            cur_page_r = read_page_from_file(r.oid, page_id_init_r + i, page_id_init_r);
                            assert(cur_page_r != NULL);
                            buffer[cur_page_slot_r].page_ptr = cur_page_r;
                        }
                        is_in_buffer[j] = 1;
                        ++i;
                        break;
                    }
                }
                if (k == i) break;
            }

            // full the inner buffer as much as possible
            i = 0;
            k = 0;
            while (i < buf_slot_inner-count_in_buffer ){
                k = i;
                for (j = 0; j < npages_s; ++j) {
                    if (inner_in_buffer[j] == 0) {
                        UINT cur_page_slot_s = request_page(page_id_init_s + j,s.oid);
                        Page *cur_page_s = buffer[cur_page_slot_s].page_ptr;
                        if (cur_page_s == NULL) {
                            cur_page_s = read_page_from_file(s.oid, page_id_init_s + i, page_id_init_s);
                            assert(cur_page_s != NULL);
                            buffer[cur_page_slot_s].page_ptr = cur_page_s;
                        }
                        inner_in_buffer[j] = 1;
                        ++i;
                        break;
                    }
                }
                if (k == i) break;
            }


            // log
//            INT count_pin_out = 0;
//            INT count_pin_in = 0;
//            for (i=0;i<cf->buf_slots;++i) {
//                if (buffer[i].oid == r.oid && buffer[i].pid >= page_id_init_r &&
//                    buffer[i].pid < page_id_init_r + npages_r && buffer[i].page_ptr != NULL) {
//                    count_pin_out ++;
//                }
//                if (buffer[i].oid == s.oid && buffer[i].pid >= page_id_init_s &&
//                    buffer[i].pid < page_id_init_s + npages_s && buffer[i].page_ptr != NULL) {
//                    count_pin_in ++;
//                }
//            }
//            printf("count_pin_out = %d\n",count_pin_out);
//            printf("count_pin_in = %d\n",count_pin_in);


            INT outer_loop_count = 0;
            INT inner_loop_count = 0;

            i = 0;
            j = 0;
            k = 0;
            l = 0;

            while (outer_loop_count < npages_r){

                while (inner_loop_count < npages_s){  // set table s inside

                    for (k = 0; k < cf->buf_slots; ++k){
                        // inner loop
                        if (buffer[k].oid == s.oid && buffer[k].pid >= page_id_init_s &&
                            buffer[k].pid < page_id_init_s + npages_s && buffer[k].page_ptr != NULL) {
                            if (inner_in_buffer[buffer[k].pid - page_id_init_s] == -1){
                                continue;
                            }

                            inner_loop_count ++;

                            printf("\n inner_loop_count = %lld\n",buffer[k].pid);

                            // try reading page from buffer
                            UINT ntuples_of_cur_page_s = 0;
                            UINT cur_page_slot_s = request_page(buffer[k].pid, s.oid);
                            Page *cur_page_s = buffer[cur_page_slot_s].page_ptr;

                            // if the page unloaded then read it from file
                            if (cur_page_s == NULL) {
                                cur_page_s = read_page_from_file(s.oid, buffer[k].pid, page_id_init_s);
                                buffer[cur_page_slot_s].page_ptr = cur_page_s;
                            }

                            if (k == npages_s - 1) {
                                ntuples_of_cur_page_s = ntuples_last_page_s;
                            } else {
                                ntuples_of_cur_page_s = ntuples_per_page_s;
                            }

                            // for each pair of tuples in R and S, check if they match
                            // once they match, add the result to the result table
                            puts ("cur_page_s");
                            for (j = 0; j < ntuples_of_cur_page_s * nattrs_s; j += nattrs_s) {
                                for (l = 0; l < nattrs_s; ++l) {
                                    printf("%d ", cur_page_s->data[j + l]);
                                }
                                printf("\n");
                            }
                            printf("\n");

                            for (i = 0; i < cf->buf_slots; ++i) {
                                // outer loop
                                if (buffer[i].oid == r.oid && buffer[i].pid >= page_id_init_r &&
                                    buffer[i].pid < page_id_init_r + npages_r && buffer[i].page_ptr != NULL) {
                                    if (is_in_buffer[buffer[i].pid - page_id_init_r] == -1){
                                        continue;
                                    }

                                    outer_loop_count++;

                                    printf("outer_loop_count = %lld\n",buffer[i].pid);

                                    UINT ntuples_of_cur_page_r = 0;
                                    UINT cur_page_slot_r = request_page(buffer[i].pid, r.oid);
                                    Page *cur_page_r = buffer[cur_page_slot_r].page_ptr;

                                    // if the page unloaded then read it from file
                                    if (cur_page_r == NULL) {
                                        printf("error");
                                        cur_page_r = read_page_from_file(r.oid, buffer[i].pid, page_id_init_r);
                                        buffer[cur_page_slot_r].page_ptr = cur_page_r;
                                    }

                                    if (i == npages_r - 1) {
                                        ntuples_of_cur_page_r = ntuples_last_page_r;
                                    } else {
                                        ntuples_of_cur_page_r = ntuples_per_page_r;
                                    }

                                    // start matching

                                    // print out the page
                                    puts ("cur_page_r");
                                    for (j = 0; j < ntuples_of_cur_page_r * nattrs_r; j += nattrs_r) {
                                        for (l = 0; l < nattrs_r; ++l) {
                                            printf("%d ", cur_page_r->data[j + l]);
                                        }
                                        printf("\n");
                                    }

                                    for (j =0; j < ntuples_of_cur_page_r * nattrs_r; j+= nattrs_r){
                                        INT r_i = cur_page_r->data[j + idx1];
                                        for (l = 0; l < ntuples_of_cur_page_s * nattrs_s; l += nattrs_s) {
                                            INT s_i = cur_page_s->data[l + idx2];
                                            if (r_i == s_i) {
                                                // add the result to the result table
                                                Tuple tup = malloc(sizeof(INT) * nattrs_res);
                                                for (m = 0; m < nattrs_r; ++m) {
                                                    tup[m] = cur_page_r->data[j + m];
                                                }
                                                for (m = 0; m < nattrs_s; ++m) {
                                                    tup[nattrs_r + m] = cur_page_s->data[l + m];
                                                }
                                                tuples_cur_table[ntuples_res] = tup;
                                                ++ntuples_res;
                                            }
                                        }

                                    }
                                }
                            }
                            // release page s
                            release_page(cur_page_s->pid, s.oid);
                            inner_in_buffer[cur_page_s->pid - page_id_init_s] = -1;
                        }
                    }

//                    // release inner pages
//                    for (k = 0; k < cf->buf_slots; ++k){
//                        // try release page from buffer
//                        if (buffer[k].oid == s.oid && buffer[k].pid >= page_id_init_s &&
//                            buffer[k].pid < page_id_init_s + npages_s && buffer[k].page_ptr != NULL) {
//                            release_page(buffer[k].pid, s.oid);
//                            inner_in_buffer[buffer[k].pid - page_id_init_s] = -1;
//                        }
//                    }

                    if (inner_loop_count < npages_s) {

                        INT count = 0;
                        for (k = 0; k < npages_s && count < buf_slot_inner; ++k) {
                            if (inner_in_buffer[k] == 0) {
                                UINT cur_page_slot_s = request_page(page_id_init_s + k, s.oid);
                                Page *cur_page_s = buffer[cur_page_slot_s].page_ptr;
                                if (cur_page_s == NULL) {
                                    cur_page_s = read_page_from_file(s.oid, page_id_init_s + k, page_id_init_s);
                                    buffer[cur_page_slot_s].page_ptr = cur_page_s;
                                }
                                inner_in_buffer[k] = 1;
                                count++;
                            }
                        }
                    }

                }


                // release outer pages
                for (i = 0; i < cf->buf_slots; ++i) {
                    // try release page from buffer
                    if (buffer[i].oid == r.oid && buffer[i].pid >= page_id_init_r &&
                        buffer[i].pid < page_id_init_r + npages_r && buffer[i].page_ptr != NULL) {
                        release_page(buffer[i].pid, r.oid);   // release page r
                        is_in_buffer[buffer[i].pid - page_id_init_r] = -1;
                    }
                }

                // reload pages
                if (outer_loop_count < npages_r) {
                    INT count = 0;
                    for (i = 0; i < npages_r && count < buf_slot_outer; ++i) {
                        if (is_in_buffer[i] == 0) {
                            UINT cur_page_slot_r = request_page(page_id_init_r + i, r.oid);
                            Page *cur_page_r = buffer[cur_page_slot_r].page_ptr;
                            if (cur_page_r == NULL) {
                                cur_page_r = read_page_from_file(r.oid, page_id_init_r + i, page_id_init_r);
                                buffer[cur_page_slot_r].page_ptr = cur_page_r;
                            }
                            is_in_buffer[i] = 1;
                            count++;
                        }
                    }
                }

            }


//            puts("result of join() init");

            result = malloc(sizeof(_Table)+sizeof(Tuple)*ntuples_res);
            result->ntuples = ntuples_res;
            result->nattrs = nattrs_res;
            for (i=0;i<ntuples_res;++i){
                result->tuples[i] = tuples_cur_table[i];
            }
            if (tuples_cur_table != NULL)
                free(tuples_cur_table);

            unpin_file(r.oid);
            unpin_file(s.oid);

            return result;



        } else {    // set table s outside

            puts("join() is nested loop join, set table s outside.\n");

            // buffer assignment
            UINT buf_slot_outer = cf->buf_slots-1;
            if (buf_slot_outer > npages_s) buf_slot_outer = npages_s;
            UINT buf_slot_inner = cf->buf_slots - buf_slot_outer;

//            printf("buf_slot_outer = %d, buf_slot_inner = %d\n", buf_slot_outer, buf_slot_inner);

            UINT count_in_buffer = 0;   // at most == buf_slot_outer
            UINT count_out_buffer = 0;  // at most == buf_slot_inner

            UINT is_in_buffer[npages_s];
            for(i=0;i<npages_s;++i) is_in_buffer[i] = 0;
            UINT inner_in_buffer[npages_r];
            for(i=0;i<npages_r;++i) inner_in_buffer[i] = 0;

            // check if pages in R,S that are in buffer
            for (i=0;i<cf->buf_slots;++i) {

                if ( buffer[i].oid == r.oid && buffer[i].pid >= page_id_init_r && buffer[i].pid < page_id_init_r + npages_r && buffer[i].page_ptr != NULL) {
                    if (count_in_buffer<buf_slot_inner) {
                        inner_in_buffer[buffer[i].pid - page_id_init_r] = 1;
                        buffer[i].pin = 1;
                        ++count_in_buffer;
//                        printf("exist in table:buffer[%d].oid = %d, buffer[%d].pid = %lld, buffer[%d].page_ptr = %p",i,buffer[i].oid,i,buffer[i].pid,i,buffer[i].page_ptr);
                    }
                    else{
                        buffer[i].pin = 0;
                    }
                }

                if ( buffer[i].oid == s.oid && buffer[i].pid >= page_id_init_s && buffer[i].pid < page_id_init_s + npages_s && buffer[i].page_ptr != NULL) {
                    if (count_out_buffer<buf_slot_outer) {
                        is_in_buffer[buffer[i].pid - page_id_init_s] = 1;
                        buffer[i].pin = 1;
                        ++count_out_buffer;
//                        printf("exist out table:buffer[%d].oid = %d, buffer[%d].pid = %lld, buffer[%d].page_ptr = %p",i,buffer[i].oid,i,buffer[i].pid,i,buffer[i].page_ptr);
                    }   // reserve at lease one page for inner loop
                    else{
                        buffer[i].pin = 0;
                    }
                }

            }

            // full the outer buffer as much as possible
            i = 0;
            k = 0;
            while (i<buf_slot_outer-count_out_buffer){
                k = i;
                for (j = 0; j < npages_s; ++j) {
                    if (is_in_buffer[j] == 0) {
                        UINT cur_page_slot_s = request_page(page_id_init_s + j,s.oid);
                        Page *cur_page_s = buffer[cur_page_slot_s].page_ptr;
                        if (cur_page_s == NULL) {
                            cur_page_s = read_page_from_file(s.oid, page_id_init_s + i, page_id_init_s);
                            assert(cur_page_s != NULL);
                            buffer[cur_page_slot_s].page_ptr = cur_page_s;
                        }
                        is_in_buffer[j] = 1;
                        ++i;
                        break;
                    }
                }
                if (k == i) break;
            }

            // full the inner buffer as much as possible
            i = 0;
            k = 0;
            while (i < buf_slot_inner-count_in_buffer){
                k = i;
                for (j = 0; j < npages_r; ++j) {
                    if (inner_in_buffer[j] == 0) {
                        UINT cur_page_slot_r = request_page(page_id_init_r + j,r.oid);
                        Page *cur_page_r = buffer[cur_page_slot_r].page_ptr;
                        if (cur_page_r == NULL) {
                            cur_page_r = read_page_from_file(r.oid, page_id_init_r + i, page_id_init_r);
                            assert(cur_page_r != NULL);
                            buffer[cur_page_slot_r].page_ptr = cur_page_r;
                        }
                        inner_in_buffer[j] = 1;
                        ++i;
                        break;
                    }
                }
                if (k == i) break;
            }


            INT outer_loop_count = 0;
            INT inner_loop_count = 0;

            i = 0;
            j = 0;
            k = 0;
            l = 0;

            while(outer_loop_count<npages_s){

                while(inner_loop_count<npages_r){
                    for (k = 0; k < cf->buf_slots; ++k){
                        // inner loop
                        if (buffer[k].oid == r.oid && buffer[k].pid >= page_id_init_r &&
                            buffer[k].pid < page_id_init_r + npages_r && buffer[k].page_ptr != NULL) {
                            if (inner_in_buffer[buffer[k].pid - page_id_init_r] == -1){
                                continue;
                            }

                            inner_loop_count ++;

                            printf("\n inner_loop_count = %lld\n",buffer[k].pid);

                            // try reading page from buffer
                            UINT ntuples_of_cur_page_r = 0;
                            UINT cur_page_slot_r = request_page(buffer[k].pid, r.oid);
                            Page *cur_page_r = buffer[cur_page_slot_r].page_ptr;

                            // if the page unloaded then read it from file
                            if (cur_page_r == NULL) {
                                cur_page_r = read_page_from_file(r.oid, buffer[k].pid, page_id_init_r);
                                buffer[cur_page_slot_r].page_ptr = cur_page_r;
                            }

                            if (k == npages_r - 1) {
                                ntuples_of_cur_page_r = ntuples_last_page_r;
                            } else {
                                ntuples_of_cur_page_r = ntuples_per_page_r;
                            }

                            // print the page
                            puts ("cur_page_r");
                            for (j = 0; j < ntuples_of_cur_page_r * nattrs_r; j += nattrs_r) {
                                for (l = 0; l < nattrs_r; ++l) {
                                    printf("%d ", cur_page_r->data[j + l]);
                                }
                                printf("\n");
                            }
                            printf("\n");

                            for (i = 0; i < cf->buf_slots; ++i) {
                                // outer loop
                                if (buffer[i].oid == s.oid && buffer[i].pid >= page_id_init_s &&
                                    buffer[i].pid < page_id_init_s + npages_s && buffer[i].page_ptr != NULL) {
                                    if (is_in_buffer[buffer[i].pid - page_id_init_s] == -1){
                                        continue;
                                    }

                                    outer_loop_count++;

                                    printf("outer_loop_count = %lld\n",buffer[i].pid);

                                    UINT ntuples_of_cur_page_s = 0;
                                    UINT cur_page_slot_s = request_page(buffer[i].pid, s.oid);
                                    Page *cur_page_s = buffer[cur_page_slot_s].page_ptr;

                                    // if the page unloaded then read it from file
                                    if (cur_page_s == NULL) {
                                        printf("error");
                                        cur_page_s = read_page_from_file(s.oid, buffer[i].pid, page_id_init_s);
                                        buffer[cur_page_slot_s].page_ptr = cur_page_s;
                                    }

                                    if (i == npages_s - 1) {
                                        ntuples_of_cur_page_s = ntuples_last_page_s;
                                    } else {
                                        ntuples_of_cur_page_s = ntuples_per_page_s;
                                    }

                                    // start matching

                                    // print out the page
                                    puts ("cur_page_s");
                                    for (j = 0; j < ntuples_of_cur_page_s * nattrs_s; j += nattrs_s) {
                                        for (l = 0; l < nattrs_s; ++l) {
                                            printf("%d ", cur_page_s->data[j + l]);
                                        }
                                        printf("\n");
                                    }

                                    for (j =0; j < ntuples_of_cur_page_r * nattrs_r; j+= nattrs_r){
                                        INT r_i = cur_page_r->data[j + idx1];
                                        for (l = 0; l < ntuples_of_cur_page_s * nattrs_s; l += nattrs_s) {
                                            INT s_i = cur_page_s->data[l + idx2];
                                            if (r_i == s_i) {
                                                // add the result to the result table
                                                Tuple tup = malloc(sizeof(INT) * nattrs_res);
                                                for (m = 0; m < nattrs_r; ++m) {
                                                    tup[m] = cur_page_r->data[j + m];
                                                }
                                                for (m = 0; m < nattrs_s; ++m) {
                                                    tup[nattrs_r + m] = cur_page_s->data[l + m];
                                                }
                                                tuples_cur_table[ntuples_res] = tup;
                                                ++ntuples_res;
                                            }
                                        }

                                    }
                                }
                            }
                            // release page s
                            release_page(cur_page_r->pid, r.oid);
                            inner_in_buffer[cur_page_r->pid - page_id_init_r] = -1;
                        }
                    }

                    if (inner_loop_count < npages_r) {

                        INT count = 0;
                        for (k = 0; k < npages_r && count < buf_slot_inner; ++k) {
                            if (inner_in_buffer[k] == 0) {
                                UINT cur_page_slot_r = request_page(page_id_init_r + k, r.oid);
                                Page *cur_page_r = buffer[cur_page_slot_r].page_ptr;
                                if (cur_page_r == NULL) {
                                    cur_page_r = read_page_from_file(r.oid, page_id_init_r + k, page_id_init_r);
                                    buffer[cur_page_slot_r].page_ptr = cur_page_r;
                                }
                                inner_in_buffer[k] = 1;
                                count++;
                            }
                        }
                    }

                }

                // release outer pages
                for (i = 0; i < cf->buf_slots; ++i) {
                    // try release page from buffer
                    if (buffer[i].oid == s.oid && buffer[i].pid >= page_id_init_s &&
                        buffer[i].pid < page_id_init_s + npages_s && buffer[i].page_ptr != NULL) {
                        release_page(buffer[i].pid, s.oid);   // release page r
                        is_in_buffer[buffer[i].pid - page_id_init_s] = -1;
                    }
                }

                // reload pages
                if (outer_loop_count < npages_s) {
                    INT count = 0;
                    for (i = 0; i < npages_s && count < buf_slot_outer; ++i) {
                        if (is_in_buffer[i] == 0) {
                            UINT cur_page_slot_s = request_page(page_id_init_s + i, s.oid);
                            Page *cur_page_s = buffer[cur_page_slot_s].page_ptr;
                            if (cur_page_s == NULL) {
                                cur_page_s = read_page_from_file(s.oid, page_id_init_s + i, page_id_init_s);
                                buffer[cur_page_slot_s].page_ptr = cur_page_s;
                            }
                            is_in_buffer[i] = 1;
                            count++;
                        }
                    }
                }

            }


            result = malloc(sizeof(_Table)+sizeof(Tuple)*ntuples_res);
            result->ntuples = ntuples_res;
            result->nattrs = nattrs_res;
            for (i=0;i<ntuples_res;++i){
                result->tuples[i] = tuples_cur_table[i];
            }
            if (tuples_cur_table != NULL)
                free(tuples_cur_table);

            unpin_file(r.oid);
            unpin_file(s.oid);

            return result;


        }


    } else {

        //// sort-merge join

        puts("sort-merge join");

        // initialize the sorted table
        INT ntuples_res_r = 0;  // for number of tuples check
        INT ntuples_res_s = 0;  // for number of tuples check

        Tuple * tuples_r = malloc(sizeof(Tuple)*ntuples_r);
        Tuple * tuples_s = malloc(sizeof(Tuple)*ntuples_s);

        for (i=0;i<ntuples_r;++i){
            tuples_r[i] = NULL;
        }
        for (i=0;i<ntuples_s;++i){
            tuples_s[i] = NULL;
        }

        // read all tuples from table r

        puts("start to read all tuples from table r");

        for (i=0;i<npages_r;++i){
            // try reading page from buffer
            UINT ntuples_of_cur_page_r = 0;
            UINT cur_page_slot_r = request_page(page_id_init_r+i,r.oid);
            Page *cur_page_r = buffer[cur_page_slot_r].page_ptr;

//            printf("read page %lld of oid: %d from buffer\n", page_id_init_r+i, r.oid);

            // if the page unloaded then read it from file
            if (cur_page_r == NULL){
                cur_page_r = read_page_from_file(r.oid, page_id_init_r+i, page_id_init_r);
                buffer[cur_page_slot_r].page_ptr = cur_page_r;
            }

            if (cur_page_r == NULL)
                puts("cur_page_r is NULL!!!!!!");

//            puts("read page from file done ");

            if (i == npages_r-1){
                ntuples_of_cur_page_r = ntuples_last_page_r;
            } else{
                ntuples_of_cur_page_r = ntuples_per_page_r;
            }

//            puts("start to copy tuples from page r");

            for (j=0; j < ntuples_of_cur_page_r*nattrs_r; j+=nattrs_r){  // for each tuple in the page

                Tuple tup = malloc(sizeof(INT)*nattrs_r);
                for (k=0;k<nattrs_r;++k){   // for each attribute in the tuple
                    tup[k] = cur_page_r->data[j+k];
                }
                tuples_r[ntuples_res_r] = tup;
                ++ntuples_res_r;
            }

//            puts("copy tuples from page done");

            release_page(cur_page_r->pid,r.oid);
        }

        if (ntuples_res_r != ntuples_r)
            printf("Error: ntuples_res_r %d != ntuples_r %d\n", ntuples_res_r, ntuples_r);


        puts("\nstart to read all tuples from table s\n");


        for (i=0;i<npages_s;++i){

//            printf("the %dth page of s\n", i);
            // try reading page from buffer
            UINT ntuples_of_cur_page_s = 0;
            UINT cur_page_slot_s = request_page(page_id_init_s+i,s.oid);
            Page *cur_page_s = buffer[cur_page_slot_s].page_ptr;

            // if the page unloaded then read it from file
            if (cur_page_s == NULL){
                cur_page_s = read_page_from_file(s.oid, page_id_init_s+i, page_id_init_s);
                buffer[cur_page_slot_s].page_ptr = cur_page_s;
            }

            if (i == npages_s-1){
                ntuples_of_cur_page_s = ntuples_last_page_s;
            } else{
                ntuples_of_cur_page_s = ntuples_per_page_s;
            }

//            puts("start to copy tuples from page s");

            for (j=0; j < ntuples_of_cur_page_s*nattrs_s; j+=nattrs_s){  // for each tuple in the page
                Tuple tup = malloc(sizeof(INT)*nattrs_s);
                for (k=0;k<nattrs_s;++k){   // for each attribute in the tuple
                    tup[k] = cur_page_s->data[j+k];
                }
                tuples_s[ntuples_res_s] = tup;
                ++ntuples_res_s;
            }

            release_page(cur_page_s->pid,s.oid);
        }

        if (ntuples_res_s != ntuples_s)
            printf("Error: ntuples_res_s %d != ntuples_s %d\n", ntuples_res_s, ntuples_s);


        puts("sort-merge join init done");

        //// sort the tables first

        // sort the tuples in r according to idx1
        idx_cmp_use_only = idx1;
        qsort(tuples_r, ntuples_r, sizeof(Tuple), compare_tuples);
        // sort the tuples in s according to idx2
        idx_cmp_use_only = idx2;
        qsort(tuples_s, ntuples_s, sizeof(Tuple), compare_tuples);


//        puts("sort-merge join sort done");

        // merge the two sorted tables

        // algorithm according to Fundamentals of Database Systems, 6th edition, p. 556

//        puts("R");
//        for (i=0;i<ntuples_r;++i){
//            for (j=0;j<nattrs_r;++j){
//                printf("%d ", tuples_r[i][j]);
//            }
//            puts("");
//        }
//
//        puts("S");
//        for (i=0;i<ntuples_s;++i){
//            for (j=0;j<nattrs_s;++j){
//                printf("%d ", tuples_s[i][j]);
//            }
//            puts("");
//        }

        i = 0;
        j = 0;
        k = 0;
        l = 0;

        while (i < ntuples_r && j < ntuples_s){
            if (tuples_r[i][idx1] < tuples_s[j][idx2]){
                ++i;
            } else if (tuples_r[i][idx1] > tuples_s[j][idx2]){
                ++j;
            } else{

//                printf("\n match: i: %d, j: %d\n\n", i, j);

                // join the two tuples
                Tuple tup = malloc(sizeof(INT)*(nattrs_res));
                for (m=0;m<nattrs_r;++m){
                    tup[m] = tuples_r[i][m];
                }
                for (m=0;m<nattrs_s;++m){
                    tup[nattrs_r+m] = tuples_s[j][m];
                }
                tuples_cur_table[ntuples_res] = tup;
                ++ntuples_res;

                // match the same value in R
                l = j+1;
                while (l < ntuples_s && tuples_r[i][idx1] == tuples_s[l][idx2]){
                    // join the two tuples
                    Tuple tup2 = malloc(sizeof(INT)*(nattrs_res));
                    for (m=0;m<nattrs_r;++m){
                        tup2[m] = tuples_r[i][m];
                    }
                    for (m=0;m<nattrs_s;++m){
                        tup2[nattrs_r+m] = tuples_s[l][m];
                    }
                    tuples_cur_table[ntuples_res] = tup2;
                    ++ntuples_res;
                    // index increase
                    ++l;
                }
                // maybe ++i instead of k = i+1
                ++i;
            }
//            // match the same value in S
//            k = i+1;
//            while(k < ntuples_r && tuples_r[k][idx1] == tuples_s[j][idx2]){
//
//                // join the two tuples
//                Tuple tup2 = malloc(sizeof(INT)*(nattrs_res));
//                for (m=0;m<nattrs_r;++m){
//                    tup2[m] = tuples_r[k][m];
//                }
//                for (m=0;m<nattrs_s;++m){
//                    tup2[nattrs_r+m] = tuples_s[j][m];
//                }
//                tuples_cur_table[ntuples_res] = tup2;
//                ++ntuples_res;
//                // index increase
//                ++k;
//
//            }
//            // set i, j
//            i = k;
//            j = l;
        }

        // write the result to file
        result = malloc(sizeof(_Table) + sizeof(Tuple) * ntuples_res);
        result->ntuples = ntuples_res;
        result->nattrs = nattrs_res;

//        puts("show result");


        for (i = 0; i < ntuples_res; ++i) {
            result->tuples[i] = tuples_cur_table[i];


//            for (j = 0; j < nattrs_res; ++j) {
//                printf("%d ", tuples_cur_table[i][j]);
//            }
//            printf("\n");

        }

//        puts("show result done");
//

        puts("sort-merge join done && start to free");

        // release tuples_r and tuples_s & tuples inside them
        for (i = 0; i < ntuples_r; ++i) {
            if (tuples_r[i] != NULL) free(tuples_r[i]);
        }
        if (tuples_r != NULL) free(tuples_r);

//        puts("sort-merge join free tuples_r done");

        for (i = 0; i < ntuples_s; ++i) {
            if (tuples_s[i] != NULL) free(tuples_s[i]);
        }
        if (tuples_s != NULL) free(tuples_s);

//        puts("sort-merge join free tuples_s done");

        // release the shell tuples_cur_table
        if (tuples_cur_table != NULL) free(tuples_cur_table);

//        puts("sort-merge join free tuples_cur_table done");

        unpin_file(r.oid);
        unpin_file(s.oid);

//        puts("sort-merge join free done && return");

        return result;

    }

//    return NULL;
}