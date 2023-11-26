#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>


// Global variables
uint64_t row_count = 0;  // Total number of rows in the database
int col_count = 0;  // Total number of columns in the database
char filenameSkeleteon[1024];   // the filename skeleton (everything before .)

char *data_filename;            // Data file name (ending in .data)
char *sparse_index_filename;    // sparse index file name (ending in .sparse_index)
char *dense_index_filename;     // dense file name (ending in .dense_index)

uint64_t *sparse_index_only_buffer;      // this only stores the key value (used in linear/binary search)
uint64_t *sparse_index_and_ptr_buffer;   // this stores both the key values and the file offset pointer (used to compute the offset in data file)

uint64_t *dense_index_only_buffer;       // this only stores the key value (used in linear/binary search)
uint64_t *dense_index_and_ptr_buffer;    // this stores both the key values and the file offset pointer (used to compute the offset in data file)

/**
 * This function returns the total count of keys in the range [from, to]
 * This function READS ONE TUPLE AT A TIME from the file and then checks for the condition (between from and to).
 *
 * The SQL equivalent is:
 *
 * SELECT COUNT(*)
 * FROM table
 * where primary_key_column_value >= from AND primary_key_column_value <= to
 *
 * This function is to query on the primary key column.
 *
 */
int primary_key_read_by_tuple(uint64_t from, uint64_t to)
{
    int match_count = 0;                                          // counter to store the number of matching tuples (in range [from,to])

    size_t tuple_size_in_bytes = sizeof(uint64_t) * col_count;       // size of a tuple in bytes
    uint64_t *tuple_data = malloc(tuple_size_in_bytes);           // allocating buffer for one tuple

    int fd = open(data_filename, O_RDONLY);                       // opening the data file
    off_t file_offset = 0;                                          // offset in file (where to read)
    for (int i = 0; i < row_count; i++)
    {
        pread(fd, tuple_data, tuple_size_in_bytes, file_offset);
        file_offset = file_offset + tuple_size_in_bytes;

        // This can be done only for primary key
        if (tuple_data[0] > to)
            break;
        
        // increase the counter if key is in the range
        if (tuple_data[0] >= from && tuple_data[0] <= to)
            match_count++;
    }
    close(fd);
    free(tuple_data);

    return match_count;
}

/**
 * TODO - Task 2 - implement this function
 * This function returns the total count of keys in the range [from, to]
 * This function READS ONE BLOCK AT A TIME from the file and then SCANS through the TUPLES in the block to check if a tuple satisfies the condition (between from and to).
 *
 * The SQL equivalent is:
 *
 * SELECT COUNT(*)
 * FROM table
 * where primary_key_column_value >= from AND primary_key_column_value <= to
 *
 * This function is to query on the primary key column.
 *
 */
int primary_key_read_by_block(uint64_t from, uint64_t to, int number_of_tuples_per_block)
{
    int match_count = 0;                                     // counter to store the number of matching tuples (in range [from,to])
    // Step 1: Allocate block buffer to read data from file
    size_t block_size_in_number_of_items = col_count * number_of_tuples_per_block;     // Every block is composed of number_of_tuples_per_block rows/tuples
    size_t block_size_in_bytes = block_size_in_number_of_items * sizeof(uint64_t);
    size_t total_number_of_blocks_in_file = (row_count * col_count) / block_size_in_number_of_items;
    uint64_t *block_data = malloc(block_size_in_bytes);

    // Step 2: read data from file, one block at a time
    int fd =  open(data_filename, O_RDONLY);
    off_t file_offset = 0;

    // Step 3: Iterate through all the blocks in the file
    for (int i=0; i < total_number_of_blocks_in_file; i++)
	{
        pread(fd, block_data, block_size_in_bytes, file_offset);
        file_offset = file_offset + block_size_in_bytes;

        // Since data is stored in row-major order, we are iterating in strides of col_count    
		for (int j=0; j < block_size_in_number_of_items; j=j+col_count)
        {
            // You can skip scanning, is the first element of the block is greater than "to" of the range
            if (block_data[j] > to) break;

            if (block_data[j] >= from && block_data[j] <= to) match_count++;
		}
	}
    
    //        For each block, iterate through all the tuples in the block to check if a tuple is in the range
    //        You can skip an entire block, just by checkin the last tuple of the block
    //        You can skip scanning, is the first element of the block is greater than "to" of the range
    // NOTE: Step 3 is nested within Step 2 (i.e. you read a block and then you parse the block one by one)
    // TODO


    // Step 4: close the file and free the block buffer
    // uncomment the following line (close) 
	close(fd);
    free(block_data);
    return match_count;
}


/**
 * This function is used to load the DENSE INDEX from disk to memory
 * Once the index file is loaded it can be used to make fast queries
 *
 */
void load_dense_index_file()
{
    // Create dense index buffer (the size of this buffer is total number of rows x 2)
    dense_index_and_ptr_buffer = malloc(row_count * 2 * sizeof(uint64_t));

    // Open the dense index file
    int indf = open(dense_index_filename, O_RDONLY);
    
    // Read data from index file
    read(indf, dense_index_and_ptr_buffer, row_count * 2 * sizeof(uint64_t));
    
    // close index file
    close(indf);

    // This is to make our life easier,
    // dense_index_and_ptr_buffer has both keys and the file pointers
    // However, we need to perform search on only the keys
    // therefore, we are making a buffer with just the keys
    // and so, its size is row_count
    // We will perform search (linear or binary) directly in this array
    // and cross reference in the dense_index_and_ptr_buffer to find the location (pointer) of a key
    dense_index_only_buffer = malloc(row_count * sizeof(uint64_t));
    for (int i=0; i < row_count; i++)
        dense_index_only_buffer[i] = dense_index_and_ptr_buffer[i * 2];
}

// Free both dense index buffers
void unload_dense_index_file()
{
    free(dense_index_and_ptr_buffer);
    free(dense_index_only_buffer);
}

// Linear search to find the the key in the index file
// Note, the key might not exist in the index file, in that case
// you need to find the key closest (from left size) to the search key
uint64_t linearSearch(uint64_t arr[], uint64_t value)
{
    for (int i=0; i < row_count; i++)
    {
        if (arr[i] >= value)
            return i;
    }

    // key-value is greater than all keys in the table in that case return the last key of the index file
    return row_count-1;
}

/**
 * TODO - Task 4 - Binary search to accomplish the same task as linear search
 */
uint64_t binarySearch(uint64_t arr[], uint64_t value)
{
    // TODO implement binary search
    // NOT: return should not be 0
    uint64_t l = 0;
    uint64_t r = row_count-1;

    uint64_t mid = (l + r) / 2;
    while (l != r) {
        mid = (l + r) / 2;

        if (arr[mid] == value) {
            return arr[mid];
        }
        else if (arr[mid] < value) { 
            l = mid + 1; 
        } 
        else {
            r = mid - 1;
        }
    }

    return row_count-1;
}

/**
 * TODO - Task 3 - implement this file
 * This function returns the total count of keys in the range [from, to]
 * This function uses the DENSE INDEX FILE (which has been loaded in the memory)
 * The DENSE INDEX FILE is used to find the file pointer for from and to
 * Since the index file is built on the clustering index, we compute the from and to block id
 * and then we directly read the right data block from the disk and scan
 * one tuple at a time from the block and then checks for the condition (between from and to).
 *
 * The SQL equivalent is:
 *
 * SELECT COUNT(*)
 * FROM table
 * where primary_key_column_value >= from AND primary_key_column_value <= to
 *
 * This function is to query on the primary key column.
 *
 */
int primary_key_read_by_dense_index_file(uint64_t from, uint64_t to, int number_of_tuples_per_block)
{
    // Step 1: Look the dense buffer is loaded in memory; find the index corresponding to "from" (using linear/binary search, as the index is already sorted)
    // uint64_t from_key = linearSearch(dense_index_only_buffer, from);

    // Step 2: Look the dense buffer is loaded in memory; find the index corresponding to "to" (using linear/binary search, as the index is already sorted)
    // uint64_t to_key = linearSearch(dense_index_only_buffer, to);

    uint64_t from_key = binarySearch(dense_index_only_buffer, from); 
    uint64_t to_key = binarySearch(dense_index_only_buffer, to);

    // Step 3: Since data is sorted based on the primary key, we can make reads in block (instead of tuples)
    // Assuming that each block is composed of "number_of_tuples_per_block" rows
    // Compute the index of the starting block and ending block in the data file
    size_t block_size_in_number_of_items = col_count * number_of_tuples_per_block;     // Every block is composed of number_of_tuples_per_block rows
    size_t block_size_in_bytes = block_size_in_number_of_items * sizeof(uint64_t);
    int from_block_index = (dense_index_and_ptr_buffer[from_key * 2 + 1]) / block_size_in_number_of_items;
    int to_block_index = (dense_index_and_ptr_buffer[to_key * 2 + 1]) / block_size_in_number_of_items;
    
    // Step 4: Allocate buffer for a block
    uint64_t *block_data = malloc(block_size_in_bytes);
    
    // Step 5: open datafile
    int fd = open(data_filename, O_RDONLY);
    off_t block_offset = from_block_index * block_size_in_bytes;
    int match_count = 0;

    // printf("From index: %d\n", from_block_index);
    // printf("To index: %d\n", to_block_index);
    // Step 6: Iterate through only those blocks that might contain data in the queried range (starting from from_block_index)
    for (int i=from_block_index; i <= to_block_index; i++)
	{
        // printf("Block index: %d\n", i);
        pread(fd, block_data, block_size_in_bytes, block_offset);
        block_offset = block_offset + block_size_in_bytes;

        // Since data is stored in row-major order, we are iterating in strides of col_count    
		for (int j=0; j < block_size_in_number_of_items; j=j+col_count)
        {
            // printf("Block data: %d", block_data[j]);
            // You can skip scanning, is the first element of the block is greater than "to" of the range
            if (block_data[j] > to) break;

            if (block_data[j] >= from && block_data[j] <= to) match_count++;
		}
	}

    // Step 7: free block buffer
    // Make sure to un comment the following statement 
    close(fd);
    free(block_data);
    return match_count;
}

/**
 * TODO - Task 5 - Implement this function
 * This function is used to load the SPARSE INDEX from disk
 * Once the index file is loaded it can be used to make fast queries
 */
void load_sparse_index_file()
{
    int indf = open(sparse_index_filename, O_RDONLY);
    // TODO - 1 - Allocate sparse_index_and_ptr_buffer
    // TODO - 2 - Read data from sparse_index_filename into sparse_index_and_ptr_buffer
    close(indf);

    // TODO - 3 - Allocate sparse_index_only_buffer buffer
    // TODO - 4 - Copy just the attribute (column) value from sparse_index_and_ptr_buffer to sparse_index_only_buffer
}

// TODO - Task 7 - Uncomment the following
void unload_sparse_index_file()
{
    //free(sparse_index_and_ptr_buffer);
    //free(sparse_index_only_buffer);
}

/**
 * TODO - Task 6 - Implement this function
 * This function returns the total count of keys in the range [from, to]
 * This function uses the SPARSE INDEX FILE (which has been loaded in the memory)
 * The SPARSE INDEX FILE is used to find the file pointer for from and to
 * Since the index file is built on the clustering index, we compute the from and to block id
 * and then we directly read the right data block from the disk and scan
 * one tuple at a time from the block and then checks for the condition (between from and to).
 *
 * The SQL equivalent is:
 *
 * SELECT COUNT(*)
 * FROM table
 * where primary_key_column_value >= from AND primary_key_column_value <= to
 *
 * This function is to query on the primary key column.
 *
 */
int primary_key_read_by_sparse_index_file(uint64_t from, uint64_t to)
{
    return -1;
}

// Function to verify the correctness of all four implementation
void verify_correctness(int number_of_queries, int method1[], int method2[], int method3[], int method4[])
{
    for(int q=0; q< number_of_queries; q++)
        if ( !(method1[q] == method2[q] == method3[q] == method4[q]) )
            printf("Error or incomplete implementation\n");
}


void queries_on_primary_key(int number_of_queries, int query_from_range[], int query_to_range[])
{
    // Buffer to store the results of all queries for the four different schemes
    int* tuple_method_result_count = malloc(sizeof(int) * number_of_queries);
    int* block_method_result_count = malloc(sizeof(int) * number_of_queries);
    int* dense_index_method_result_count = malloc(sizeof(int) * number_of_queries);
    int* sparse_index_method_result_count = malloc(sizeof(int) * number_of_queries);


    // Queries using one tuple I/O at a time
    clock_t start_b1 = clock();
    for (int q = 0; q < number_of_queries; q++)
    {
        tuple_method_result_count[q] = primary_key_read_by_tuple(query_from_range[q], query_to_range[q]);
        printf("[Tuple I/O method] Count of tuples in the range [%d, %d] = %d\n", query_from_range[q], query_to_range[q], tuple_method_result_count[q]);
    }
    clock_t end_b1 = clock();
    float seconds_b1 = (float)(end_b1 - start_b1) / CLOCKS_PER_SEC;
    printf("\n");

    // Queries using one block I/O at a time
    clock_t start_b2 = clock();
    int number_of_tuples_per_block = 400;
    for (int q = 0; q < number_of_queries; q++)
    {
        block_method_result_count[q] = primary_key_read_by_block(query_from_range[q], query_to_range[q], number_of_tuples_per_block);
        printf("[Block I/O method] Count of tuples in the range [%d, %d] = %d\n", query_from_range[q], query_to_range[q], block_method_result_count[q]);
    }
    clock_t end_b2 = clock();
    float seconds_b2 = (float)(end_b2 - start_b2) / CLOCKS_PER_SEC;
    printf("\n");

    // load dense index file
    load_dense_index_file();

    // Queries using dense index file
    clock_t start_b3 = clock();
    for (int q = 0; q < number_of_queries; q++)
    {
        dense_index_method_result_count[q] = primary_key_read_by_dense_index_file(query_from_range[q], query_to_range[q], number_of_tuples_per_block);
        printf("[Using Dense Index file] Count of tuples in the range [%d, %d] = %d\n", query_from_range[q], query_to_range[q], dense_index_method_result_count[q]);
    }
    clock_t end_b3 = clock();
    float seconds_b3 = (float)(end_b3 - start_b3) / CLOCKS_PER_SEC;
    printf("\n");

    // unload dense index file
    unload_dense_index_file();

    // load sparse index file
    load_sparse_index_file();

    // Queries using sparse index file
    clock_t start_b4 = clock();
    for (int q = 0; q < number_of_queries; q++)
    {
        sparse_index_method_result_count[q] = primary_key_read_by_sparse_index_file(query_from_range[q], query_to_range[q]);
        printf("[Using Sparse Index file] Count of tuples in the range [%d, %d] = %d\n", query_from_range[q], query_to_range[q], sparse_index_method_result_count[q]);
    }
    clock_t end_b4 = clock();
    float seconds_b4 = (float)(end_b4 - start_b4) / CLOCKS_PER_SEC;
    printf("\n");

    // unload sparse index file
    unload_sparse_index_file();

    verify_correctness(number_of_queries, tuple_method_result_count, block_method_result_count, dense_index_method_result_count, sparse_index_method_result_count);

    free(tuple_method_result_count);
    free(block_method_result_count);
    free(dense_index_method_result_count);
    free(sparse_index_method_result_count);
    printf("Time Tuple method %f | Block method (1024) %f | Dense Index method %f | Sparse Index method %f \n", seconds_b1, seconds_b2, seconds_b3, seconds_b4);
}



int main(int argc, char *argv[])
{
    char *filename = argv[1];

    FILE *fptr;
    fptr = fopen(filename, "r");
    fscanf(fptr, "%s\n%lld\n%d", filenameSkeleteon, &row_count, &col_count);
    fclose(fptr);

    data_filename = malloc(strlen(filenameSkeleteon) + 6);
    strcpy(data_filename, filenameSkeleteon);
    strcat(data_filename, ".data");

    sparse_index_filename = malloc(strlen(filenameSkeleteon) + 14);
    strcpy(sparse_index_filename, filenameSkeleteon);
    strcat(sparse_index_filename, ".sparse_index");

    dense_index_filename = malloc(strlen(filenameSkeleteon) + 13);
    strcpy(dense_index_filename, filenameSkeleteon);
    strcat(dense_index_filename, ".dense_index");

    // ALl queries on the primary key
    int number_of_queries = 8;
    int *query_from_range = malloc(sizeof(int) * number_of_queries);
    int *query_to_range = malloc(sizeof(int) * number_of_queries);
    
    query_from_range[0] = 159999000;
    query_from_range[1] = 19990000;
    query_from_range[2] = 1599000;
    query_from_range[3] = 15999990;
    query_from_range[4] = 129999000;
    query_from_range[5] = 1999000;
    query_from_range[6] = 10;
    query_from_range[7] = 179999000;
    
    query_to_range[0] = 160000000;
    query_to_range[1] = 20000000;
    query_to_range[2] = 16000000;
    query_to_range[3] = 16000000;
    query_to_range[4] = 139000000;
    query_to_range[5] = 1700000;
    query_to_range[6] = 50;
    query_to_range[7] = 180000000;

    // ALl queries on the non-primary key
    queries_on_primary_key(number_of_queries, query_from_range, query_to_range);
    
    free(query_from_range);
    free(query_to_range);
    free(data_filename);
    free(sparse_index_filename);
    free(dense_index_filename);

    return 0;
}
