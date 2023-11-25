#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>

uint64_t row_count = 0;
int col_count = 0;
char filenameSkeleteon[1024];

char *data_filename;            // Data file name (ending in .data)
char *sparse_index_filename;    // sparse index file name (ending in .sparse_index)
char *dense_index_filename;     // dense file name (ending in .dense_index)

// Function to write dense index file
void create_dense_clustering_key()
{
    // Step 1: Allocate block buffer to read data from the data file
    size_t block_size_in_number_of_items = col_count * 400;     // Every block is composed of 400 rows
    size_t block_size_in_bytes = block_size_in_number_of_items * sizeof(uint64_t);
    size_t total_number_of_blocks_in_file = (row_count * col_count) / block_size_in_number_of_items;
    uint64_t *block_data = malloc(block_size_in_bytes);

    // Step 2: the index file is made up of two columns and "row_count" rows, 
    // col1 stores the actual keys and col2 stores the file pointer (in terms of count, and not bytes)
    size_t index_buffer_size_in_number_of_items = row_count * 2;
    size_t index_buffer_size_in_bytes = index_buffer_size_in_number_of_items * sizeof(uint64_t);
	uint64_t *index_buffer = malloc(index_buffer_size_in_bytes);

    // Step 3: read the data file, block by block and populate the index_buffer
    int tuple_count = 0;
    int fd =  open(data_filename, O_RDONLY);
    off_t file_offset = 0;
    for (int i=0; i < total_number_of_blocks_in_file; i++)
	{
        pread(fd, block_data, block_size_in_bytes, file_offset);
        file_offset = file_offset + block_size_in_bytes;

        // Since data is stored in row-major order, we are iterating in strides of col_count    
		for (int j=0; j < block_size_in_number_of_items; j=j+col_count)
        {
            // Step 4: create the entries of the dense index file
            // printf("J value: %d\n", j);
			index_buffer[2 * tuple_count + 0] = block_data[j];
			index_buffer[2 * tuple_count + 1] = tuple_count * col_count;
            //printf("IB %d %d\n", index_buffer[2 * tuple_count + 0], index_buffer[2 * tuple_count + 1]);
            tuple_count = tuple_count + 1;
		}
	}
    printf("Tuples: %d", tuple_count);
	close(fd);
	
    // Step 5: Write the index buffer to file (to be later opened while performing queries)
	int indexF =  open(dense_index_filename, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH);
    write(indexF, index_buffer, index_buffer_size_in_bytes);
	close(indexF);

	free(index_buffer);
    free(block_data);
}


// TODO - Task 1 - Implement this function to generate sparse index file
// The index file should be one magnitude smaller in size compared to the dense index file
// (simplest way to implement this will be to store every 10th key in the sparse index file
void create_sparse_clustering_key()
{
    // Step 1: Allocate block buffer to read data from the data file
    size_t block_size_in_number_of_items = col_count * 400;     // Every block is composed of 400 rows
    size_t block_size_in_bytes = block_size_in_number_of_items * sizeof(uint64_t);
    size_t total_number_of_blocks_in_file = (row_count * col_count) / block_size_in_number_of_items;
    uint64_t *block_data = malloc(block_size_in_bytes);

    size_t index_buffer_size_in_number_of_items = row_count / 10 * 2;
    size_t index_buffer_size_in_bytes = index_buffer_size_in_number_of_items * sizeof(uint64_t);
	uint64_t *index_buffer = malloc(index_buffer_size_in_bytes);

    int tuple_count = 0;
    int fd =  open(data_filename, O_RDONLY);
    off_t file_offset = 0;

    for (int i=0; i < total_number_of_blocks_in_file; i++)
	{
        pread(fd, block_data, block_size_in_bytes, file_offset);
        file_offset = file_offset + block_size_in_bytes;

        // Since data is stored in row-major order, we are iterating in strides of col_count    
		for (int j=0; j < block_size_in_number_of_items; j=j+col_count*10)
        {
            // printf("J: %d\n", j);
            // printf("Col count: %d\n", col_count);
            // Step 4: create the entries of the dense index file
			index_buffer[2 * tuple_count + 0] = block_data[j];
			index_buffer[2 * tuple_count + 1] = tuple_count * col_count;
            // printf("IB %d %d\n", index_buffer[2 * tuple_count + 0], index_buffer[2 * tuple_count + 1]);
            tuple_count = tuple_count + 1;
		}
	}

    printf("Tuples: %d", tuple_count);

	close(fd);
	
    // Step 5: Write the index buffer to file (to be later opened while performing queries)
	int indexF =  open(sparse_index_filename, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH);
    write(indexF, index_buffer, index_buffer_size_in_bytes);
	close(indexF);

	free(index_buffer);
    free(block_data);
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

    printf("Data file name %s\n", data_filename);
    printf("Index file name %s\n", dense_index_filename);


    clock_t start_di = clock();
    create_dense_clustering_key();        
    clock_t end_di = clock();
    float seconds_di = (float)(end_di - start_di) / CLOCKS_PER_SEC;


    clock_t start_si = clock();
    create_sparse_clustering_key();        
    clock_t end_si = clock();
    float seconds_si = (float)(end_si - start_si) / CLOCKS_PER_SEC;

    printf("Time Taken to create Dense Index file, %s: %f \n", dense_index_filename, seconds_di);
    printf("Time Taken to create Sparse Index file, %s: %f \n", sparse_index_filename, seconds_si);

    free(data_filename);
    free(sparse_index_filename);
    free(dense_index_filename);

    return 0;
}
