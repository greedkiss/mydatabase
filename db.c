#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


typedef struct {
    char * buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_DUPLICATE_KEY,
} ExecuteResult;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

#define TABLE_MAX_PAGES 100

typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  void * pages[TABLE_MAX_PAGES];
}Pager;

typedef struct {
  Pager* pager;
  uint32_t root_page_num;
} Table;

typedef struct {
  Table * table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;
} Cursor;



Table * db_open(const char * filename){
  Pager * pager = pager_open(filename);

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

}

int main(int argc, char * argv[]){
  if(argc < 2){
    printf("Must supply a db filename\n");
    exit(EXIT_FAILURE);
  }

  char * filename = argv[1];
  Table* table = db_open(filename);


}