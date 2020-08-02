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

typedef enum {NODE_INTERNAL, NODE_LEAF}  NodeType;

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

const uint32_t PAGE_SIZE = 4094U;
//节点类型,中间节点，叶子节点
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_OFFSET;

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);


//节点类型＋是否根节点＋父亲指针大小
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;

//node头大小＋leaf_node_num_cells_size
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;


//pager以文件为存储方式
//以页为基本单位存储数据
Pager * pager_open(const char * filename){
  int fd = open(filename, O_RDWR|O_CREAT, S_IWUSR|S_IRUSR);

  if(fd == -1){
    printf("unable to open the file\n");
    exit(EXIT_FAILURE);
  }
  
  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = file_length/PAGE_SIZE;

  if(file_length / PAGE_SIZE != 0){
    printf("必须是4096整数倍\n");
    exit(EXIT_FAILURE);
  }

  for(uint32_t i = 0; i< TABLE_MAX_PAGES; i++){
    pager->pages[i] = NULL;
  }

  return pager;
}

//返回的pager->page[]数组存的是堆地址初始值
//如果该页不存在则申请4096空间，否则直接返回
void * get_page(Pager* pager, uint32_t page_num){
  if(page_num > TABLE_MAX_PAGES){
    printf("最多100个page\n");
    exit(EXIT_FAILURE);
  }

  if(pager->pages[page_num] == NULL){
    void * page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    if(pager->file_length % PAGE_SIZE){
      num_pages += 1;
    }

    if(page_num < num_pages){
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if(bytes_read == -1){
        printf("读文件错误\n");
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[num_pages] = page;

    if(page_num > pager->num_pages){
      pager->num_pages = page_num + 1;
    }

    return pager->pages[num_pages];
  }
}

//Page堆空间开始８字节为节点类型
//标为中间节点或者叶子节点
void set_node_type(void * node, NodeType type){
  uint8_t value = type;
  *((uint8_t *) (node + NODE_TYPE_OFFSET)) = value;
}

//表明该页是不是根节点
void set_node_root(void * node, bool is_root){
  uint8_t value = is_root;
  *((uint8_t *) (node + IS_ROOT_OFFSET)) = value;
}

uint32_t* leaf_node_num_cells(void * node){
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t * leaf_node_next_leaf(void * node){
  return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

//node是堆起始地址值
void initialize_leaf_node(void * node){
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;
}

//实例化table和pager
//如果db为空则顺便设置根节点
//db结构为b-树
Table * db_open(const char * filename){
  Pager * pager = pager_open(filename);

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

  //如果pager中没有数据
  //该page为根节点
  if(pager->num_pages == 0){
    void * root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }

  return table;
}

InputBuffer * new_input_buffer() {
  InputBuffer * input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void print_prompt(){ printf("db > ");}

//getline获取输入行
//here could be hacked 
void read_input(InputBuffer* input_buffer){
  ssize_t bytes_read = 
    getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
  
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

//释放堆空间
void close_input_buffer(InputBuffer* input_buffer){
  free(input_buffer->buffer);
  free(input_buffer);
}

//把堆上的数据写到文件中
void pager_flush(Pager* pager, uint32_t page_num){
  if(pager->pages[page_num] == NULL){
    printf("flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if(offset == -1){
    printf("seek error\n");
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_descriptor , pager->pages[page_num], PAGE_SIZE);

  if(bytes_written == -1){
    printf("flush error\n");
    exit(EXIT_FAILURE);
  }
}

void db_close(Table* table){
  Pager* pager = table->pager;

  for(uint32_t i = 0; i< pager->num_pages; i++){
    if(pager->pages[i] == NULL){
      continue;
    }
    pager_flush(pager, i);
  }
}

MetaCommandResult do_meta_command(InputBuffer * input_buffer, Table * table){
  if(strcmp(input_buffer->buffer, ".exit") == 0){
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  }
}

int main(int argc, char * argv[]){
  if(argc < 2){
    printf("Must supply a db filename\n");
    exit(EXIT_FAILURE);
  }

  char * filename = argv[1];
  Table* table = db_open(filename);

  InputBuffer * input_buffer = new_input_buffer();
  while(true){
    print_prompt();
    read_input(input_buffer);

    if(input_buffer->buffer[0] == '.'){
      switch(do_meta_command(input_buffer, table)) {

      }
    }


  }


}