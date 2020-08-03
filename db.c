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

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE+1];
  char email[COLUMN_EMAIL_SIZE+1];
}Row;

typedef enum {STATEMENT_INSERT, STATEMENT_SELECT} StatementType;

typedef struct {
  StatementType type;
  Row row_to_insert;
}Statement;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZE_STATEMENT
} PrepareResult;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0) ->Attribute);

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE  = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;


const uint32_t PAGE_SIZE = 4094U;
/*
 * Common Node Header Layout
 */
//节点类型，是否是根节点，父亲节点
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Internal Node Header Layout
 */
//中间节点关键字个数，中间节点右孩子
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Body Layout
 */
//
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
/* Keep this small for testing */
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

/*
 * Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;


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
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  int result = close(pager->file_descriptor);
  if(result == -1){
    printf("ERROR CLOSING DB\n");
    exit(EXIT_FAILURE);
  }
  for(uint32_t i = 0; i<TABLE_MAX_PAGES; i++){
    void * page = pager->pages[i];
    if(page){
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

NodeType get_node_type(void * node){
  uint8_t value = *((uint8_t *) (node+ NODE_TYPE_OFFSET));
  return (NodeType)value;
}

void indent(uint32_t level){
  for(uint32_t i = 0; i< level; i++){
    printf("  ");
  }
}

void * leaf_node_cell(void * node, uint32_t cell_num){
  return node+ LEAF_NODE_HEADER_SIZE+ cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num){
  return leaf_node_cell(node , cell_num);
}

uint32_t* internal_node_num_keys(void* node){
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child(void* node){
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

//中间节点最多为3个key
//返回
uint32_t* internal_node_cell(void* node , uint32_t cell_num){
  return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child(void * node , uint32_t child_num){
  uint32_t num_keys = *internal_node_num_keys(node);
  if(child_num > num_keys){
    printf("最多%d个孩子", num_keys);
    exit(EXIT_FAILURE);
  } else if(child_num == num_keys){
    return internal_node_right_child(node);
  } else {
    return internal_node_cell(node, child_num);
  }
}

uint32_t* internal_node_key(void* node, uint32_t key_num){
  return (void*) internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}
//
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level){
  void * node = get_page(pager, page_num);
  uint32_t num_keys, child;

  switch(get_node_type(node)){
    case(NODE_LEAF):
      num_keys = *leaf_node_num_cells(node);
      indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indentation_level+1);
        printf("- %d\n", *leaf_node_key(node, i));
      }
      break;
    case(NODE_INTERNAL):
      num_keys = *internal_node_num_keys(node);
      indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      for(uint32_t i = 0; i < num_keys ; i++){
        child = *internal_node_child(node, i);
        print_tree(pager, child, indentation_level+1);

        indent(indentation_level+1);
        printf(" - key %d\n", *internal_node_key(node, i));
      }
      child = *internal_node_right_child(node);
      print_tree(pager, child, indentation_level+1);
      break;
  }
}

void print_constants(){
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEAGER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

MetaCommandResult do_meta_command(InputBuffer * input_buffer, Table * table){
  if(strcmp(input_buffer->buffer, ".exit") == 0){
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  }else if(strcmp(input_buffer->buffer, ".btree") == 0){
    printf("tree:\n");
    print_tree(table->pager, 0, 0);
    return META_COMMAND_SUCCESS;
  }else if(strcmp(input_buffer->buffer, ".constants") == 0){
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  }else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

//主要是初始化statement
PrepareResult prepare_insert(InputBuffer * input_buffer, Statement* statement){
  statement->type = STATEMENT_INSERT;

  char * keyword = strtok(input_buffer->buffer, " ");
  char * id_string = strtok(NULL, " ");
  char * username = strtok(NULL, " ");
  char * email = strtok(NULL, " ");

  if(id_string == NULL || username == NULL || email == NULL){
    return PREPARE_SYNTAX_ERROR;
  }

  //可以替换为一个强转
  int id = atoi(id_string);
  if(id < 0){
    return  PREPARE_NEGATIVE_ID;
  }

  if(strlen(username) > COLUMN_USERNAME_SIZE){
    return PREPARE_STRING_TOO_LONG;
  }
  if(strlen(email) > COLUMN_EMAIL_SIZE){
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer * input_buffer, Statement* statement){
  if(strncmp(input_buffer->buffer, "insert", 6) == 0){
    return prepare_insert(input_buffer, statement);
  }
  if(strcmp(input_buffer->buffer, "select") == 0){
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZE_STATEMENT;
} 

//找到叶子节点后,节点信息必然在叶子结点上
//用二分法查找cell
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key){
  void* node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;
  cursor->end_of_table = false;

  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while(one_past_max_index != min_index){
    uint32_t index = (min_index + one_past_max_index)/2;
    uint32_t key_at_index = *leaf_node_key(node, index);
    if(key == key_at_index){
      cursor->cell_num = index;
      return cursor;
    }
    if(key < key_at_index){
      one_past_max_index = index;
    }else {
      min_index = index+1;
    }
  }

  cursor->cell_num = min_index;
  return cursor;
}

//返回子节点索引位置
uint32_t internal_node_find_child(void* node, uint32_t key){
  uint32_t num_keys = *internal_node_num_keys(node);

  uint32_t min_index = 0;
  uint32_t max_index = num_keys;
  
  while(min_index != max_index){
    uint32_t index = (min_index + max_index )/2;
    uint32_t key_to_right = *internal_node_key(node, index);
    if(key_to_right >= key){
      max_index = index;
    }else{
      min_index = index + 1;
    }
  }

  return min_index;
}

//node上child的信息是pager的堆地址索引
Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key){
  void* node = get_page(table->pager, page_num);

  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_num = *internal_node_child(node, child_index);
  void* child = get_page(table->pager, child_num);
  switch(get_node_type(child)){
    case NODE_LEAF:
      return leaf_node_find(table, child_num, key);
    case NODE_INTERNAL:
      return internal_node_find(table, child_num, key);
  }
}

Cursor* table_find(Table* table, uint32_t key){
  uint32_t root_page_num = table->root_page_num;
  void* root_node = get_page(table->pager, root_page_num);

  if(get_node_type(root_node) == NODE_LEAF){
    return leaf_node_find(table, root_page_num, key);
  }else{
    return internal_node_find(table, root_page_num, key);
  }
}

//返回最后面的key为最大的key
uint32_t get_node_max_key(void* node){
  switch(get_node_type(node)){
    case NODE_INTERNAL:
      return *internal_node_key(node, *internal_node_num_keys(node)-1);
    case NODE_LEAF:
      return *leaf_node_key(node, *leaf_node_num_cells(node)-1);
  }
}

uint32_t get_unused_page_num(Pager* pager){
  return pager->num_pages;
}

uint32_t* node_parent(void* node){
  return node + PARENT_POINTER_OFFSET;
}

void* leaf_node_value(void* node, uint32_t cell_num){
  return leaf_node_cell(node, cell_num)+ LEAF_NODE_KEY_SIZE;
}

//将Row中的信息拷贝到缓冲区pager中
void serilaize_row(Row* resource, void* destination){
  memcpy(destination+ ID_OFFSET, &resource->id, ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &resource->username, USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &resource->email, EMAIL_SIZE);
}

void initialize_internal_node(void * node){
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
}

void create_new_root(Table* table, uint32_t right_child_page_num){
  void* root = get_page(table->pager , table->root_page_num);
  void* right_child = get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  void* left_child = get_page(table->pager, left_child_page_num);

  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;
  uint32_t left_child_max_key = get_node_max_key(left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key){
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

//b数节点分裂
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value){
  void* old_node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t old_max = get_node_max_key(old_node);
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void* new_node = get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  for(int32_t i = LEAF_NODE_MAX_CELLS; i>=0; i--){
    void * destination_node;
    if(i>=LEAF_NODE_LEFT_SPLIT_COUNT){
      destination_node = new_node;
    }else{
      destination_node = old_node;
    }
    uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
    void* destination = leaf_node_cell(destination_node, index_within_node);

    //使cell顺序排列，如果cell_num正好找到则赋值
    //否则移动除该位置，类似于顺序排序的数组插入
    if(i == cursor->cell_num){
      serialize_row(value, leaf_node_value(destination_node, index_within_node));
      *leaf_node_key(destination_node, index_within_node) = key;
    }else if(i > cursor->cell_num){
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    }else{
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }

    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if(is_node_root(old_node)){
      return create_new_root(cursor->table, new_page_num);
    }else {
      uint32_t parent_page_num = *node_parent(old_node);
      uint32_t new_max = get_node_max_key(old_node);
      void * parent = get_page(cursor->table->pager, parent_page_num);

      update_internal_node_key(parent, old_max, new_max); 
    }





  }




}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value){
  void * node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  //分裂
  if(num_cells > LEAF_NODE_MAX_CELLS){
    leaf_node_split_and_insert(cursor, key, value);
  }
}

ExecuteResult execute_insert(Statement* statement, Table* table){
  Row* row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;
  Cursor* cursor = table_find(table, key_to_insert);

  void * node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  //插入的key已经存在
  if(cursor->cell_num < num_cells){
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    if(key_at_index == key_to_insert){
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
}

ExecuteResult execute_statement(Statement* statement , Table* table){
  switch(statement->type) {
    case(STATEMENT_INSERT):
      return execute_insert(statement, table);
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
        case(META_COMMAND_SUCCESS):
          continue;
        case(META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("unrecognized command '%s'\n", input_buffer->buffer);
          continue;
      }
    }

    Statement statement;
    switch(prepare_statement(input_buffer, &statement)) {
      case(PREPARE_SUCCESS):
        break;
      case(PREPARE_NEGATIVE_ID):
        printf("ID MUST BE POSITIVE\n");
        break;
      case(PREPARE_STRING_TOO_LONG):
        printf("string is too long\n");
        break;
      case(PREPARE_SYNTAX_ERROR):
        printf("syntax error\n");
        break;
      case(PREPARE_UNRECOGNIZE_STATEMENT):
        printf("unrecognized command at start of %s .\n", input_buffer->buffer);
        continue;
    }

    switch(execute_statement(&statement, table)){
      case(EXECUTE_SUCCESS):
        printf("executed.\n");
        break;
      case(EXECUTE_DUPLICATE_KEY):
        printf("error: duplicate key.\n");
        break;
    }

  }


}