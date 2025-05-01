#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#define COLUMN_USERNAME_SIZE    32
#define COLUMN_EMAIL_SIZE       255

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

#define SIZE_OF_ATTRIBUTE(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES         100

const uint32_t ID_SIZE          =   SIZE_OF_ATTRIBUTE(Row, id);
const uint32_t USERNAME_SIZE    =   SIZE_OF_ATTRIBUTE(Row, username);
const uint32_t EMAIL_SIZE       =   SIZE_OF_ATTRIBUTE(Row, email);
const uint32_t ID_OFFSET        =   0;
const uint32_t USERNAME_OFFSET  =   ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET     =   USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE         =   ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE        =   4096;
const uint32_t ROWS_PER_PAGE    =   PAGE_SIZE   /   ROW_SIZE;
const uint32_t TABLE_MAX_ROWS   =   ROWS_PER_PAGE   *   TABLE_MAX_PAGES;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT, 
    STATEMENT_SELECT
} StatementType;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct
{
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef struct {
    StatementType type;
    Row rowToInsert;
} Statement;

typedef struct {
    uint32_t numRows;
    void* pages[TABLE_MAX_PAGES];
} Table;

InputBuffer* newInputBuffer();
void printPrompt();
void readInputBuffer(InputBuffer* inputBuffer);
void closeInputBuffer(InputBuffer* inputBuffer);
MetaCommandResult doMetaCommand(InputBuffer* inputBuffer);
PrepareResult prepareStatement(InputBuffer* inputBuffer, Statement* statement);
ExecuteResult executeInsert(Statement* statement, Table* table);
ExecuteResult executeSelect(Statement* statement, Table* table);
ExecuteResult executeStatement(Statement* statement, Table* table);
void serializeRow(Row* source, void* destination);
void deserializeRow(void* source, Row* destination);
void* rowSlot(Table* table, uint32_t rowNum);
void printRow(Row* row);
Table* newTable();
void freeTable(Table* table);

int main(int argc, char* argv[])
{
    InputBuffer* inputBuffer = newInputBuffer();
    Table* table = newTable();
    while (true)
    {
        printPrompt();
        readInputBuffer(inputBuffer);
        if (inputBuffer->buffer[0] == '.') {
            switch (doMetaCommand(inputBuffer))
            {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", inputBuffer->buffer);
                    continue;
            }
        }
        Statement statement;
        switch(prepareStatement(inputBuffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", inputBuffer->buffer);
                continue;
        }
        switch (executeStatement(&statement, table))
        {
            case EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;    
            case EXECUTE_TABLE_FULL:
                printf("Error: Table full.\n");
                break;
        }   
    }
    
    return 0;
}

InputBuffer* newInputBuffer() {
    InputBuffer* inputBuffer = malloc(sizeof(inputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->buffer_length = 0;
    inputBuffer->input_length = 0;
    return inputBuffer;
}

void printPrompt() { printf("opendb > "); }

void readInputBuffer(InputBuffer* inputBuffer) {
    size_t bytesRead = (size_t)getline(&(inputBuffer->buffer), &(inputBuffer->buffer_length), stdin);
    if (bytesRead <= 0)
    {
        printf("Error while reading input \n");
        exit(EXIT_FAILURE);
    }
    // Ignoring trailing new line
    inputBuffer->input_length = bytesRead - 1;
    inputBuffer->buffer[bytesRead - 1] = 0;
}

void closeInputBuffer(InputBuffer* inputBuffer) {
    free(inputBuffer->buffer);
    free(inputBuffer);
}

MetaCommandResult doMetaCommand(InputBuffer* inputBuffer) {
    if (strcmp(inputBuffer->buffer, ".exit") == 0)
    {
        exit(EXIT_SUCCESS);
    }else {
       return META_COMMAND_UNRECOGNIZED_COMMAND;
    }  
}

PrepareResult prepareStatement(InputBuffer* inputBuffer, Statement* statement) {
    if (strncmp(inputBuffer->buffer, "INSERT", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        int argsAssigned = sscanf(
            inputBuffer->buffer,
            "INSERT %d %s %s",
            &(statement->rowToInsert.id),
            statement->rowToInsert.username,
            statement->rowToInsert.email
        );
        if(argsAssigned < 3) {
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if (strncmp(inputBuffer->buffer, "SELECT", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult executeInsert(Statement* statement, Table* table) {
    if(table->numRows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Row* rowToInsert = &(statement->rowToInsert);
    serializeRow(rowToInsert, rowSlot(table, table->numRows));
    table->numRows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement, Table* table) {
    Row row;
    for (uint32_t i = 0; i < table->numRows; i++) {
        deserializeRow(rowSlot(table, i), &row);
        printRow(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult executeStatement(Statement* statement, Table* table) {
    switch (statement->type)
    {
        case STATEMENT_INSERT:
            return executeInsert(statement, table);
        case STATEMENT_SELECT:
            return executeSelect(statement, table);
    }
}

void serializeRow(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserializeRow(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* rowSlot(Table* table, uint32_t rowNum) {
    uint32_t pageNum = rowNum / ROWS_PER_PAGE;
    void* page = table->pages[pageNum];
    if(page == NULL) {
        // Allocate memory only when we try to access page;
        page = table->pages[pageNum] = malloc(PAGE_SIZE);
    }
    uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return page + byteOffset;
}

void printRow(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

Table* newTable() {
    Table* table = (Table*)malloc(sizeof(Table));
    table->numRows = 0;
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        table->pages[i] = NULL;
    }
    return table;
}

void freeTable(Table* table) {
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        free(table->pages[i]);
    }
    free(table);
}