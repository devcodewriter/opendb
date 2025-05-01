#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT, 
    STATEMENT_SELECT
} StatementType;

typedef struct
{
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef struct {
    StatementType type;
} Statement;


InputBuffer* newInputBuffer();
void printPrompt();
void readInputBuffer(InputBuffer* inputBuffer);
void closeInputBuffer(InputBuffer* inputBuffer);
MetaCommandResult doMetaCommand(InputBuffer* inputBuffer);
PrepareResult prepareStatement(InputBuffer* inputBuffer, Statement* statement);
void executeStatement(Statement* statement);

int main(int argc, char* argv[])
{
    InputBuffer* inputBuffer = newInputBuffer();
    while (true)
    {
        printPrompt();
        readInputBuffer(inputBuffer);
        // if (strcmp(inputBuffer->buffer, ".exit") == 0) {
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
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", inputBuffer->buffer);
                continue;
        }
        executeStatement(&statement);
        printf("Executed.\n");
        // }    
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
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (strncmp(inputBuffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void executeStatement(Statement* statement) {
    switch (statement->type)
    {
        case STATEMENT_INSERT:
            printf("This is where we would do an 'INSERT'.\n");
            break;
    
        case STATEMENT_SELECT:
            printf("This is where we would do an 'SELECT'.\n");
            break;
    }
}