#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

typedef struct
{
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

InputBuffer* newInputBuffer();
void printPrompt();
void readInputBuffer(InputBuffer* inputBuffer);
void closeInputBuffer(InputBuffer* inputBuffer);

int main(int argc, char* argv[])
{
    InputBuffer* inputBuffer = newInputBuffer();
    while (true)
    {
        printPrompt();
        readInputBuffer(inputBuffer);
        if (strcmp(inputBuffer->buffer, ".exit") == 0)
        {
            closeInputBuffer(inputBuffer);
            exit(EXIT_SUCCESS);
        }else
        {
            printf("Unrecognized command '%s'\n", inputBuffer->buffer);
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