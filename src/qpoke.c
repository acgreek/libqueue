#include <stdio.h>
#include <stdlib.h>
#include <queue.h>
#include "queueutils.h"

int main(int argc, char **argv) {
  struct Queue q;
  struct QueueData d;
  int64_t l = 0;
  int64_t i = 0;

  if(argc != 3) {
    puts("Too few arguments. Usage: qpoke <index> <value>");
    return EXIT_FAILURE;
  }
  if(queue_open(&q, QUEUEUTILS_QUEUE) != LIBQUEUE_SUCCESS) {
    puts("Failed to open the queue.");
    return EXIT_FAILURE;
  }
  if(queue_len(&q, &l) != LIBQUEUE_SUCCESS) {
    puts("Failed to read the queue length.");
    closequeue(&q);
    return EXIT_FAILURE;
  }
  if((i = (int64_t)atoi((const char*)argv[i])) < 0
      || (i+1)>l) {
    puts("Index value is out of bounds.");
    closequeue(&q);
    return EXIT_FAILURE;
  }
  d.v = argv[2];
  d.vlen = sizeof(char)*(strlen(argv[2])+1);
  if(queue_poke(&q, i-1, &d) != LIBQUEUE_SUCCESS) {
    puts("Failed to poke.");
    closequeue(&q);
    return EXIT_FAILURE;
  }
  return closequeue(&q);
}
