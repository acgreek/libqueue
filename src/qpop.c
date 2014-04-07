#include <stdio.h>
#include <stdlib.h>
#include <queue.h>

#include "queueutils.h"

int main(int argc, char **argv) {
  struct Queue q;
  struct QueueData d;
  int64_t l = 0;

  if(queue_open(&q, "queueutils-stack0") != LIBQUEUE_SUCCESS) {
    puts("Failed to open the queue.");
    return EXIT_FAILURE;
  }
  if(queue_len(&q, &l) != LIBQUEUE_SUCCESS) {
    puts("Failed to retrieve the queue length.");
    closequeue(&q);
    return EXIT_FAILURE;
  }
  if(l == 0) {
    closequeue(&q);
    return EXIT_FAILURE;
  }
  if(queue_pop(&q, &d) != LIBQUEUE_SUCCESS){
    puts("Failed to retrieve the value from the queue.");
    closequeue(&q);
    return EXIT_FAILURE;
  }
  printf("%s\n", (const char*)d.v);
  free(d.v);
  return closequeue(&q);
}
