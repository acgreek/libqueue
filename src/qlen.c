#include <stdlib.h>
#include <queue.h>
#include "queueutils.h"

int main(int argc, char **argv) {
  struct Queue q;
  int64_t l = 0;
  if(queue_open(&q, QUEUEUTILS_QUEUE) != LIBQUEUE_SUCCESS) {
    puts("Failed to open the queue.");
    return EXIT_FAILURE;
  }
  if(queue_len(&q, &l) != LIBQUEUE_SUCCESS) {
    puts("Failed to get the queue length.");
    closequeue(&q);
    return EXIT_FAILURE;
  }
  printf("%d\n", l);
  return closequeue(&q);
}
