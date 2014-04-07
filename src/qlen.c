#include <stdlib.h>
#include <queue.h>
#include "queueutils.h"

int main(int argc, char **argv) {
  struct Queue q;
  int64_t l = 0;
  int opt = 0;
  char *cq = NULL;

  while((opt = getopt(argc, argv, "hq:")) != -1)
    switch(opt) {
      case 'q':
        cq = strdup(optarg);
        break;
      default:
        puts("Usage: qlen [-h] [-q queue-name] [--]");
        return EXIT_FAILURE;
    }

  if(queue_open(&q, SELECTQUEUE(cq)) != LIBQUEUE_SUCCESS) {
    puts("Failed to open the queue.");
    return EXIT_FAILURE;
  }
  if(queue_len(&q, &l) != LIBQUEUE_SUCCESS) {
    puts("Failed to get the queue length.");
    closequeue(&q);
    return EXIT_FAILURE;
  }
  printf("%d\n", l);
  if(cq != NULL)
    free(cq);
  return closequeue(&q);
}
