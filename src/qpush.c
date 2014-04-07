#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <getopt.h>
#include <queue.h>
#include "queueutils.h"

int main(int argc, char **argv) {
  struct Queue q;
  struct QueueData d;
  int i = 0;
  char *cq = NULL;
  int opt = 0;

  while((opt = getopt(argc, argv, "hq:")) != -1)
    switch(opt) {
      case 'q':
        cq = strdup(optarg);
        break;
      default:
      case 'h':
        puts("Usage: qpush [-h] [-q queue-name] [--] <args>");
        return EXIT_FAILURE;
    }

  i = optind-1;

  if(queue_open(&q, SELECTQUEUE(cq)) != LIBQUEUE_SUCCESS) {
    puts("Failed to open the queue.");
    return EXIT_FAILURE;
  }
  while(argv[++i]) {
    d.v = argv[i];
    d.vlen = sizeof(char)*(strlen(argv[i])+1);
    if(queue_push(&q, &d) != LIBQUEUE_SUCCESS) {
      fputs("Failed to push value onto the queue: ", stdout);
      puts(argv[i]);
    }
  }
  if(queue_close(&q) != LIBQUEUE_SUCCESS)
    puts("Failed to close the queue");
  if(cq != NULL)
    free(cq);
  return EXIT_SUCCESS;
}
