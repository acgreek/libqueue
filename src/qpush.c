#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <queue.h>

int main(int argc, char **argv) {
  struct Queue q;
  struct QueueData d;
  int i = 0;
  if(queue_open(&q, "queueutils-stack0") != LIBQUEUE_SUCCESS) {
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
  return 0;
}
