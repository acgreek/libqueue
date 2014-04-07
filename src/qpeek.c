/*
 * libqueue - provides persistent, named data storage queues
 * Copyright (C) 2014 Jens Oliver John <dev@2ion.de>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * */

#include <stdio.h>
#include <stdlib.h>
#include <queue.h>
#include "queueutils.h"

int main(int argc, char **argv) {
  struct Queue q;
  struct QueueData d;
  int64_t l = 0;
  int i = 0;
  int64_t j = 0;
  char *cq = NULL;
  int opt = 0;

  while((opt = getopt(argc, argv, "hq:")) != -1)
    switch(opt) {
      case 'q':
        cq = strdup(optarg);
        break;
      default:
      case 'h':
        puts("Usage: qpeek [-h] [-q queue-name] [--] <args>");
        return EXIT_FAILURE;
    }

  i = optind-1;

  if(queue_open(&q, SELECTQUEUE(cq)) != LIBQUEUE_SUCCESS) {
    puts("Failed to open the queue.");
    return EXIT_FAILURE;
  }
  if(queue_len(&q, &l) != LIBQUEUE_SUCCESS) {
    puts("Failed to read the queue length.");
    closequeue(&q);
    return EXIT_FAILURE;
  }
  while(argv[++i]) {
    if((j = (int64_t)atoi((const char*)argv[i])) < 0
        || (j+1)>l) {
      printf("Index out of bounds: %d (%d)\n", j, l);
      continue;
    }
    if(queue_peek(&q, j-1, &d) != LIBQUEUE_SUCCESS) {
      printf("Failed to peek at element #%d\n", j);
      continue;
    }
    printf("@%d: %s\n", j, (const char*)d.v);
    free(d.v);
  }
  if(cq != NULL)
    free(cq);
  return closequeue(&q);
}
