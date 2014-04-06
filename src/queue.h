#ifndef QUEUE_H
#define QUEUE_H

#include <stdlib.h>
#include <error.h>
#include <errno.h>
#include <kclangc.h>

struct Queue {
  int t;
};

int queue_open(struct Queue *q, const char *id, int idx);


#endif /* QUEUE_H */
