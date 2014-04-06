#ifndef QUEUE_H
#define QUEUE_H

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <error.h>
#include <errno.h>
#include <kclangc.h>
#include <basedir.h>
#include <libgen.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#define QUEUE_DATADIR ("libqueue")
#define QUEUE_TUNINGSUFFIX "#type=kct#zcomp=gz#opts=cs#apow=0#bnum=30"

enum {
  LIBQUEUE_FAILURE = -1,
  LIBQUEUE_SUCCESS = 0,
  LIBQUEUE_MEM_ERROR = -2
};

struct Queue {
  KCDB *db;
  KCCUR *cur;
};

struct QueueData {
  void *v;
  size_t vlen;
};

int queue_open(struct Queue *q, const char *id);
int queue_push(struct Queue *q, struct QueueData *d);
int queue_pop(struct Queue *q, struct QueueData *d);
int queue_len(struct Queue *q, int64_t *len);
int queue_peek(struct Queue *q, int64_t s, struct QueueData *d);
int queue_poke(struct Queue *q, int64_t s, struct QueueData *d);
int queue_close(struct Queue *q);

#endif /* QUEUE_H */
