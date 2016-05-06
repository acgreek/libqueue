/*
 * libqueue - provides persistent, named data storage queues
 * Copyright (C) 2014-2016 Jens Oliver John <dev@2ion.de>
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

#include "queue.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

static int  updateStateOnDisk(struct Queue * q) {
    lseek(q->stateFd, 0, SEEK_SET);
    if (sizeof (struct QueueStateFileStruct) != write (q->stateFd,&q->state,sizeof (struct QueueStateFileStruct)  ))
       return -1;
    fdatasync(q->stateFd);
    return 0;
}

struct Queue * queue_open(const char *path) {
    assert(path != NULL);
    if (0 != access (path, R_OK| W_OK))
        return NULL;
    struct Queue * q = malloc (sizeof (struct Queue));
    if (NULL == q)
        return NULL;
    memset (q, 0 , sizeof (struct Queue));
    q->path = strdup (path);
    char statefilepath[2048];
    snprintf(statefilepath, sizeof(statefilepath),"%s/queue.state", q->path);
    q->stateFd = open (statefilepath,O_CREAT| O_RDWR, S_IRUSR| S_IWUSR );
    if (q->stateFd == -1) {
        free(q);
        return NULL;
    }
    if (sizeof (struct QueueStateFileStruct) != read(q->stateFd, &q->state,sizeof (struct QueueStateFileStruct) ) ) {
        memset (&q->state, 0 , sizeof (struct QueueStateFileStruct));
        updateStateOnDisk(q);
    }
    if ('\0' != q->state.writeFile[0]) {
        snprintf(statefilepath, sizeof(statefilepath),"%s/%s", q->path, q->state.writeFile);
        q->writeFd = open (statefilepath,O_CREAT| O_RDWR, S_IRUSR| S_IWUSR );
        lseek(q->writeFd,q->state.writeOffset , SEEK_CUR);
    }
    return q;
}

int queue_close(struct Queue *q) {
  assert(q != NULL);
  if (q->stateFd)
      close(q->stateFd);
  if (q->writeFd)
      close(q->writeFd);
  if (q->path)
      free(q->path);
  free(q);
  return LIBQUEUE_SUCCESS;
}

int queue_push(struct Queue *q, struct QueueData *d) {
    assert(q != NULL);
    assert(d != NULL);
    assert(d->v != NULL);
    //  return LIBQUEUE_FAILURE;
    if ('\0' == q->state.writeFile[0]) {
        if (q->writeFd)
            close(q->writeFd);
        snprintf(q->state.writeFile, sizeof(q->state.writeFile)-1, "queue.%zu", time(NULL));
        char statefilepath[2048];
        snprintf(statefilepath, sizeof(statefilepath),"%s/%s", q->path, q->state.writeFile);
        q->writeFd = open (statefilepath,O_CREAT| O_RDWR, S_IRUSR| S_IWUSR );
        if (q->writeFd == 0)
            return LIBQUEUE_FAILURE;
    }

    size_t cur = lseek(q->writeFd,0, SEEK_CUR);

    if (sizeof(u_int64_t) != write (q->writeFd, &d->vlen, sizeof(u_int64_t))) {
        lseek(q->writeFd, cur, SEEK_SET);
        return LIBQUEUE_FAILURE;
    }
    if (d->vlen != write (q->writeFd, d->v, d->vlen)) {
        lseek(q->writeFd, cur, SEEK_SET);
        return LIBQUEUE_FAILURE;
    }
    fdatasync(q->writeFd);
    cur = lseek(q->writeFd,0, SEEK_CUR);
    q->state.writeOffset = cur;
    updateStateOnDisk(q);
    return LIBQUEUE_SUCCESS;
}

int queue_pop(struct Queue *q, struct QueueData *d) {
  assert(q != NULL);
  assert(d != NULL);
  return LIBQUEUE_SUCCESS;
}

int queue_len(struct Queue *q, int64_t *lenbuf) {
  assert(lenbuf != NULL);
  return LIBQUEUE_SUCCESS;
}

int queue_peek(struct Queue *q, int64_t idx, struct QueueData *d) {
  assert(q != NULL);
  assert(d != NULL);
  return LIBQUEUE_SUCCESS;
}

int queue_poke(struct Queue *q, int64_t idx, struct QueueData *d) {
  assert(q != NULL);
  assert(d != NULL);
  assert(d->v != NULL);
  return LIBQUEUE_SUCCESS;
}
