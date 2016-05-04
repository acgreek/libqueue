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

int queue_open(struct Queue *q, const char *id) {
  assert(q != NULL);
  assert(id != NULL);

  xdgHandle xh;
  const char *ddir = NULL;
  char *dbfpath = NULL;
  char *qpath = NULL;
  size_t qpathlen = 0;
  size_t dbfpathlen = 0;

  /* locate database file */
  if(!xdgInitHandle(&xh))
    return LIBQUEUE_FAILURE;
  ddir = xdgDataHome(&xh);
  dbfpathlen = snprintf(NULL, 0, "%s/%s/%s%s",
      ddir, QUEUE_DATADIR, id, QUEUE_TUNINGSUFFIX);
  dbfpath = malloc(dbfpathlen);
  if(dbfpath==NULL) {
    xdgWipeHandle(&xh);
    return LIBQUEUE_MEM_ERROR;
  }
  snprintf(dbfpath, dbfpathlen, "%s/%s/%s%s",
      ddir, QUEUE_DATADIR, id, QUEUE_TUNINGSUFFIX);

  /* create config directory if necessary */
  qpathlen = sizeof(char)*(strlen(ddir)+2+strlen(QUEUE_DATADIR));
  qpath=malloc(qpathlen);
  if(qpath==NULL) {
    xdgWipeHandle(&xh);
    return LIBQUEUE_MEM_ERROR;
  }
  snprintf(qpath, qpathlen, "%s/%s", ddir, QUEUE_DATADIR);
  if(access((const char*)qpath, F_OK) != 0
      && xdgMakePath(qpath, S_IRWXU) != 0) {
      xdgWipeHandle(&xh);
      return LIBQUEUE_FAILURE;
  }

  /* open the database */
  q->db = kcdbnew();
  if(!kcdbopen(q->db, dbfpath, KCOWRITER | KCOCREATE))
    return LIBQUEUE_FAILURE;
  q->cur = kcdbcursor(q->db);

  /* free memory we've used */
  free(dbfpath);
  xdgWipeHandle(&xh);

  return LIBQUEUE_SUCCESS;
}

int queue_close(struct Queue *q) {
  assert(q != NULL);
  kccurdel(q->cur);
  if(!kcdbclose(q->db))
    return LIBQUEUE_FAILURE;
  return LIBQUEUE_SUCCESS;
}

int queue_push(struct Queue *q, struct QueueData *d) {
  assert(q != NULL);
  assert(d != NULL);
  assert(d->v != NULL);
  int64_t ni = kcdbcount(q->db);
  if(!kcdbadd(q->db,
        (const char*)(&ni),
        sizeof(int64_t), 
        (const char*)d->v,
        d->vlen))
    return LIBQUEUE_FAILURE;
  else
    return LIBQUEUE_SUCCESS;
}

int queue_pop(struct Queue *q, struct QueueData *d) {
  assert(q != NULL);
  assert(d != NULL);
  char *vbuf = NULL;
  size_t vbuflen = 0;
  int64_t i = kcdbcount(q->db)-1;
  if((vbuf = kcdbseize(q->db,
          (const char*)(&i),
          sizeof(int64_t),
          &vbuflen)) == NULL)
    return LIBQUEUE_FAILURE;
  d->v = malloc(vbuflen);
  if(d->v==NULL) {
    kcfree(vbuf);
    return LIBQUEUE_MEM_ERROR;
  }
  memcpy(d->v, vbuf, vbuflen);
  d->vlen = vbuflen;
  kcfree(vbuf);
  return LIBQUEUE_SUCCESS;
}

int queue_len(struct Queue *q, int64_t *lenbuf) {
  assert(lenbuf != NULL);
  *lenbuf = kcdbcount(q->db);
  return LIBQUEUE_SUCCESS;
}

int queue_peek(struct Queue *q, int64_t idx, struct QueueData *d) {
  assert(q != NULL);
  assert(d != NULL);
  char *vbuf = NULL;
  size_t vbuflen = 0;
  int64_t i = idx + 1;
  if(i > kcdbcount(q->db))
    return LIBQUEUE_FAILURE;
  if((vbuf = kcdbget(q->db,
          (const char*)(&i),
          sizeof(int64_t),
          &vbuflen)) == NULL)
    return LIBQUEUE_FAILURE;
  d->v = malloc(vbuflen);
  if(d->v==NULL) {
    kcfree(vbuf);
    return LIBQUEUE_MEM_ERROR;
  }
  memcpy(d->v, vbuf, vbuflen);
  d->vlen = vbuflen;
  kcfree(vbuf);
  return LIBQUEUE_SUCCESS;
}

int queue_poke(struct Queue *q, int64_t idx, struct QueueData *d) {
  assert(q != NULL);
  assert(d != NULL);
  assert(d->v != NULL);
  int64_t i = idx + 1;
  if(i > kcdbcount(q->db))
    return LIBQUEUE_FAILURE;
  if(!kcdbreplace(q->db, (const char*)(&i), sizeof(int64_t),
        d->v, d->vlen))
    return LIBQUEUE_FAILURE;
  return LIBQUEUE_SUCCESS;
}
