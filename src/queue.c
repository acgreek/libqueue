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
  dbfpathlen = sizeof(char)*(strlen(ddir) +
      1 + // '/'
      strlen(QUEUE_DATADIR) +
      1 + // '/'
      strlen(id) +
      strlen(QUEUE_TUNINGSUFFIX) + 
      2);
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
  if(!kcdbset(q->db, (const char*)(&i), sizeof(int64_t),
        d->v, d->vlen))
    return LIBQUEUE_FAILURE;
  return LIBQUEUE_SUCCESS;
}
