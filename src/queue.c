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
#include <sys/file.h>
#include <leveldb/c.h>

#include <fcntl.h>

struct Queue {
    leveldb_t * db;
    leveldb_iterator_t* readItr;
    leveldb_iterator_t* writeItr;
     leveldb_readoptions_t* rop;
     leveldb_writeoptions_t* wop;
};

struct Queue * queue_open(const char *path) {
    char * errptr=NULL;
    leveldb_options_t* options = leveldb_options_create();
    leveldb_options_set_create_if_missing(options, 1);
    leveldb_t * db = leveldb_open(options, path, &errptr);
    if (db) {
        struct Queue * q = malloc(sizeof (struct Queue));
        memset(q, 0, sizeof(struct Queue));
        q->db = db;
        q->rop = leveldb_readoptions_create();
        q->wop = leveldb_writeoptions_create();
        return q;
    }
    free(errptr);


    return NULL;
}

int queue_close(struct Queue *q) {
    assert(q != NULL);
    if (q->readItr)
        leveldb_iter_destroy(q->readItr);
    if (q->writeItr)
        leveldb_iter_destroy(q->readItr);
    q->writeItr= NULL;
    q->readItr= NULL;
    if (q->db)
        leveldb_close(q->db);
    q->db= NULL;
    return LIBQUEUE_SUCCESS;
}

int queue_push(struct Queue *q, struct QueueData *d) {
    assert(q != NULL);
    assert(d != NULL);
    assert(d->v != NULL);
    if (NULL == q->writeItr)
        q->readItr= leveldb_create_iterator(q->db,q->rop);
    else {
        leveldb_iter_seek_to_last(q->readItr);
    }
    char key[1024];
    size_t klen;
    if (0 == leveldb_iter_valid(q->readItr)) {
        klen = snprintf(key, sizeof(key)-1 , "%d", 0);
    }
    else  {
        char * lkey= NULL;

        lkey = (char *)leveldb_iter_key(q->readItr, &klen);
        size_t cl= strtol(lkey, NULL, 10);
        klen = snprintf(key, sizeof(key)-1 , "%zd", cl+1);
    }
    char * errptr = NULL;
    leveldb_put(q->db, q->wop,key, klen,d->v, d->vlen, &errptr);
    return LIBQUEUE_SUCCESS;
}

int queue_pop(struct Queue *q, struct QueueData *d) {
    assert(q != NULL);
    assert(d != NULL);
    if (NULL == q->readItr )
        q->readItr= leveldb_create_iterator(q->db,q->rop);
    else {
        leveldb_iter_seek_to_first(q->readItr);
    }
    if (0 == leveldb_iter_valid(q->readItr)) {
        return LIBQUEUE_FAILURE;
    }
    d->v = (char *)leveldb_iter_value(q->readItr, &d->vlen);
    if (d->v) {
        char * tmp = malloc (d->vlen);
        memcpy(tmp, d->v, d->vlen);
        d->v = tmp;
    }
    size_t klen = 0;
    char * key= NULL;
    key = (char *)leveldb_iter_key(q->readItr, &klen);
    leveldb_iter_next(q->readItr);
    char * errptr=NULL;
    leveldb_delete(q->db,  q->wop,key, klen, &errptr);
    return LIBQUEUE_SUCCESS;
}

int queue_len(struct Queue *q, int64_t *lenbuf) {
    assert(lenbuf != NULL);
    if (NULL == q->readItr) {
        q->readItr= leveldb_create_iterator(q->db,q->rop);
    }
    leveldb_iter_seek_to_first(q->readItr);
    if (0 == leveldb_iter_valid(q->readItr)) {
        *lenbuf = 0;
    }
    // TODO, figure out fast way to get size
    *lenbuf= 10;
    return LIBQUEUE_SUCCESS;
}

int queue_peek(struct Queue *q, int64_t idx, struct QueueData *d) {
    assert(q != NULL);
    assert(d != NULL);
    if (NULL == q->readItr )
        q->readItr= leveldb_create_iterator(q->db,q->rop);
    else {
        leveldb_iter_seek_to_first(q->readItr);
    }
    if (0 == leveldb_iter_valid(q->readItr)) {
        return LIBQUEUE_FAILURE;
    }
    d->v = (char *)leveldb_iter_value(q->readItr, &d->vlen);
    return LIBQUEUE_SUCCESS;
}

int queue_poke(struct Queue *q, int64_t idx, struct QueueData *d) {
    assert(q != NULL);
    assert(d != NULL);
    assert(d->v != NULL);
    return LIBQUEUE_SUCCESS;
}
