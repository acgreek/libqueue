#include <stdlib.h>
#include <stdio.h>
#include "queue.h"
#include <libgen.h>
#include <string.h>

int main(int argc, char **argv) {
    struct Queue *q;
    int32_t ft = 42;
    struct QueueData qd = { "HENRY" , sizeof("HENRY") };
    struct QueueData qd2;
    puts(argv[0]);

    char template[] = "/tmp/qtest_XXXXXX";
    mkdtemp(template);

    if(NULL == (q=queue_open(template))) {
        puts("there was an error opening the queue!");
        return 1;
    }
    puts("queue successfully opened!");

    if(queue_push(q, &qd) != 0)
        puts("pushing HENRY failed!");

    if(queue_pop(q, &qd2) != 0) {
        puts("popping HENRY failed!");
    } else {
        puts("here's what popped!");
        puts((const char*)qd2.v);
        free(qd2.v);
    }

    qd.v = &ft;
    qd.vlen = sizeof(int32_t);
    queue_push(q, &qd);
    queue_pop(q, &qd2);
    printf("popping forty-two: %d\n", *(int32_t*)qd2.v);
    if (qd2.v)
        free(qd2.v);

    int i;
    char buffer[1024];
    for (i = 0; i < 15; i++) {
        qd2.v = buffer;
        qd2.vlen= sprintf(buffer,"%d",i);
        queue_push(q, &qd2);
    }
    for (i = 0; i < 15; i++) {
        queue_pop(q, &qd2);
        int len = sprintf(buffer,"%d",i);
        if (len != qd2.vlen || 0 != memcmp(buffer, qd2.v, qd2.vlen)) {
            printf("pop value not expected at index %d: %s\n", i, (char *)qd2.v);
        }
        if (qd2.v)
            free(qd2.v);
    }

    if(queue_close(q) != 0)
        puts("there was an error closing the queue!");
    puts("queue successfully closed!");
    //TODO lazy
    char cmdline[1024];
    sprintf(cmdline, "rm -rf %s", template);
    system(cmdline);
    return 0;
}
