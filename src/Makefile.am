lib_LTLIBRARIES = libqueue.la
libqueue_la_SOURCES = queue.c
libqueue_la_CFLAGS = $(LIBKYOTOCABINET_CFLAGS) $(LIBXDGBASEDIR_CFLAGS)
libqueue_la_LDFLAGS = $(LIBKYOTOCABINET_LIBS) $(LIBXDGBASEDIR_LIBS)
include_HEADERS = queue.h

bin_PROGRAMS = qpush qpop qlen qpeek qpoke

qpush_SOURCES = qpush.c queueutils.c queueutils.h
qpush_LDFLAGS = -lqueue

qpop_SOURCES = qpop.c queueutils.c queueutils.h
qpop_LDFLAGS= -lqueue

qlen_SOURCES = qlen.c queueutils.c queueutils.h
qlen_LDFLAGS = -lqueue

qpeek_SOURCES = qpeek.c queueutils.c queueutils.h
qpeek_LDFLAGS = -lqueue

qpoke_SOURCES = qpoke.c queueutils.c queueutils.h
qpoke_LDFLAGS = -lqueue
