lib_LTLIBRARIES = libqueue.la
libqueue_la_SOURCES = queue.c
libqueue_la_CFLAGS = $(LIBKYOTOCABINET_CFLAGS) $(LIBXDGBASEDIR_CFLAGS)
libqueue_la_LDFLAGS = $(LIBKYOTOCABINET_LIBS) $(LIBXDGBASEDIR_LIBS)
include_HEADERS = queue.h

bin_PROGRAMS = qpush qpop
qpush_SOURCES = qpush.c
qpush_LDFLAGS = -lqueue
qpop_SOURCES = qpop.c
qpop_LDFLAGS= -lqueue
