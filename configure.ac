AC_INIT([libqueue], [0.9], [dev@2ion.de])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AM_PROG_AR
LT_INIT
AC_PROG_CC
AC_CONFIG_SRCDIR([src/queue.c])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile src/Makefile])
PKG_CHECK_MODULES([LIBKYOTOCABINET], [kyotocabinet])
PKG_CHECK_MODULES([LIBXDGBASEDIR], [libxdg-basedir])
AC_OUTPUT
