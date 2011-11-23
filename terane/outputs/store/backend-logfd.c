/*
 * Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Terane.
 *
 * Terane is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Terane is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "backend.h"

static int logfd[2] = { -1, -1 };
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * terane_log_fd: return the fd where log messages are written to.
 */
PyObject *
terane_Module_log_fd (PyObject *self)
{
    int fd;

    pthread_mutex_lock (&lock);
    if (logfd[0] < 0)
        pipe(logfd);
    fd = logfd[0];
    pthread_mutex_unlock (&lock);

    return PyInt_FromLong ((long) fd);
}

/*
 * terane_log_msg: write a message to the log fd.
 */
void
terane_log_msg (int level, const char *logger, const char *fmt, ...)
{
    va_list ap;
    char buffer[8192];
    int nwritten, ret;

    va_start (ap, fmt);

    pthread_mutex_lock (&lock);

    nwritten = snprintf (buffer, 8192, "%i %s ", level, logger);
    ret = write (logfd[1], buffer, nwritten);
    nwritten = vsnprintf (buffer, 8192, fmt, ap);
    ret = write (logfd[1], buffer, nwritten);
    ret = write (logfd[1], "\n", 1);

    pthread_mutex_unlock (&lock);

    va_end (ap);
}
