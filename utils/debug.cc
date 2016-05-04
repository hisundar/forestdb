#include "debug.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "time_utils.h"
#if !defined(WIN32) && !defined(_WIN32)
#include <sys/wait.h>
#include <unistd.h>
#include <execinfo.h>
#endif // !defined(WIN32) && !defined(_WIN32)

#include "fdb_internal.h"

#define N_DBG_SWITCH (256)

static uint8_t _global_dbg_switch[N_DBG_SWITCH];
static void* _global_dbg_addr[N_DBG_SWITCH];
static uint64_t _global_dbg_uint64_t[N_DBG_SWITCH];

fdb_fatal_error_callback fatal_error_callback = NULL;

// LCOV_EXCL_START
void _dbg_sw_set(int n)
{
    _global_dbg_switch[n] = 1;
}

void _dbg_sw_clear(int n)
{
    _global_dbg_switch[n] = 0;
}

void _dbg_set_addr(int n, void *addr)
{
    _global_dbg_addr[n] = addr;
}

void * _dbg_get_addr(int n)
{
    return _global_dbg_addr[n];
}

void _dbg_set_uint64_t(int n, uint64_t val)
{
    _global_dbg_uint64_t[n] = val;
}

uint64_t _dbg_get_uint64_t(int n)
{
    return _global_dbg_uint64_t[n];
}

int _dbg_is_sw_set(int n)
{
    return _global_dbg_switch[n];
}

void dump_backtraces() {
#ifdef __linux__
    char my_pid[16];
    char proc_name[512];
    sprintf(my_pid, "--pid=%d", getpid());
    proc_name[readlink("/proc/self/exe", proc_name, 511)] = '\0';
    printf("Dumping backtrace for %s pid=%s..\n", proc_name, my_pid);
    int gdb_pid = fork();
    if (!gdb_pid) { // child process...
        printf("Running gdb backtrace for %s pid=%s\n", proc_name, my_pid);
        execlp("gdb", "gdb", "--batch", "-n",
               "-ex", "thread apply all bt 8",
               proc_name, my_pid, NULL);
        abort(); /* If gdb failed to start */
    } else {
        int ret_stat;
        waitpid(gdb_pid, &ret_stat, 0);
        if (!ret_stat) {
            fprintf(stderr, "\ngdb dumping successful\n");
        } else {
            fprintf(stderr, "\ngdb backtracing incomplete\n");
        }
    }
#elif !defined(WIN32) && !defined(_WIN32)
    void *callstack[10];
    char **backtrace_buf;
    int frames = backtrace(callstack, 10);
    backtrace_buf = backtrace_symbols(callstack, frames);
    if (backtrace_buf) {
        for (int i = 0; i < frames; ++i) {
            fprintf(stderr, "%d : %s\n", i, backtrace_buf[i]);
        } // (no need to free memory as process is crashing)
    }
#endif // __linux__ or WIN32 || _WIN32
}

void _dbg_assert(int line, const char *file, uint64_t val, uint64_t expected) {
    char *hang_process;
    fprintf(stderr, "Assertion in %p != %p in %s:%d\n",
           (void *)val, (void *)expected, file, line);

    // Invoke the fatal error callback if registered.
    if (fatal_error_callback != NULL) {
        fatal_error_callback();
    }

    hang_process = getenv("HANG_ON_ASSERTION");
    if (hang_process) {
        fprintf(stderr, "Hanging process...");
        fprintf(stderr, "\n");
        while (1) {
            usleep(1000);
        }
    }
}

void dbg_print_buf(void *buf, uint64_t buflen, bool hex, int align)
{
    if (buf) {
        if (!hex) {
            // plaintext
            fprintf(stderr, "%.*s\n", (int)buflen, (char*)buf);
        } else {
            // hex dump
            size_t i, j;
            fprintf(stderr, "(hex) 0x%" _X64 ", %" _F64 " (0x%" _X64 ") bytes\n",
                    (uint64_t)buf, buflen, buflen);
            for (i=0;i<buflen;i+=align) {
                fprintf(stderr, "   %04x   ", (int)i);
                for (j=i; j<i+align; ++j){
                    if (j<buflen) {
                        fprintf(stderr, "%02x ", ((uint8_t*)buf)[j]);
                    } else {
                        fprintf(stderr, "   ");
                    }
                    if ((j+1)%8 == 0) {
                        fprintf(stderr, " ");
                    }
                }
                fprintf(stderr, " ");
                for (j=i; j<i+align && j<buflen; ++j){
                    // print only readable ascii character
                    fprintf(stderr, "%c",
                     (0x20 <= ((char*)buf)[j] && ((char*)buf)[j] <= 0x7d)?
                               ((char*)buf)[j] : '.'  );
                }
                fprintf(stderr, "\n");
            }
        }
    } else {
        fprintf(stderr, "(null)\n");
    }
}

// LCOV_EXCL_STOP

