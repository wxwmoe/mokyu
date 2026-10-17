/* Linux test interposer: fail only fsync under the dedicated backend root. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int fsync(int fd) {
    int (*sync_file)(int) = dlsym(RTLD_NEXT, "fsync");
    const char *root = getenv("MOKYU_TEST_FSYNC_ROOT");
    const char *fault = getenv("MOKYU_TEST_FSYNC_FAULT");
    const char *trace = getenv("MOKYU_TEST_FSYNC_TRACE");
    char link[64], path[PATH_MAX], mode = 0;
    struct stat info;
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(link, path, sizeof(path) - 1);
    if (!root || !fault || !trace || !sync_file || length < 0)
        return sync_file ? sync_file(fd) : (errno = ENOSYS, -1);
    path[length] = 0;
    size_t prefix = strlen(root);
    if (strncmp(path, root, prefix) || (path[prefix] && path[prefix] != '/'))
        return sync_file(fd);
    if (fstat(fd, &info)) return -1;
    char target = S_ISDIR(info.st_mode) ? 'd' : 'f';
    int marker = open(fault, O_RDONLY);
    if (marker >= 0) {
        if (read(marker, &mode, 1) != 1) mode = 0;
        close(marker);
    }
    int result;
    if (mode == target) {
        errno = EIO;
        result = -1;
    } else {
        result = sync_file(fd);
    }
    int saved = errno;
    int log = open(trace, O_WRONLY | O_APPEND | O_CREAT, 0600);
    if (log >= 0) {
        char line[4] = {target, ' ', result ? '1' : '0', '\n'};
        if (write(log, line, sizeof(line)) != sizeof(line)) {
            close(log);
            errno = EIO;
            return -1;
        }
        close(log);
    }
    errno = saved;
    return result;
}
