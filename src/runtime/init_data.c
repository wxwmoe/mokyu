#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Only the empty bind root may be adopted. Keep its descriptor through setup. */
int main(void) {
    if (getuid() != 10001 || getgid() != 10001)
        return 1;

    int fd = open("/data", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) < 0)
        return 1;
    if (st.st_uid == 10001 && (st.st_mode & S_IRWXU) == S_IRWXU)
        return 0;

    DIR *dir = fdopendir(fd);
    if (!dir)
        return 1;
    errno = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") && strcmp(entry->d_name, ".."))
            return 1;
    }
    if (errno || fchown(fd, 10001, 10001) < 0)
        return 1;
    return (st.st_mode & S_IRWXU) != S_IRWXU && fchmod(fd, (st.st_mode & 0777) | S_IRWXU) < 0;
}
