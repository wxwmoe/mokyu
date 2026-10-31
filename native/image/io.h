#pragma once
#include <stdio.h>
#include <stdlib.h>
static inline void fail(const char *message) {
    fprintf(stderr, "%s\n", message);
    exit(2);
}
static inline void check(int result) {
    if (result)
        fail("invalid image or unsupported transformation");
}
static inline FILE *open_output(const char *path) {
    FILE *f = fopen(path, "wb");
    if (!f)
        fail("cannot open image output");
    return f;
}
static inline void write_bytes(FILE *f, const void *bytes, size_t length) {
    if (fwrite(bytes, 1, length, f) != length)
        fail("cannot write image output");
}
static inline void close_output(FILE *f) {
    if (fclose(f))
        fail("cannot finish image output");
}
