#include "source.h"
#include "encode.h"

#define NEXT(call)                                                                                 \
    do {                                                                                           \
        VipsImage *next = NULL;                                                                    \
        check(call);                                                                               \
        g_object_unref(im);                                                                        \
        im = next;                                                                                 \
    } while (0)

static void dimensions(int w, int h, uint64_t pixels) {
    if (w < 1 || h < 1 || w > 32768 || h > 32768 || (uint64_t)w * h > pixels)
        fail("image exceeds its pixel budget");
}
static VipsImage *operations(Source *s, int count, char **args, uint64_t pixels) {
    VipsImage *im = s->image;
    int large = (uint64_t)im->Xsize * im->Ysize > pixels, orient_after_resize = 0;
    if (!strcmp(s->range, "sdr")) {
        if (vips_image_get_typeof(im, VIPS_META_ICC_NAME)) {
            NEXT(vips_icc_transform(im, &next, "srgb", "embedded", TRUE, "depth", 8, NULL));
        } else if (im->Type != VIPS_INTERPRETATION_sRGB) {
            NEXT(vips_colourspace(im, &next, VIPS_INTERPRETATION_sRGB, NULL));
        }
    }
    for (int i = 0; i < count;) {
        const char *op = args[i++];
        if (large && !strcmp(op, "orient")) {
            orient_after_resize = 1;
            continue;
        }
        if (large && strcmp(op, "resize"))
            fail("large images require a resize before other transformations");
        if (!strcmp(op, "orient")) {
            NEXT(vips_autorot(im, &next, NULL));
        } else if (!strcmp(op, "resize") && count - i >= 4) {
            const char *mode = args[i++];
            int w = number(args[i++], 32768), h = number(args[i++], 32768),
                up = number(args[i++], 1);
            int iw = im->Xsize, ih = im->Ysize;
            if (orient_after_resize && vips_image_get_orientation(im) >= 5) {
                iw = im->Ysize;
                ih = im->Xsize;
            }
            double sx = w ? (double)w / iw : 0, sy = h ? (double)h / ih : 0;
            double scale = !w ? sy : !h ? sx : !strcmp(mode, "cover") ? fmax(sx, sy) : fmin(sx, sy);
            if (!up)
                scale = fmin(scale, 1);
            dimensions((int)fmax(1, floor(iw * scale + .5)), (int)fmax(1, floor(ih * scale + .5)),
                       pixels);
            if (scale != 1)
                NEXT(vips_resize(im, &next, scale, "kernel", VIPS_KERNEL_LANCZOS3, NULL));
            if (orient_after_resize) {
                NEXT(vips_autorot(im, &next, NULL));
                orient_after_resize = 0;
            }
            large = 0;
        } else if (!strcmp(op, "crop") && count - i >= 3) {
            int width = number(args[i++], 32768), height = number(args[i++], 32768);
            int w = MIN(width, im->Xsize), h = MIN(height, im->Ysize);
            const char *anchor = args[i++];
            int dx = im->Xsize - w, dy = im->Ysize - h;
            int left = strstr(anchor, "west") ? 0 : strstr(anchor, "east") ? dx : dx / 2;
            int top = strstr(anchor, "north") ? 0 : strstr(anchor, "south") ? dy : dy / 2;
            dimensions(w, h, pixels);
            NEXT(vips_extract_area(im, &next, left, top, w, h, NULL));
        } else if (!strcmp(op, "background") && count - i >= 1) {
            const char *text = args[i++];
            char *end;
            unsigned long c = strtoul(text, &end, 16);
            if (strlen(text) != 6 || *end)
                fail("invalid background");
            if (vips_image_hasalpha(im)) {
                VipsImage *flat = flatten_on(im, s, c);
                g_object_unref(im);
                im = flat;
            }
        } else
            fail("invalid image operation");
        dimensions(im->Xsize, im->Ysize, pixels);
    }
    dimensions(im->Xsize, im->Ysize, pixels);
    return im;
}
static void encode(VipsImage *im, Source *s, const char *fmt, int q, int progressive,
                   const char *out) {
    int hdr = !strcmp(s->range, "pq") || !strcmp(s->range, "hlg");
    if (strcmp(s->range, "sdr") && !hdr)
        fail("unsupported source dynamic range");
    if (hdr && strcmp(fmt, "jxl") && strcmp(fmt, "avif") && strcmp(fmt, "jpeg"))
        fail("output cannot preserve HDR");
    if (!strcmp(fmt, "png")) {
        check(vips_pngsave(im, out, "strip", TRUE, "compression", 4, NULL));
        return;
    }
    if (!strcmp(fmt, "webp")) {
        check(vips_webpsave(im, out, "strip", TRUE, "Q", q, "lossless", FALSE, "effort", 4, NULL));
        return;
    }
    if (!strcmp(fmt, "jpeg")) {
        VipsImage *flat = flatten_on(im, s, 0xffffff);
        if (hdr)
            encode_hdr_jpeg(flat, s, q, progressive, out);
        else
            encode_jpeg(flat, q, progressive, out);
        g_object_unref(flat);
        return;
    }
    if (strcmp(fmt, "jxl") && strcmp(fmt, "avif"))
        fail("unsupported image output");
    int depth = im->BandFmt == VIPS_FORMAT_USHORT ? 16 : im->BandFmt == VIPS_FORMAT_UCHAR ? 8 : 0;
    if (!depth || (hdr && depth < 16) || (im->Bands != 3 && im->Bands != 4))
        fail("unsupported pixel layout");
    size_t size;
    void *pixels = vips_image_write_to_memory(im, &size);
    if (!pixels || size != (size_t)im->Xsize * im->Ysize * im->Bands * (depth / 8))
        fail("cannot read image pixels");
    if (!strcmp(fmt, "jxl"))
        encode_jxl(im, s, pixels, size, depth, hdr, q, out);
    else
        encode_avif(im, s, pixels, depth, hdr, q, out);
    g_free(pixels);
}
static void describe(const Source *s) {
    printf("{\"width\":%d,\"height\":%d,\"format\":\"%s\",\"range\":\"%s\",\"frames\":%d,"
           "\"alpha\":%s}", s->image->Xsize, s->image->Ysize, s->format, s->range, s->frames,
           s->alpha ? "true" : "false");
}

int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "version")) {
        puts(IMAGE_VERSION);
        return 0;
    }
    if (argc < 5 || VIPS_INIT("mokyu-image"))
        fail("invalid image invocation");
    vips_concurrency_set(1);
    vips_cache_set_max(0);
    uint64_t pixels = number(argv[3], 128000000);
    int page = !strcmp(argv[4], "-") ? -1 : number(argv[4], 255);
    Source s;
    read_source(argv[2], page, pixels, !strcmp(argv[1], "render"), 1, &s);
    if (!strcmp(argv[1], "probe")) {
        describe(&s);
        putchar('\n');
        g_object_unref(s.image);
        vips_shutdown();
        return 0;
    }
    if (strcmp(argv[1], "render") || argc < 10 || (s.frames > 1 && page < 0))
        fail("static input or explicit frame required");
    const char *out = argv[5], *fmt = argv[6], *mode = argv[7];
    if (!strcmp(fmt, "source"))
        fmt = s.format;
    int q = number(argv[8], 100), progressive = number(argv[9], 1);
    if (!q && !strcmp(mode, "source"))
        q = !strcmp(s.format, "jpeg") && s.quality > 0 ? s.quality
            : !strcmp(fmt, "jpeg")                     ? 95
            : !strcmp(fmt, "webp")                     ? 80
                                                       : 85;
    else if (!q)
        q = 85;
    else if (!strcmp(mode, "relative") && !strcmp(s.format, "jpeg")) {
        if (s.quality < 0)
            fail("source quality is unavailable");
        q = MAX(1, (s.quality * q + 50) / 100);
    } else if (strcmp(mode, "exact") && !strcmp(s.format, fmt) &&
               (!strcmp(fmt, "jpeg") || !strcmp(fmt, "webp"))) {
        if (s.quality < 0)
            fail("source quality is unavailable");
        q = MIN(q, s.quality);
    }
    putchar('[');
    if (page > 0 && s.frames > 1) {
        Source original;
        read_source(argv[2], -1, pixels, 0, 1, &original);
        describe(&original);
        g_object_unref(original.image);
    } else {
        describe(&s);
    }
    VipsImage *im = operations(&s, argc - 10, argv + 10, pixels);
    encode(im, &s, fmt, q, progressive, out);
    g_object_unref(im);
    Source image;
    read_source(out, -1, pixels, 0, 0, &image);
    putchar(',');
    describe(&image);
    puts("]");
    g_object_unref(image.image);
    vips_shutdown();
    return 0;
}
