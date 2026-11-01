#pragma once
#include "io.h"
#include <vips/vips.h>
#include <libheif/heif.h>
#include <stdio.h>
#include <jpeglib.h>
#include <webp/demux.h>
#include <jxl/decode.h>
#include <jxl/version.h>
#include <math.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

typedef struct {
    VipsImage *image;
    const char *format, *range;
    int frames, alpha, quality, primaries, transfer, matrix, sequential;
} Source;
#include "hdr.h"

static int number(const char *s, int max) {
    char *end;
    long n = strtol(s, &end, 10);
    if (!*s || *end || n < 0 || n > max)
        fail("invalid image parameter");
    return (int)n;
}
static uint32_t be32(const unsigned char *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
}
static void jpeg_failure(j_common_ptr state) {
    (void)state;
    fail("invalid JPEG header");
}
extern int VP8EstimateQuality(const uint8_t *data, size_t size);

static int jpeg_quality(const unsigned char *p, size_t n, Source *s) {
    struct jpeg_decompress_struct dec;
    struct jpeg_compress_struct enc;
    struct jpeg_error_mgr errors;
    dec.err = jpeg_std_error(&errors);
    errors.error_exit = jpeg_failure;
    jpeg_create_decompress(&dec);
    jpeg_mem_src(&dec, p, n);
    jpeg_save_markers(&dec, JPEG_APP0 + 1, 65535);
    jpeg_save_markers(&dec, JPEG_APP0 + 2, 65535);
    jpeg_read_header(&dec, TRUE);
    s->sequential = !dec.progressive_mode && dec.data_precision == 8 &&
                    (dec.num_components == 1 || dec.num_components == 3);
    for (jpeg_saved_marker_ptr m = dec.marker_list; m; m = m->next) {
        if (memmem(m->data, m->data_length, "hdrgm:", 6) ||
            memmem(m->data, m->data_length, "urn:iso:std:iso:ts:21496",
                   strlen("urn:iso:std:iso:ts:21496")) ||
            (m->data_length >= 4 && !memcmp(m->data, "MPF\0", 4)))
            s->range = "gain_map";
    }
    int best = -1;
    unsigned long error = ~0UL;
    if (dec.num_components && dec.comp_info[0].quant_tbl_no < NUM_QUANT_TBLS) {
        JQUANT_TBL *table = dec.quant_tbl_ptrs[dec.comp_info[0].quant_tbl_no];
        if (table) {
            enc.err = jpeg_std_error(&errors);
            errors.error_exit = jpeg_failure;
            jpeg_create_compress(&enc);
            enc.input_components = 3;
            enc.in_color_space = JCS_RGB;
            jpeg_set_defaults(&enc);
            for (int q = 1; q <= 100; q++) {
                jpeg_set_quality(&enc, q, TRUE);
                unsigned long delta = 0;
                for (int i = 0; i < 64; i++)
                    delta += labs((long)table->quantval[i] - enc.quant_tbl_ptrs[0]->quantval[i]);
                if (delta < error) {
                    error = delta;
                    best = q;
                }
            }
            jpeg_destroy_compress(&enc);
        }
    }
    jpeg_destroy_decompress(&dec);
    return best;
}
static void heif_profile(const unsigned char *p, size_t n, Source *s) {
    struct heif_context *ctx = heif_context_alloc();
    struct heif_image_handle *h = NULL;
    if (!ctx || heif_context_read_from_memory_without_copy(ctx, p, n, NULL).code ||
        heif_context_get_primary_image_handle(ctx, &h).code)
        fail("invalid primary image");
    struct heif_color_profile_nclx *color = NULL;
    if (!heif_image_handle_get_nclx_color_profile(h, &color).code && color) {
        s->primaries = color->color_primaries;
        s->transfer = color->transfer_characteristics;
        s->matrix = color->matrix_coefficients;
        if (s->transfer == 16)
            s->range = "pq";
        if (s->transfer == 18)
            s->range = "hlg";
        if ((s->transfer == 16 || s->transfer == 18) && s->primaries != 9 && s->primaries != 12)
            s->range = "unknown";
        heif_nclx_color_profile_free(color);
    }
    s->alpha = heif_image_handle_has_alpha_channel(h);
    uint32_t box = be32(p);
    if (box < 16 || box > n)
        fail("invalid image file type");
    for (size_t i = 8; i + 4 <= box; i += 4) {
        if (!memcmp(p + i, "tmap", 4))
            s->range = "gain_map";
        if (!memcmp(p + i, "avis", 4) || !memcmp(p + i, "msf1", 4))
            fail("image sequences are unsupported");
    }
    heif_image_handle_release(h);
    heif_context_free(ctx);
}
static void jxl_profile(const unsigned char *p, size_t n, Source *s) {
    JxlDecoder *dec = JxlDecoderCreate(NULL);
    JxlBasicInfo info;
    if (!dec ||
        JxlDecoderSubscribeEvents(dec, JXL_DEC_BASIC_INFO | JXL_DEC_COLOR_ENCODING) !=
            JXL_DEC_SUCCESS ||
        JxlDecoderSetInput(dec, p, n) != JXL_DEC_SUCCESS)
        fail("invalid JPEG XL");
    JxlDecoderCloseInput(dec);
    if (JxlDecoderProcessInput(dec) != JXL_DEC_BASIC_INFO ||
        JxlDecoderGetBasicInfo(dec, &info) != JXL_DEC_SUCCESS || info.have_animation)
        fail("invalid or animated JPEG XL");
    if (JxlDecoderProcessInput(dec) != JXL_DEC_COLOR_ENCODING)
        fail("missing JPEG XL colour encoding");
    JxlColorEncoding color;
    s->range = "unknown";
#if JPEGXL_MAJOR_VERSION == 0 && JPEGXL_MINOR_VERSION < 9
    JxlDecoderStatus status =
        JxlDecoderGetColorAsEncodedProfile(dec, NULL, JXL_COLOR_PROFILE_TARGET_ORIGINAL, &color);
#else
    JxlDecoderStatus status =
        JxlDecoderGetColorAsEncodedProfile(dec, JXL_COLOR_PROFILE_TARGET_ORIGINAL, &color);
#endif
    if (status == JXL_DEC_SUCCESS) {
        if (color.transfer_function == JXL_TRANSFER_FUNCTION_SRGB ||
            color.transfer_function == JXL_TRANSFER_FUNCTION_709 ||
            color.transfer_function == JXL_TRANSFER_FUNCTION_GAMMA)
            s->range = "sdr";
        else if (color.white_point == JXL_WHITE_POINT_D65 &&
                 (color.primaries == JXL_PRIMARIES_P3 || color.primaries == JXL_PRIMARIES_2100)) {
            s->primaries = color.primaries == JXL_PRIMARIES_P3 ? 12 : 9;
            s->matrix = s->primaries == 9 ? 9 : 12;
            if (color.transfer_function == JXL_TRANSFER_FUNCTION_PQ) {
                s->range = "pq";
                s->transfer = 16;
            }
            if (color.transfer_function == JXL_TRANSFER_FUNCTION_HLG) {
                s->range = "hlg";
                s->transfer = 18;
            }
        }
    }
    JxlDecoderDestroy(dec);
}
static void source_dimensions(uint32_t w, uint32_t h, int frames, uint64_t pixels) {
    if (!w || !h || w > 32768 || h > 32768 || frames < 1 || frames > 256 ||
        (uint64_t)w * h > pixels / (unsigned)frames)
        fail("image exceeds its pixel or frame budget");
}
static void read_source(const char *path, int page, uint64_t pixels, int render, int input, Source *s) {
    GMappedFile *f = g_mapped_file_new(path, FALSE, NULL);
    if (!f)
        fail("image input unavailable");
    size_t n = g_mapped_file_get_length(f);
    const unsigned char *p = (const unsigned char *)g_mapped_file_get_contents(f);
    if (n < 12)
        fail("truncated image");
    const char *limit = input ? getenv("MOKYU_IMAGE_INPUT_PIXELS") : NULL;
    uint64_t source_pixels = limit ? (uint64_t)number(limit, 150000000) : pixels;
    *s = (Source){.frames = 1, .quality = -1, .range = "sdr"};
    const char *loader = NULL;
    if (p[0] == 0xff && p[1] == 0xd8) {
        s->format = "jpeg";
        loader = "jpegload";
        s->quality = jpeg_quality(p, n, s);
    } else if (!memcmp(p, "\x89PNG\r\n\x1a\n", 8)) {
        s->format = "png";
        loader = "pngload";
        s->quality = 100;
        if (n < 33 || be32(p + 8) != 13 || memcmp(p + 12, "IHDR", 4))
            fail("invalid PNG header");
        source_dimensions(be32(p + 16), be32(p + 20), 1, source_pixels);
        s->sequential = p[24] == 8 && p[28] == 0;
        for (size_t i = 8; i + 12 <= n;) {
            uint32_t len = be32(p + i);
            if (len > n - i - 12)
                fail("truncated PNG chunk");
            if (!memcmp(p + i + 4, "acTL", 4))
                fail("animated PNG is unsupported");
            if (!memcmp(p + i + 4, "cICP", 4) && len >= 4 && (p[i + 9] == 16 || p[i + 9] == 18))
                s->range = "unknown";
            if (!memcmp(p + i + 4, "IDAT", 4))
                break;
            i += 12 + (size_t)len;
        }
    } else if (!memcmp(p, "RIFF", 4) && !memcmp(p + 8, "WEBP", 4)) {
        s->format = "webp";
        loader = "webpload";
        WebPData data = {p, n};
        WebPDemuxer *d = WebPDemux(&data);
        if (!d)
            fail("invalid WebP");
        s->frames = WebPDemuxGetI(d, WEBP_FF_FRAME_COUNT);
        WebPDemuxDelete(d);
        s->quality = VP8EstimateQuality(p, n);
        if (s->quality > 100)
            s->quality = 100;
    } else if (!memcmp(p, "GIF87a", 6) || !memcmp(p, "GIF89a", 6)) {
        s->format = "gif";
        loader = "gifload";
        s->quality = 100;
    } else if (!memcmp(p + 4, "ftyp", 4)) {
        const char *mime = heif_get_file_mime_type(p, n > INT_MAX ? INT_MAX : (int)n);
        if (!mime || (strcmp(mime, "image/heic") && strcmp(mime, "image/heif") &&
                      strcmp(mime, "image/avif")))
            fail("unsupported image container");
        s->format = !strcmp(mime, "image/avif") ? "avif" : "heif";
        loader = "heifload";
        heif_profile(p, n, s);
    } else if ((p[0] == 0xff && p[1] == 0x0a) || !memcmp(p, "\0\0\0\x0cJXL \r\n\x87\n", 12)) {
        s->format = "jxl";
        loader = "jxlload";
        jxl_profile(p, n, s);
    } else
        fail("unsupported image signature");
    if ((!strcmp(s->format, "webp") || !strcmp(s->format, "gif")) && page >= 0)
        check(vips_call(loader, path, &s->image, "page", page, "n", 1, "fail_on",
                        VIPS_FAIL_ON_ERROR, NULL));
    else {
        if (page > 0)
            fail("frame selection is unsupported for this input");
        check(vips_call(loader, path, &s->image, "fail_on", VIPS_FAIL_ON_ERROR, NULL));
    }
    if ((!strcmp(s->format, "gif") || !strcmp(s->format, "webp") || !strcmp(s->format, "jxl")) &&
        vips_image_get_typeof(s->image, "n-pages"))
        check(vips_image_get_int(s->image, "n-pages", &s->frames));
    int w = s->image->Xsize, h = s->image->Ysize;
    source_dimensions(w, h, s->frames, source_pixels);
    if (page >= s->frames)
        fail("image exceeds its pixel or frame budget");
    if ((uint64_t)w * h * s->frames > pixels) {
        if (s->frames != 1 || !s->sequential || strcmp(s->range, "sdr"))
            fail("image exceeds its working pixel budget");
        if (render) {
            g_object_unref(s->image);
            check(vips_call(loader, path, &s->image, "access", VIPS_ACCESS_SEQUENTIAL, "fail_on",
                            VIPS_FAIL_ON_ERROR, NULL));
        }
    }
    s->alpha = vips_image_hasalpha(s->image);
    if (!strcmp(s->format, "jpeg") && !strcmp(s->range, "gain_map") && n <= INT_MAX &&
        is_uhdr_image((void *)p, (int)n)) {
        s->range = "pq";
        if (render) {
            VipsImage *base = s->image;
            s->image = hdr_pixels(p, n, s, w, h);
            g_object_unref(base);
        }
    }
    g_mapped_file_unref(f);
}
