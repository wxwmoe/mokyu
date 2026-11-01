#pragma once
#include <ultrahdr_api.h>

void image_hdr_scans(int progressive);
static void hdr_ok(uhdr_error_info_t result) {
    if (result.error_code != UHDR_CODEC_OK)
        fail("invalid HDR JPEG or exhausted processing budget");
}
static VipsImage *rgb16(uint16_t *data, int w, int h, int orientation) {
    VipsImage *memory =
                  vips_image_new_from_memory(data, (size_t)w * h * 6, w, h, 3, VIPS_FORMAT_USHORT),
              *typed = NULL;
    if (!memory)
        fail("cannot allocate HDR pixels");
    g_object_set_data_full(G_OBJECT(memory), "image-pixels", data, g_free);
    check(vips_copy(memory, &typed, "interpretation", VIPS_INTERPRETATION_RGB16, NULL));
    g_object_unref(memory);
    if (orientation > 1)
        vips_image_set_int(typed, VIPS_META_ORIENTATION, orientation);
    return typed;
}
static VipsImage *hdr_pixels(const unsigned char *p, size_t n, Source *s, int w, int h) {
    uhdr_codec_private_t *d = uhdr_create_decoder();
    if (!d)
        fail("cannot allocate HDR JPEG decoder");
    uhdr_compressed_image_t in = {.data = (void *)p,
                                  .data_sz = n,
                                  .capacity = n,
                                  .cg = UHDR_CG_UNSPECIFIED,
                                  .ct = UHDR_CT_UNSPECIFIED,
                                  .range = UHDR_CR_UNSPECIFIED};
    hdr_ok(uhdr_dec_set_image(d, &in));
    hdr_ok(uhdr_dec_set_out_color_transfer(d, UHDR_CT_PQ));
    hdr_ok(uhdr_dec_set_out_img_format(d, UHDR_IMG_FMT_32bppRGBA1010102));
    hdr_ok(uhdr_dec_probe(d));
    int gw = uhdr_dec_get_gainmap_width(d), gh = uhdr_dec_get_gainmap_height(d);
    if (uhdr_dec_get_image_width(d) != w || uhdr_dec_get_image_height(d) != h || gw < 1 || gh < 1 ||
        gw > w || gh > h)
        fail("invalid HDR JPEG dimensions");
    hdr_ok(uhdr_decode(d));
    uhdr_raw_image_t *raw = uhdr_get_decoded_image(d);
    if (!raw || raw->w != (unsigned)w || raw->h != (unsigned)h || !raw->planes[0] ||
        raw->stride[0] < (unsigned)w || raw->ct != UHDR_CT_PQ)
        fail("invalid HDR JPEG representation");
    if (raw->cg == UHDR_CG_BT_2100)
        s->primaries = 9;
    else if (raw->cg == UHDR_CG_DISPLAY_P3)
        s->primaries = 12;
    else if (raw->cg == UHDR_CG_BT_709)
        s->primaries = 1;
    else
        fail("unsupported HDR JPEG gamut");
    s->matrix = s->primaries;
    s->transfer = 16;
    uint16_t *pixels = g_try_malloc_n((size_t)w * h, 6);
    if (!pixels)
        fail("cannot allocate HDR pixels");
    for (int y = 0; y < h; y++)
        for (int x = 0; x < w; x++) {
            uint32_t v = ((uint32_t *)raw->planes[0])[(size_t)y * raw->stride[0] + x];
            for (int c = 0; c < 3; c++)
                pixels[3 * ((size_t)y * w + x) + c] =
                    (((v >> (10 * c)) & 1023) * 65535u + 511) / 1023;
        }
    uhdr_release_decoder(d);
    return rgb16(pixels, w, h, vips_image_get_orientation(s->image));
}
static double hdr_linear(double x, int pq) {
    if (pq) {
        double t = pow(x, 32.0 / 2523);
        return pow(fmax(t - 3424.0 / 4096, 0) / (2413.0 / 128 - 2392.0 / 128 * t), 16384.0 / 2610);
    }
    return x <= .5 ? x * x / 3 : (exp((x - .55991073) / .17883277) + .28466892) / 12;
}
static double hdr_code(double x, int pq) {
    if (pq) {
        double t = pow(x, 2610.0 / 16384);
        return pow((3424.0 / 4096 + 2413.0 / 128 * t) / (1 + 2392.0 / 128 * t), 2523.0 / 32);
    }
    return x <= 1.0 / 12 ? sqrt(3 * x) : .17883277 * log(12 * x - .28466892) + .55991073;
}
static VipsImage *flatten_on(VipsImage *im, Source *s, unsigned long color) {
    if (!vips_image_hasalpha(im))
        return g_object_ref(im);
    double rgb[] = {(color >> 16) & 255, (color >> 8) & 255, color & 255};
    if (!strcmp(s->range, "sdr")) {
        VipsArrayDouble *bg = vips_array_double_new(rgb, 3);
        VipsImage *out = NULL;
        check(vips_flatten(im, &out, "background", bg, NULL));
        vips_area_unref((VipsArea *)bg);
        return out;
    }
    if ((strcmp(s->range, "pq") && strcmp(s->range, "hlg")) || im->BandFmt != VIPS_FORMAT_USHORT ||
        im->Bands != 4)
        fail("unsupported HDR background");
    for (int c = 0; c < 3; c++) {
        double x = rgb[c] / 255;
        rgb[c] = x <= .04045 ? x / 12.92 : pow((x + .055) / 1.055, 2.4);
    }
    const double srgb[9] = {1, 0, 0, 0, 1, 0, 0, 0, 1};
    const double p3[9] = {.822462, .177538, 0, .033194, .966806, 0, .017083, .072397, .910520};
    const double bt2020[9] = {.627404, .329283, .043313, .069097, .919540,
                              .011363, .016392, .088013, .895595};
    const double *matrix = s->primaries == 9 ? bt2020 : s->primaries == 12 ? p3 : srgb;
    int pq = s->transfer == 16;
    double white = pq ? 203.0 / 10000 : hdr_linear(.75, 0), bg[3];
    for (int c = 0; c < 3; c++)
        bg[c] = white *
                (matrix[3 * c] * rgb[0] + matrix[3 * c + 1] * rgb[1] + matrix[3 * c + 2] * rgb[2]);
    size_t n = (size_t)im->Xsize * im->Ysize, size = 0;
    uint16_t *in = vips_image_write_to_memory(im, &size), *out = g_try_malloc_n(n, 6);
    if (!in || !out || size != n * 8)
        fail("cannot allocate HDR background");
    for (size_t i = 0; i < n; i++)
        for (int c = 0; c < 3; c++) {
            uint16_t a = in[4 * i + 3], v = in[4 * i + c];
            if (a == 65535)
                out[3 * i + c] = v;
            else {
                double linear = (a * hdr_linear(v / 65535.0, pq) + (65535 - a) * bg[c]) / 65535;
                out[3 * i + c] = lround(fmin(1, fmax(0, hdr_code(linear, pq))) * 65535);
            }
        }
    g_free(in);
    return rgb16(out, im->Xsize, im->Ysize, vips_image_get_orientation(im));
}
static void encode_hdr_jpeg(VipsImage *im, Source *s, int quality, int progressive,
                            const char *path) {
    if (im->Bands != 3 || im->BandFmt != VIPS_FORMAT_USHORT)
        fail("HDR JPEG requires high precision RGB pixels");
    size_t n = (size_t)im->Xsize * im->Ysize, size = 0;
    uint16_t *in = vips_image_write_to_memory(im, &size);
    uint32_t *packed = g_try_malloc_n(n, 4);
    if (!in || !packed || size != n * 6)
        fail("cannot allocate HDR JPEG pixels");
    for (size_t i = 0; i < n; i++) {
        uint32_t v = 3u << 30;
        for (int c = 0; c < 3; c++)
            v |= ((in[3 * i + c] * 1023u + 32767) / 65535) << (10 * c);
        packed[i] = v;
    }
    g_free(in);
    uhdr_raw_image_t raw = {.fmt = UHDR_IMG_FMT_32bppRGBA1010102,
                            .cg = s->primaries == 12  ? UHDR_CG_DISPLAY_P3
                                  : s->primaries == 9 ? UHDR_CG_BT_2100
                                                      : UHDR_CG_BT_709,
                            .ct = s->transfer == 16 ? UHDR_CT_PQ : UHDR_CT_HLG,
                            .range = UHDR_CR_FULL_RANGE,
                            .w = (unsigned)im->Xsize,
                            .h = (unsigned)im->Ysize,
                            .planes = {packed},
                            .stride = {(unsigned)im->Xsize}};
    image_hdr_scans(progressive);
    uhdr_codec_private_t *e = uhdr_create_encoder();
    if (!e)
        fail("cannot allocate HDR JPEG encoder");
    hdr_ok(uhdr_enc_set_raw_image(e, &raw, UHDR_HDR_IMG));
    hdr_ok(uhdr_enc_set_quality(e, quality, UHDR_BASE_IMG));
    hdr_ok(uhdr_enc_set_quality(e, 95, UHDR_GAIN_MAP_IMG));
    hdr_ok(uhdr_enc_set_using_multi_channel_gainmap(e, 1));
    hdr_ok(uhdr_encode(e));
    uhdr_compressed_image_t *output = uhdr_get_encoded_stream(e);
    if (!output || !output->data || !output->data_sz)
        fail("empty HDR JPEG");
    FILE *f = open_output(path);
    write_bytes(f, output->data, output->data_sz);
    close_output(f);
    uhdr_release_encoder(e);
    g_free(packed);
}
