#include <vips/vips.h>
#include "lib/jpegli/encode.h"
#include "io.h"

static int hdr_scans;
extern "C" void image_hdr_scans(int progressive) { hdr_scans = progressive; }
extern "C" void image_hdr_start(j_compress_ptr enc, boolean tables) {
    jpegli_set_progressive_level(enc, hdr_scans);
    enc->optimize_coding = TRUE;
    jpegli_start_compress(enc, tables);
}

extern "C" float image_distance(int q) { return jpegli_quality_to_distance(q); }

extern "C" void encode_jpeg(VipsImage *im, int quality, int progressive, const char *path) {
    if (im->Bands != 3 || im->BandFmt != VIPS_FORMAT_UCHAR)
        fail("JPEG requires RGB pixels");
    FILE *f = open_output(path);
    struct jpeg_compress_struct enc;
    struct jpeg_error_mgr err;
    enc.err = jpegli_std_error(&err);
    err.error_exit = [](j_common_ptr) { fail("JPEG encoding failed"); };
    jpegli_create_compress(&enc);
    jpegli_stdio_dest(&enc, f);
    enc.image_width = im->Xsize;
    enc.image_height = im->Ysize;
    enc.input_components = 3;
    enc.in_color_space = JCS_RGB;
    jpegli_set_cicp_transfer_function(&enc, 13);
    jpegli_set_defaults(&enc);
    enc.comp_info[0].h_samp_factor = 1;
    enc.comp_info[0].v_samp_factor = 1;
    jpegli_set_distance(&enc, jpegli_quality_to_distance(quality), TRUE);
    jpegli_set_progressive_level(&enc, progressive);
    enc.optimize_coding = TRUE;
    jpegli_start_compress(&enc, TRUE);
    VipsRegion *region = vips_region_new(im);
    if (!region)
        fail("cannot read image pixels");
    for (int y = 0; y < im->Ysize;) {
        VipsRect strip = {0, y, im->Xsize, MIN(32, im->Ysize - y)};
        check(vips_region_prepare(region, &strip));
        for (int i = 0; i < strip.height; i++, y++) {
            JSAMPROW row = (JSAMPROW)VIPS_REGION_ADDR(region, 0, y);
            if (jpegli_write_scanlines(&enc, &row, 1) != 1)
                fail("JPEG encoding failed");
        }
    }
    jpegli_finish_compress(&enc);
    jpegli_destroy_compress(&enc);
    g_object_unref(region);
    close_output(f);
}
