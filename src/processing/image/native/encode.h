#pragma once
#include <avif/avif.h>
#include <jxl/encode.h>
void encode_jpeg(VipsImage *im, int quality, int progressive, const char *path);
float image_distance(int quality);
static void jxl_ok(JxlEncoderStatus status) {
    if (status != JXL_ENC_SUCCESS)
        fail("JPEG XL encoding failed");
}
static void encode_jxl(VipsImage *im, Source *source, void *pixels, size_t size, int depth, int hdr,
                       int quality, const char *path) {
    JxlEncoder *enc = JxlEncoderCreate(NULL);
    if (!enc)
        fail("cannot allocate JPEG XL encoder");
    JxlBasicInfo info;
    JxlEncoderInitBasicInfo(&info);
    info.xsize = im->Xsize;
    info.ysize = im->Ysize;
    info.bits_per_sample = depth;
    info.num_extra_channels = im->Bands - 3;
    info.alpha_bits = info.num_extra_channels ? depth : 0;
    info.uses_original_profile = quality == 100;
    jxl_ok(JxlEncoderSetBasicInfo(enc, &info));
    JxlColorEncoding color;
    JxlColorEncodingSetToSRGB(&color, JXL_FALSE);
    if (hdr) {
        color.primaries = source->primaries == 12  ? JXL_PRIMARIES_P3
                          : source->primaries == 9 ? JXL_PRIMARIES_2100
                                                   : JXL_PRIMARIES_SRGB;
        color.transfer_function =
            source->transfer == 16 ? JXL_TRANSFER_FUNCTION_PQ : JXL_TRANSFER_FUNCTION_HLG;
        color.rendering_intent = JXL_RENDERING_INTENT_RELATIVE;
    }
    jxl_ok(JxlEncoderSetColorEncoding(enc, &color));
    JxlEncoderFrameSettings *frame = JxlEncoderFrameSettingsCreate(enc, NULL);
    if (!frame)
        fail("cannot allocate JPEG XL frame");
    jxl_ok(JxlEncoderFrameSettingsSetOption(frame, JXL_ENC_FRAME_SETTING_EFFORT, 5));
    if (quality == 100)
        jxl_ok(JxlEncoderSetFrameLossless(frame, JXL_TRUE));
    else
        jxl_ok(JxlEncoderSetFrameDistance(frame, image_distance(quality)));
    JxlPixelFormat fmt = {(uint32_t)im->Bands, depth == 16 ? JXL_TYPE_UINT16 : JXL_TYPE_UINT8,
                          JXL_NATIVE_ENDIAN, 0};
    jxl_ok(JxlEncoderAddImageFrame(frame, &fmt, pixels, size));
    JxlEncoderCloseInput(enc);
    FILE *f = open_output(path);
    JxlEncoderStatus status;
    do {
        uint8_t buffer[65536], *next = buffer;
        size_t remaining = sizeof(buffer);
        status = JxlEncoderProcessOutput(enc, &next, &remaining);
        if (status != JXL_ENC_NEED_MORE_OUTPUT)
            jxl_ok(status);
        write_bytes(f, buffer, sizeof(buffer) - remaining);
    } while (status == JXL_ENC_NEED_MORE_OUTPUT);
    close_output(f);
    JxlEncoderDestroy(enc);
}
static void encode_avif(VipsImage *im, Source *source, void *pixels, int depth, int hdr,
                        int quality, const char *path) {
    avifImage *image =
        avifImageCreate(im->Xsize, im->Ysize, hdr ? 10 : 8, AVIF_PIXEL_FORMAT_YUV444);
    avifEncoder *enc = avifEncoderCreate();
    if (!image || !enc)
        fail("cannot allocate AVIF encoder");
    image->colorPrimaries = hdr ? source->primaries : AVIF_COLOR_PRIMARIES_BT709;
    image->transferCharacteristics = hdr ? source->transfer : AVIF_TRANSFER_CHARACTERISTICS_SRGB;
    image->matrixCoefficients = hdr ? source->matrix : AVIF_MATRIX_COEFFICIENTS_BT709;
    image->yuvRange = AVIF_RANGE_FULL;
    avifRGBImage rgb;
    avifRGBImageSetDefaults(&rgb, image);
    rgb.format = im->Bands == 4 ? AVIF_RGB_FORMAT_RGBA : AVIF_RGB_FORMAT_RGB;
    rgb.depth = depth;
    rgb.pixels = pixels;
    rgb.rowBytes = im->Xsize * im->Bands * (depth / 8);
    if (avifImageRGBToYUV(image, &rgb) != AVIF_RESULT_OK)
        fail("AVIF color conversion failed");
    enc->maxThreads = 1;
    enc->speed = 8;
#if AVIF_VERSION_MAJOR >= 1
    enc->quality = quality;
    enc->qualityAlpha = AVIF_QUALITY_LOSSLESS;
#else
    enc->minQuantizer = enc->maxQuantizer = (100 - quality) * 63 / 100;
    enc->minQuantizerAlpha = enc->maxQuantizerAlpha = 0;
#endif
    avifRWData data = AVIF_DATA_EMPTY;
    if (avifEncoderWrite(enc, image, &data) != AVIF_RESULT_OK)
        fail("AVIF encoding failed");
    FILE *f = open_output(path);
    write_bytes(f, data.data, data.size);
    close_output(f);
    avifRWDataFree(&data);
    avifEncoderDestroy(enc);
    avifImageDestroy(image);
}
