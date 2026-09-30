# Resource Han Rounded CN

Self-hosted WOFF2 subsets of [Resource Han Rounded 0.990](https://github.com/CyanoHao/Resource-Han-Rounded/releases/tag/v0.990), using the CN Regular (400), Medium (500), and Bold (700) faces. Copyright and SIL Open Font License 1.1 are preserved in [OFL.txt](OFL.txt), the font metadata, and the frontend distribution notices.

The `ui` subset contains characters used by the Chinese locale. Remaining upstream characters are split into disjoint groups of at most 512 code points, so filenames and future translations can load additional glyphs without downloading the entire family. Font declarations are available in every locale; `unicode-range` downloads only the subsets needed by visible text, and `font-display: swap` keeps the interface usable while loading. The upstream glyph set also covers common traditional Chinese characters and Japanese kana. Latin text and numbers use Nunito first; Japanese uses Zen Maru Gothic before this family.

To regenerate, install `fonttools[woff]==4.66.1`, extract the upstream `RHR-CN-0.990.7z` archive, then run:

```sh
python web/scripts/subset-font.py /path/to/extracted-fonts
```

Source archive SHA-256: `e7005f7b4a7a0b8352d32c4a1358ff47564eb73be7fdb2db00d9f792755e9dc7`.

Generated subsets are committed assets; ordinary application builds need no Python or font conversion. Regeneration is optional when translations change: characters outside the existing `ui` subset are already covered by the other subsets. Keep the license and this provenance when updating the family.
