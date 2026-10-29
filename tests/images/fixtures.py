"""Create deterministic image inputs without running the processing engine."""
import csv
import hashlib
import json
import os
from pathlib import Path
import re

from PIL import Image, ImageDraw

FIXTURES = Path(__file__).resolve().parents[1] / 'fixtures'
SIZES = {'landscape': (2400, 1200), 'portrait': (1200, 2400), 'small': (100, 50)}


def styles():
    lines = (FIXTURES / 'image-styles.txt').read_text(encoding='utf-8').splitlines()
    with (FIXTURES / 'image-style-sizes.tsv').open(encoding='utf-8', newline='') as source:
        expected = list(csv.DictReader(source, delimiter='\t'))
    assert len(lines) == len(expected) == 50
    assert len({row['name'] for row in expected}) == 50
    for index, (line, row) in enumerate(zip(lines, expected), 1):
        match = re.fullmatch(r'styleName:\s*([^,]+),\s*styleBody:\s*(.+)', line)
        assert match and match[1] == row['name'] and row['id'] == f'S{index:02}'
        row['body'] = match[2]
        geometry = re.search(r'\.(\d+)([pix])\.', row['name'])
        resize = re.search(r'/resize,([^/]+)', row['body'])
        assert bool(geometry) == bool(resize), row['name']
        if geometry:
            size, mode = geometry.groups()
            bounds = {'m': 'lfit', 'limit': '1'}
            if mode in ('p', 'x'):
                bounds['w'] = size
            if mode in ('i', 'x'):
                bounds['h'] = size
            assert dict(item.split('_', 1) for item in resize[1].split(',')) == bounds, row['name']
        quality = re.match(r'q(\d+)\.', row['name'])
        assert row['quality'] == (f'cap-{quality[1]}' if quality else 'default')
        assert re.findall(r'/quality,Q_(\d+)', row['body']) == ([quality[1]] if quality else [])
        assert row['format'] == row['name'].rsplit('.', 1)[-1]
        assert re.findall(r'/format,([^/]+)', row['body']) == ['jpg' if row['format'] == 'jpeg' else 'webp']
        assert ('/interlace,1' in row['body']) == (row['progressive'] == 'true') == (row['format'] == 'jpeg')
        for name in SIZES:
            row[name] = [int(value) for value in row[name].split('x')]
            assert len(row[name]) == 2 and all(value > 0 for value in row[name])
            assert all(value <= original for value, original in zip(row[name], SIZES[name])), row['name']
        assert row['small'] == list(SIZES['small']), row['name']
    for index in (32, 33, 44, 45):
        assert expected[index]['landscape'] == [300, 150]
        assert expected[index]['portrait'] == [75, 150]
    assert expected[20]['body'] == 'image/interlace,1/format,jpg'
    return expected


def generate(folder):
    assert folder.is_absolute() and not folder.exists(), 'use a new absolute fixture directory'
    cases = styles()
    folder.mkdir(parents=True)
    inputs = []
    for name, size in SIZES.items():
        image = Image.new('RGB', size)
        draw = ImageDraw.Draw(image)
        for x in range(size[0]):
            draw.line([(x, 0), (x, size[1])], fill=(x * 255 // size[0], 120, 255 - x * 255 // size[0]))
        draw.rectangle((size[0] // 4, size[1] // 4, size[0] * 3 // 4, size[1] * 3 // 4), outline='white', width=4)
        for suffix, format_name in [('jpg', 'JPEG'), ('png', 'PNG'), ('webp', 'WEBP')]:
            file = folder / f'{name}.{suffix}'
            image.save(file, quality=95)
            with Image.open(file) as decoded:
                decoded.load()
                assert decoded.size == size and decoded.format == format_name
            inputs.append({'file': file.name, 'size': size, 'format': format_name,
                           'sha256': hashlib.sha256(file.read_bytes()).hexdigest()})
    alpha = Image.new('RGBA', (512, 256))
    ImageDraw.Draw(alpha).ellipse((96, 32, 416, 224), fill=(240, 100, 160, 180))
    alpha.save(folder / 'transparent.png')
    frames = [Image.new('RGB', (64, 32), color) for color in ('red', 'blue')]
    frames[0].save(folder / 'animated.gif', save_all=True, append_images=frames[1:], duration=100, loop=0)
    exif = Image.Exif()
    exif[274] = 6
    Image.new('RGB', (12, 8), 'green').save(folder / 'orientation-6.jpg', exif=exif)
    Image.new('RGB', (128, 64), 'orange').save(folder / 'quality-60.jpg', quality=60)
    (folder / 'damaged.jpg').write_bytes(b'\xff\xd8broken')
    with Image.open(folder / 'transparent.png') as decoded:
        assert decoded.mode == 'RGBA' and decoded.getextrema()[3] == (0, 180)
    with Image.open(folder / 'animated.gif') as decoded:
        assert decoded.n_frames == 2
    with Image.open(folder / 'orientation-6.jpg') as decoded:
        assert decoded.getexif()[274] == 6
    manifest = {'inputs': inputs, 'styles': cases, 'base_cases': len(inputs) * len(cases)}
    (folder / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    return manifest


if __name__ == '__main__':
    result = generate(Path(os.environ['MOKYU_TEST_IMAGE_ROOT']))
    print(f"PASS fixtures: {len(result['styles'])} styles, {len(result['inputs'])} static inputs, {result['base_cases']} processing cases planned")
