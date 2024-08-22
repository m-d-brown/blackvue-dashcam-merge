#!python3.11

from datetime import datetime
import collections
import concurrent.futures
import ffmpeg
import os
import os.path
import sys
import traceback


# Parallelism may or may not speed up the process. The MacBook M2, for example,
# has only a single core media engine for H264 encode/decode acceleration.
WORKERS=1


# Parse a filename like '20240813_091545_NF.mp4' and return ('F',
# datetime(...)).
def parse_blackvue_filename(name):
    root, ext = os.path.splitext(name)
    if ext != '.mp4':
        return None
    parts = root.split('_')
    if len(parts) != 3:
        return None
    kind_char = parts[2][-1]
    kind = ''
    if kind_char == 'F':
        kind = 'front'
    elif kind_char == 'R':
        kind = 'rear'
    else:
        return None
    # Example: '20240813' + '165705'
    t = datetime.strptime(parts[0] + parts[1], '%Y%m%d%H%M%S')
    return kind, t


# Print bytes b, spanning multiple lines, prefixed by title.
def print_bytes(title, b):
    if b is None:
        return
    lines = str(b, 'utf-8').splitlines(keepends=False)
    if lines:
        lines.insert(0, f'{title}')
        print('\n\t'.join(lines))


def print_traceback(title, exc, tb):
    lines = [f'{title} generated an exception: {exc.__class__.__name__}: {exc}']
    lines.extend(tb.splitlines())
    print('\n\t'.join(lines))


# SourceVideo describes an input video.
SourceVideo = collections.namedtuple('SourceVideo', 'path, probe')


def new_source_video(path):
    return SourceVideo(path, ffmpeg.probe(path))


# Return audio and video streams in ffprobe. Returns None for a stream
# if they aren't found.
def get_av_ffprobe_streams(ffprobe):
    a = None
    v = None
    for s in ffprobe['streams']:
        t = s['codec_type']
        if t == 'audio':
            a = s
        elif t == 'video':
            v = s
    return a, v


# srcs is [SourceVideo]
def process_videos(srcs, dst_path):
    srcs.sort()

    streams = []
    out_bit_rate = 0
    for src in srcs:
        input = ffmpeg.input(src.path)
        streams.append(input.video)
        a, v = get_av_ffprobe_streams(src.probe)

        if v is None:
            raise RuntimeError('cannot find video stream in {src.path} ffprobe output')
        bit_rate  = int(v['bit_rate'])
        if bit_rate > out_bit_rate:
            out_bit_rate = bit_rate

        if a is None:
            dur = v['duration']
            # https://github.com/kkroening/ffmpeg-python/issues/303
            audio = ffmpeg.input(f"anullsrc=cl=mono:r=16000:d={dur}s", f='lavfi')
        else:
            audio = input.audio

        streams.append(audio)

    stream = ffmpeg.concat(*streams, a=1, v=1)
    dst_path_partial = dst_path + '.partial.mp4'
    stream = ffmpeg.output(
            stream,
            dst_path_partial,
            format='mp4',

            # Mac specific
            vcodec='h264_videotoolbox',
            constant_bit_rate=1,

            video_bitrate=out_bit_rate,
            # TODO: Chose frame rate and audio bit rate dynamically from
            #       input files as well.
            r=30,
            acodec='aac',
            ac=1,
            audio_bitrate=16000,
            loglevel='error',
            )
    stream = ffmpeg.overwrite_output(stream)
    paths = [s.path for s in srcs]
    print(f'starting {dst_path} bit_rate={out_bit_rate} from {len(srcs)} videos: {paths}')
    try:
        out, err = ffmpeg.run(stream,
                              quiet=True,
                              overwrite_output=True,
                              )
    except ffmpeg.Error as e:
        print_bytes(f'{dst_path} generated exception: stdout', e.stdout)
        print_bytes(f'{dst_path} generated exception: stderr', e.stderr)
        return

    print_bytes(f'{dst_path} stdout', out)
    print_bytes(f'{dst_path} stderr', err)

    os.rename(dst_path_partial, dst_path)
    print(f'done {dst_path}')


# Return {'destination_path': ['source_path']}
def find_dst_videos(src_dir, dst_dir):
    dst_to_srcs = collections.defaultdict(list)
    for root, dirs, files in os.walk(src_dir):
        for file in files:
            parsed = parse_blackvue_filename(file)
            if parsed is None:
                continue
            kind, video_time = parsed

            src_path = os.path.join(root, file)
            day = video_time.strftime('%Y%m%d')
            hour = video_time.strftime('%H')
            dst_path = os.path.join(dst_dir, day, kind, day + '-' + hour + '.mp4')
            if not os.path.exists(dst_path):
                dst_to_srcs[dst_path].append(src_path)
    return dst_to_srcs


def process(src_dir, dst_dir):
    dst_to_srcs = find_dst_videos(src_dir, dst_dir)

    path_to_src_vid = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_src = {}
        for _, srcs in dst_to_srcs.items():
            for s in srcs:
                f = executor.submit(new_source_video, s)
                future_to_src[f] = s
        for f in concurrent.futures.as_completed(future_to_src):
            src = future_to_src[f]
            try:
                r = f.result()
            except Exception as exc:
                print_traceback(src, exc, traceback.format_exc())
            else:
                path_to_src_vid[r.path] = r
    print(f'probed {len(path_to_src_vid)} videos')

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        future_to_dst = {}
        running = 0

        for (dst, src_paths) in dst_to_srcs.items():
            srcs = [path_to_src_vid[p] for p in src_paths]

            dir = os.path.dirname(dst)
            if not os.path.exists(dir):
                os.makedirs(dir)

            running += 1
            f = executor.submit(process_videos, srcs, dst)
            future_to_dst[f] = dst

        print(f'queued {running} videos')

        for future in concurrent.futures.as_completed(future_to_dst):
            dst = future_to_dst[future]
            try:
                data = future.result()
            except Exception as exc:
                lines = [f'{dst} generated an exception: {exc.__class__.__name__}: {exc}']
                lines.extend(traceback.format_exc().splitlines())
                print('\n\t'.join(lines))
            running -= 1
            total = len(future_to_dst)
            print(f'{running} of {total} remain')


def main():
    if len(sys.argv) != 3:
        print('need two args: [src-directory] [dst-directory]')
        sys.exit(1)

    process(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
