# blackvue-dashcam-merge

Regularize and merge video files produced by a Blackvue dashcam using ffmpeg-python.

This tool finds all videos in a directory, which are typically one minute long.
It groups them by hour and camera source (front or back). To concatenate, it
re-encodes all videos with the largest detected bit rate, with a constant
rate, and adds in silent audio tracks for files without audio (from parking
mode). It then concatenates all videos into one file that's up to an hour
long.

Requires [ffmpeg-python](https://kkroening.github.io/ffmpeg-python/).

Tested on a MacBook M2 with 4K output from a [Blackvue DR970X-2CH
Plus](https://shop.blackvue.com/product/dr970x-2ch-plus/). The tool uses the
`h264_videotoolbox` video codec (for Mac). This could be made configurable
with future pull requests.

Run as `python blackvue_dashcam_merge.py [src_dir] [dst_dir}`. For example,

```
% python blackvue_dashcam_merge.py /Volumes/BLACKVUE/BlackVue/Record/ ~/Dashcam/
```


