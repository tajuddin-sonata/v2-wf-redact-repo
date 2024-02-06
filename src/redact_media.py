from json import dumps
import logging
from pathlib import Path
from typing import Union
import ffmpeg
from flask import abort
from typing import Tuple, List


def merge_timing_windows(timings_list: Union[List[dict], None]):
    if (not timings_list) or len(timings_list) < 1:
        return None
    timings_list.sort(key=lambda x: (x["start"], x["end"]))
    merged_list = [timings_list[0]]
    for curr in timings_list:
        prev = merged_list[-1]
        if curr["start"] <= prev["end"]:
            prev["end"] = max(prev["end"], curr["end"])
        else:
            merged_list.append(curr)
    return merged_list


def build_ffmpeg_pipeline(
    temp_folder: str,
    input_file: ffmpeg.nodes.FilterableStream,
    timings_list: List[dict],
    file_details: dict,
# ) -> Union[ffmpeg.nodes.OutputStream, None]:
) -> Tuple[str,ffmpeg.nodes.OutputStream]:
    # logging.warning(dumps(file_details,indent=2))
    # Merge overlapping timing windows
    to_beep = merge_timing_windows(timings_list)
    to_ignore = []

    if (not to_beep) or len(to_beep) < 1:
        abort(500, 'no redaction windows for ffmpeg')

    # Generate inverse of timing windows bounded by file duration
    prev_end = 0
    for window in to_beep:
        if window["start"] > prev_end:
            to_ignore.append({"start": prev_end, "end": window["start"]})
        prev_end = window["end"]
    if prev_end < file_details["file_duration"]:
        to_ignore.append({"start": prev_end, "end": file_details["file_duration"]})

    # Generate ffmpeg format for timing windows
    # e.g. between(t,1.45,5.62)+between(t,8.8,9.4)+between(t,16,17.5)
    to_beep_args = "+".join(
        [
            "between(t," + str(window["start"]) + "," + str(window["end"]) + ")"
            for window in to_beep
        ]
    )
    to_ignore_args = "+".join(
        [
            "between(t," + str(window["start"]) + "," + str(window["end"]) + ")"
            for window in to_ignore
        ]
    )

    # Build filter_complex for:
    # muting base audio channel inside the timing windows,
    # arg_mute = "[0]volume=0:enable='" + to_beep_args + "'[main]"
    filter_audio_muted = input_file.audio.filter(
        "volume", volume=0, enable=to_beep_args
    )
    # creating a sine wave stream, muting the sine wave on the inverse windows
    # arg_sine = "sine=f=300,pan=stereo|FL=c0|FR=c0,volume=0:enable='" + to_ignore_args + "'[beep]"
    filter_audio_sine = ffmpeg.input(
        "sine=f=300:duration={}".format(str(file_details["file_duration"])),
        format="lavfi",
    ).filter("volume", volume=0, enable=to_ignore_args)
    # # merging the streams
    # arg_merge = "[main][beep]amix=inputs=2:duration=first"
    filter_audio_merged = ffmpeg.filter(
        (filter_audio_muted, filter_audio_sine), "amix", inputs=2, duration="first"
    )

    outnodes = [filter_audio_merged]
    settings = {}
    # settings["c:a"] = file_details["audio_codec"]

    # Alter settings and inputs if the media file is a video
    if file_details["has_video"]:
        outnodes = [input_file.video] + outnodes
        # settings["c:v"] = "copy"
        # if file_details["file_format"] == "mp4":
            # settings["movflags"] = "frag_keyframe"
            # file_details["file_format"] = "ismv"

    # Create the final merged output pipeline
    temp_file_path=Path(temp_folder,"media").with_suffix(file_details["file_ext"]).as_posix()
    output_merged = ffmpeg.output(
        # *outnodes, "pipe:", format=file_details["file_format"], **settings
        *outnodes, temp_file_path, **settings
    )
    return temp_file_path, output_merged


def get_file_details(temp_folder, filename: str, uri: str, duration=None):
    probe = ffmpeg.probe(uri)
    audio_stream = next(
        (stream for stream in probe["streams"] if stream["codec_type"] == "audio"), None
    )
    video_stream = next(
        (stream for stream in probe["streams"] if stream["codec_type"] == "video"), None
    )
    file_details = {
        "file_duration": (
            float(probe["format"]["duration"])
            if "duration" in probe["format"]
            else float(duration)
            if duration
            else None
        ),
        # "file_format": pick_output_format(filename, probe["format"]["format_name"]),
        "file_ext": Path(filename).suffix,
        # "has_audio": audio_stream != None,
        # "audio_duration": (
        #     (
        #         float(audio_stream["duration"])
        #         if "duration" in audio_stream
        #         else (float(fallback_duration) if fallback_duration else None)
        #     )
        #     if audio_stream
        #     else None
        # ),
        # "audio_codec": str(audio_stream["codec_name"]) if audio_stream else None,
        "has_video": video_stream != None,
        # "video_duration": (
        #     (
        #         float(video_stream["duration"])
        #         if "duration" in video_stream
        #         else (float(fallback_duration) if fallback_duration else None)
        #     )
        #     if video_stream
        #     else None
        # ),
        # "video_codec": str(video_stream["codec_name"]) if video_stream else None,
    }
    return file_details


# def pick_output_format(filename: str, formats: str):
#     if "," not in formats:
#         return formats
#     formats_list = formats.split(",")
#     if "mp4" in formats_list:
#         return "mp4"
#     if "matroska" in formats_list:
#         return "matroska"
#     return Path(filename).suffix.replace(".", "")


def redact_media_file(
    temp_folder: str, filename: str, signed_url: str, timings_list: list[dict], duration
):
    file_details = get_file_details(temp_folder, filename, signed_url, duration)
    temp_file_path,ffmpeg_pipeline = build_ffmpeg_pipeline(
        temp_folder, ffmpeg.input(signed_url), timings_list, file_details
    )
    try:
        ffmpeg.run(ffmpeg_pipeline, quiet=True, overwrite_output=True)
        return temp_file_path
    except ffmpeg._run.Error as e:
        logging.error(dumps({"ffmpeg_error": str(e.stderr, "utf-8")}, indent=2))
        abort(500, "media conversion failed for file: {}".format(filename))
