import os
import subprocess
import traceback
from pathlib import Path
from typing import Optional
from loguru import logger
from labelu.internal.common.config import settings


class VideoStreamer:
    """Utility class for converting videos to HLS streaming format"""

    @staticmethod
    def is_video_file(filename: str) -> bool:
        """Check if file is a video file"""
        video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.webm', '.m4v']
        return any(filename.lower().endswith(ext) for ext in video_extensions)

    @staticmethod
    def get_hls_path(original_path: str) -> str:
        """Get HLS playlist path for original video path"""
        path = Path(original_path)
        hls_dir = path.parent / f"{path.stem}_hls"
        return str(hls_dir / "playlist.m3u8")

    @staticmethod
    def get_converted_mp4_path(original_path: str) -> str:
        path = Path(original_path)
        return str(path.parent / "output.mp4")

    @staticmethod
    def get_hls_dir(original_path: str) -> Path:
        """Get HLS directory path for original video"""
        path = Path(original_path)
        return path.parent / f"{path.stem}_hls"

    @staticmethod
    async def convert_mp4(input_path: str, force_reconvert: bool = False) -> Optional[Path]:
        """
        Convert video to HLS format
        Returns the playlist path if successful, None if failed
        """
        try:
            input_file = Path(input_path)
            if not input_file.exists():
                logger.error(f"Input video file not found: {input_path}")
                return None

            output_path = input_file.parent / f"{input_file.name.split('.')[0]}_output.mp4"

            # cmd = f"ffmpeg -i '{str(input_file)}' -c:v libx264 -profile:v high -level 4.0 -pix_fmt yuv420p -r 30 -c:a aac -b:a 128k -y '{output_path}'"
            cmd = f"ffmpeg -i '{str(input_file)}' -c:v libx264 -crf 28 -preset slow -vf scale=-2:720 -an -movflags +faststart '{output_path}'"
            logger.info(f"Converting video to HLS: {input_path}, cmd: {cmd}")

            # Run FFmpeg conversion
            process = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )

            if process.returncode == 0:
                logger.info(f"Successfully converted video: {output_path}")
                return output_path
            else:
                logger.error(f"FFmpeg conversion failed: {process.stderr}")
                return None

        except subprocess.TimeoutExpired:
            logger.error(f"Video conversion timeout for {input_path}")
            return None
        except Exception as e:
            logger.error(f"Error converting video to HLS: {traceback.format_exc()}")
            # logger.error(f"Error converting video to HLS: {str(e)}")
            return None

    @staticmethod
    def get_streaming_url(original_path: str, base_url: str) -> str:
        """Get streaming URL for video"""
        if VideoStreamer.is_video_file(original_path):
            converted_mp4_path = VideoStreamer.get_converted_mp4_path(original_path)
            # Convert absolute path to relative URL path
            relative_path = os.path.relpath(converted_mp4_path, settings.MEDIA_ROOT)
            return f"{base_url}/{relative_path.replace(os.sep, '/')}"
        return f"{base_url}/{original_path.replace(os.sep, '/')}"
