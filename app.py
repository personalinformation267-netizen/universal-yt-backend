import os
import sys
import uuid
import time
import json
import logging
import threading
import shutil
import glob
import subprocess
from flask import Flask, request, jsonify, send_from_directory, url_for
from flask_cors import CORS
import yt_dlp

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Backend")

# ============================================================
# CONFIGURATION
# ============================================================
# ABSOLUTE PATHS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
TEMP_DIR = os.path.join(BASE_DIR, "temp")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# FLASK APP
# Serve 'downloads' folder as static
app = Flask(__name__, static_folder=None) 

# CORS
# 1. ENABLE CORS GLOBALLY
# Allow all origins "*"
CORS(app, resources={r"/*": {"origins": "*"}})

# FFMPEG
FFMPEG_PATH = "ffmpeg" # Assumes ffmpeg is in PATH (common on Render)

# JOBS STORAGE
JOBS = {}

# ============================================================
# UTILITIES
# ============================================================
def format_size(bytes_val):
    if not bytes_val: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} TB"

def update_job(job_id, status, **kwargs):
    if job_id not in JOBS:
        JOBS[job_id] = {}
    JOBS[job_id]["status"] = status
    JOBS[job_id].update(kwargs)
    JOBS[job_id]["updated_at"] = time.time()

# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "service": "Universal YT Backend",
        "version": "1.0.0"
    })

# 3. Public download serving route
# 4. SERVE DOWNLOADED FILES PUBLICLY
@app.route("/files/<path:filename>")
def serve_files(filename):
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)

# Alias for old frontend compatibility if needed, but requirements requested /files/
@app.route("/downloads/<path:filename>")
def serve_download_alias(filename):
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)

@app.route("/api/analyze", methods=["POST"])
def analyze():
    try:
        data = request.json
        url = data.get("url")
        if not url:
            return jsonify({"status": "error", "message": "No URL provided"}), 400

        logger.info(f"Analyzing: {url}")
        
        ydl_opts = {
            'quiet': True,
            'noplaylist': True,
            'extract_flat': False,
            'cookiefile': None,
            # 'cookiesfrombrowser': ('chrome',), # Can be unstable on server environments like Render
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
        # Parse Info
        formats = info.get('formats', [])
        
        # Qualities
        # Collect distinct video-only or muxed streams by resolution
        # We need accurate filesize.
        video_options = {}
        
        for f in formats:
            if f.get('vcodec') != 'none':
                height = f.get('height')
                if not height: continue
                
                # We prefer 'mp4' usually, or 'webm' if better quality
                # Let's verify filesize
                fs = f.get('filesize') or f.get('filesize_approx') or 0
                label = f"{height}p"
                
                # Make a key based on resolution + ext to offer choices? 
                # Or just resolution? Requirements: "Available video qualities with accurate file sizes"
                # Simplest is one option per resolution (best filesize/codec).
                
                if height not in video_options:
                    video_options[height] = {
                        "quality": label,
                        "height": height,
                        "filesize_bytes": fs,
                        "filesize": format_size(fs),
                        "format_id": f['format_id'],
                        "ext": f.get('ext')
                    }
                else:
                    # Update if this one is 'better' (e.g. mp4 vs webm, or larger filesize usually means higher bitrate)
                    # Or just keep the first found (usually lowest quality in list? No, formats are sorted).
                    # yt-dlp sorts worst to best. So overwriting usually keeps best.
                    video_options[height] = {
                        "quality": label,
                        "height": height,
                        "filesize_bytes": fs,
                        "filesize": format_size(fs),
                        "format_id": f['format_id'],
                        "ext": f.get('ext')
                    }

        sorted_qualities = sorted(video_options.values(), key=lambda x: x['height'], reverse=True)
        
        # Audio Tracks
        # Look for unique languages in audio-only streams
        audio_map = {} # lang -> {details}
        for f in formats:
            if f.get('vcodec') == 'none' and f.get('acodec') != 'none':
                lang = f.get('language') or 'Unknown'
                if lang not in audio_map:
                    audio_map[lang] = {
                        "lang": lang,
                        "format_id": f['format_id'] # Store one format_id, preferably m4a/aac
                    }
        
        audio_list = sorted(list(audio_map.values()), key=lambda x: x['lang'])
        
        # Subtitles
        # Check 'subtitles' and 'automatic_captions' if needed, mostly 'subtitles'
        subs_list = sorted(list(info.get('subtitles', {}).keys()))
        
        response = {
            "status": "success",
            "title": info.get('title'),
            "thumbnail": info.get('thumbnail'),
            "channel": info.get('uploader'),
            "duration": info.get('duration'), # seconds
            "qualities": sorted_qualities,
            "audio_tracks": audio_list,
            "subtitles": subs_list
        }
        return jsonify(response)

    except Exception as e:
        logger.error(f"Analyze Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/download", methods=["POST"])
def download():
    try:
        data = request.json
        if not data:
            return jsonify({"status": "error", "message": "No data"}), 400
            
        url = data.get("url")
        if not url:
            return jsonify({"status": "error", "message": "URL required"}), 400
            
        # Parameters
        job_id = str(uuid.uuid4())
        dl_type = data.get("type", "mp4") # mp4 or mp3
        quality_fid = data.get("quality") # video format id
        audio_langs = data.get("audio", []) # list of langs
        sub_langs = data.get("subtitle", []) # list of langs
        
        # We need the host URL to construct the download link
        host_url = request.host_url # e.g. https://myapp.onrender.com/
        
        update_job(job_id, "queued", percentage=0, message="queued")
        
        # Spawn Thread
        t = threading.Thread(
            target=process_download,
            args=(job_id, url, dl_type, quality_fid, audio_langs, sub_langs, host_url)
        )
        t.daemon = True
        t.start()
        
        return jsonify({
            "status": "success",
            "job_id": job_id,
            "message": "Download started"
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/progress/<job_id>")
def progress(job_id):
    if job_id in JOBS:
        return jsonify(JOBS[job_id])
    return jsonify({"status": "error", "message": "Job not found"}), 404

# ============================================================
# WORKER LOGIC
# ============================================================
def process_download(job_id, url, dl_type, video_fid, audio_langs, sub_langs, host_url):
    job_work_dir = os.path.join(TEMP_DIR, job_id)
    os.makedirs(job_work_dir, exist_ok=True)
    
    try:
        logger.info(f"[{job_id}] Processing {dl_type} for {url}")
        update_job(job_id, "processing", percentage=5, message="Starting...")
        
        ydl_common = {
            'quiet': True,
            'outtmpl': os.path.join(job_work_dir, '%(title)s.%(ext)s'),
            'ffmpeg_location': FFMPEG_PATH
        }

        final_filename = f"download_{job_id}.{dl_type}"
        final_path = os.path.join(DOWNLOAD_DIR, final_filename)

        if dl_type == 'mp3':
            # MP3 MODE
            # Download best audio and convert
            update_job(job_id, "downloading", percentage=20, message="Downloading audio...")
            
            opts = ydl_common.copy()
            opts.update({
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
            })
            
            # Remove cookiesfrombrowser for production stability or use 'cookies' file if needed
            # opts['cookiesfrombrowser'] = ('chrome',) 
            
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
                
            # Find the mp3
            files = glob.glob(os.path.join(job_work_dir, "*.mp3"))
            if not files:
                raise Exception("MP3 conversion failed")
            
            # Move to downloads
            shutil.move(files[0], final_path)
            
        else:
            # MP4 MODE
            # 1. Video
            update_job(job_id, "downloading_video", percentage=10, message="Downloading video...")
            
            v_opts = ydl_common.copy()
            v_opts['format'] = video_fid if video_fid else 'bestvideo[ext=mp4]/bestvideo'
            v_opts['outtmpl'] = os.path.join(job_work_dir, "video.%(ext)s")
            
            with yt_dlp.YoutubeDL(v_opts) as ydl:
                ydl.download([url])
                
            v_files = glob.glob(os.path.join(job_work_dir, "video.*"))
            if not v_files: raise Exception("Original video download failed")
            video_file = v_files[0]
            
            inputs = [video_file]
            
            # 2. Audio(s)
            
            audio_files = []
            
            if not audio_langs:
                 # Default audio if none selected (to avoid silent video)
                 # Or maybe the user selected a video format that already includes audio?
                 # If video_fid corresponds to video-only, we need audio.
                 # Let's download 'bestaudio' as default.
                 logger.info("No audio selected, fetching default best audio")
                 a_opts = ydl_common.copy()
                 a_opts['format'] = 'bestaudio'
                 a_opts['outtmpl'] = os.path.join(job_work_dir, "audio_default.%(ext)s")
                 with yt_dlp.YoutubeDL(a_opts) as ydl:
                     ydl.download([url])
                 audio_files.append(glob.glob(os.path.join(job_work_dir, "audio_default.*"))[0])
            
            for idx, lang in enumerate(audio_langs):
                update_job(job_id, "downloading_audio", percentage=30, message=f"Downloading audio ({lang})...")
                # Try to get best audio for this language
                # Fallback to just bestaudio if specific lang not found/tagged?
                # Using format filter
                a_fmt = f"bestaudio[language={lang}]/bestaudio" 
                
                a_opts = ydl_common.copy()
                a_opts['format'] = a_fmt
                a_opts['outtmpl'] = os.path.join(job_work_dir, f"audio_{idx}.%(ext)s")
                
                try:
                    with yt_dlp.YoutubeDL(a_opts) as ydl:
                        ydl.download([url])
                    found = glob.glob(os.path.join(job_work_dir, f"audio_{idx}.*"))
                    if found:
                        audio_files.append(found[0])
                except Exception as e:
                    logger.warning(f"Could not download audio for {lang}: {e}")
            
            # 3. Subtitles
            sub_files = [] # (path, lang)
            if sub_langs:
                update_job(job_id, "downloading_subs", percentage=50, message="Downloading subtitles...")
                s_opts = ydl_common.copy()
                s_opts['skip_download'] = True
                s_opts['writesubtitles'] = True
                s_opts['subtitleslangs'] = sub_langs
                s_opts['outtmpl'] = os.path.join(job_work_dir, "subs")
                
                with yt_dlp.YoutubeDL(s_opts) as ydl:
                    ydl.download([url])
                
                # Identify downloaded subs
                for lang in sub_langs:
                    # formats: subs.lang.vtt, etc.
                    s_found = glob.glob(os.path.join(job_work_dir, f"subs.{lang}.*"))
                    if s_found:
                        sub_files.append((s_found[0], lang))

            # 4. Merge
            update_job(job_id, "merging", percentage=80, message="Merging...")
            
            # Construct FFmpeg command
            cmd = [FFMPEG_PATH, '-y']
            
            maps = [] # collect maps
            input_counter = 0
            
            # Video Input
            cmd.extend(['-i', video_file])
            maps.extend(['-map', f'{input_counter}:v'])
            input_counter += 1
            
            # Audio Inputs
            for af in audio_files:
                cmd.extend(['-i', af])
                maps.extend(['-map', f'{input_counter}:a'])
                input_counter += 1
                
            # Subtitle Inputs
            for sf, sl in sub_files:
                cmd.extend(['-i', sf])
                maps.extend(['-map', f'{input_counter}:s'])
                # Metadata
                # stream specifier relative to output file's stream indices can be tricky
                # safe way: -metadata:s:s:N language=...
                # We simply append metadata args later or rely on map order
                input_counter += 1
            
            cmd.extend(maps)
            
            # Subtitle Language Metadata
            # Calculate stream index for subs. If we have V video streams and A audio streams...
            # The -map order defines the output stream order.
            # 1 Video + N Audios + M Subs
            # Subtitles start at index 1 + len(audio_files)
            
            current_sub_idx = 0
            for sf, sl in sub_files:
                cmd.extend([f'-metadata:s:s:{current_sub_idx}', f'language={sl}'])
                current_sub_idx += 1
            
            cmd.extend(['-c:v', 'copy', '-c:a', 'aac', '-c:s', 'mov_text'])
            cmd.append(final_path)
            
            logger.info(f"FFmpeg CMD: {cmd}")
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Cleanup
        shutil.rmtree(job_work_dir, ignore_errors=True)
        
        # Result
        # Using /files/ route as requested
        dl_url = f"{host_url}files/{final_filename}"
        
        update_job(job_id, "completed", 
            percentage=100, 
            message="Ready", 
            filename=final_filename,
            download_url=dl_url
        )
        logger.info(f"[{job_id}] Success: {dl_url}")

    except Exception as e:
        logger.error(f"[{job_id}] Error: {e}")
        update_job(job_id, "error", message=str(e))
        shutil.rmtree(job_work_dir, ignore_errors=True)

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
