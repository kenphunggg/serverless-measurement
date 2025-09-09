#!/bin/bash

# Set the default location of your video file
VIDEO_FILE="input.mp4"
# --------------------

# This function prints the help message
print_usage() {
    echo "Usage: $0 <resolution>"
    echo "  - Sets the streaming resolution."
    echo
    echo "Available resolutions:"
    echo "  2160p, 1440p, 1080p, 720p, 480p, 360p, 240p"
    echo
    echo "Example: $0 720p"
}

# Check if a resolution is provided
if [ "$#" -ne 1 ]; then
    print_usage
    exit 1
fi

# Check if the video file exists
if [ ! -f "$VIDEO_FILE" ]; then
    echo "Error: Video file not found at '$VIDEO_FILE'"
    echo "Please edit the script to set the correct file path."
    exit 1
fi

RESOLUTION=$1
SCALE=""

# Set the scale based on the resolution argument
case $RESOLUTION in
    "2160p") SCALE="scale=3840:2160";;
    "1440p") SCALE="scale=2560:1440";;
    "1080p") SCALE="scale=1920:1080";;
    "720p")  SCALE="scale=1280:720";;
    "480p")  SCALE="scale=854:480";;
    "360p")  SCALE="scale=640:360";;
    "240p")  SCALE="scale=426:240";;
    *)
        echo "Error: Unsupported resolution '$RESOLUTION'."
        echo
        print_usage
        exit 1
        ;;
esac

# --- MODIFICATION START ---
# Use a while loop to ensure the stream restarts if it ever stops.
while true
do
    echo "Starting stream for '$VIDEO_FILE' at $RESOLUTION ($SCALE)..."

    # Run the FFmpeg command
    ffmpeg -stream_loop -1 -re -i "$VIDEO_FILE" \
            -vf "$SCALE" \
            -c:v libx264 -preset veryfast -tune zerolatency \
            -b:v 64M \
            -c:a aac -b:a 128k \
            -f mpegts "srt://0.0.0.0:5000?mode=listener&listen_timeout=-1"

    echo "Stream interrupted. Restarting in 5 seconds..."
    sleep 5
done
# --- MODIFICATION END ---