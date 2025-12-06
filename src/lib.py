import logging
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional, Tuple

import requests


class Node:
    def __init__(self, ip_address, hostname, interface):
        self.ip_address = ip_address
        self.hostname = hostname
        self.interface = interface

    def __repr__(self):
        return f"Node(hostname='{self.hostname}', ip='{self.ip_address}', interface='{self.interface}')"


class DatabaseInfo:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password


class StreamingInfo:
    def __init__(self, streaming_source, streaming_uri, streaming_resolution):
        self.streaming_source = streaming_source
        self.streaming_uri = streaming_uri
        self.streaming_resolution = streaming_resolution


class PrometheusServer:
    def __init__(self, ip, port, interface):
        self.ip = ip
        self.port = port
        self.interface = interface


class ClusterInfo:
    def __init__(
        self,
        master_node: Node,
        worker_nodes: List[Node] = None,
        database_info: DatabaseInfo = None,
        streaming_info: StreamingInfo = None,
        prom_server: PrometheusServer = None,
    ):
        self.master_node = master_node
        self.worker_nodes = worker_nodes if worker_nodes is not None else []
        self.database_info: DatabaseInfo = database_info
        self.streaming_info: StreamingInfo = streaming_info
        self.prom_server: PrometheusServer = prom_server
        self.prometheus_ip = f"http://{self.prom_server.ip}:{self.prom_server.port}"

    def add_worker(self, worker_node: Node):
        self.worker_nodes.append(worker_node)

    def __repr__(self):
        return f"Master: {self.master_node}, Workers: {self.worker_nodes}"


class CreateResultFile:
    @staticmethod
    def create(test_case: str, nodename: str, filename: str, header: str) -> str:
        """
        Creates a directory and file structure for storing curl results.
        The final path will be: result/{test_case/{nodename}/{filename}.csv

        Args:
            nodename: The name of the node, used as a subfolder.
            filename: The base name for the output .csv file.

        Returns:
            The full path to the created file.

        Raises:
            OSError: If the directory or file cannot be created due to permissions
                    or other OS-level issues.
        """
        # 1. Construct the full desired file path
        file_path = os.path.join("result", test_case, nodename, filename)

        # 2. Extract the directory portion of the path
        directory = os.path.dirname(file_path)

        # 3. Create the directories. If an error occurs, let it raise.
        # We log our intent before the operation that might fail.
        logging.info(f"Ensuring directory exists: '{directory}'")
        os.makedirs(directory, exist_ok=True)

        # 4. Create the file. The 'with' statement handles potential IOErrors.
        # This is a safe way to "touch" a file.
        with open(file_path, "a") as f:
            # header = "time_namelookup,time_connect,time_appconnect,time_pretransfer,time_redirect,time_starttransfer,time_total\n"
            f.write(header)
        logging.info(f"File ensured: '{file_path}'")

        return file_path

    @staticmethod
    def web_baseline(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="1_0_baseline",
            nodename=nodename,
            filename=filename,
            header="timestamp,cpu_usage(%),mem_usage(%),network(MBps)\n",
        )

    @staticmethod
    def web_curl(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="1_1_curl",
            nodename=nodename,
            filename=filename,
            header="time_namelookup,time_connect,time_appconnect,time_pretransfer,time_redirect,time_starttransfer,time_total\n",
        )

    @staticmethod
    def web_resource(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="1_2_resource_web",
            nodename=nodename,
            filename=filename,
            header="timestamp,cpu_usage(vCPU),mem_usage(Bytes),network_in(Bps),network_out(Bps)\n",
        )

    def web_curl_cold(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="1_3_curl_cold",
            nodename=nodename,
            filename=filename,
            header="time_namelookup,time_connect,time_appconnect,time_pretransfer,time_redirect,time_starttransfer,time_total\n",
        )

    @staticmethod
    def streaming_baseline(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="2_0_baseline",
            nodename=nodename,
            filename=filename,
            header="timestamp,cpu_usage(%),mem_usage(%),network(MBps)\n",
        )

    @staticmethod
    def streaming_timeToFirstFrame(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="2_1_timeToFirstFrame",
            nodename=nodename,
            filename=filename,
            header="time_to_first_frame\n",
        )

    @staticmethod
    def streaming_bitrate_fps(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="2_2_bitrate_fps",
            nodename=nodename,
            filename=filename,
            header="fps\n",
        )

    @staticmethod
    def streaming_resource(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="2_3_streaming_prom",
            nodename=nodename,
            filename=filename,
            header="timestamp,cpu_usage(mCPU),mem_usage(MB),network_in(MBps), network_out(MBps)\n",
        )

    @staticmethod
    def streaming_timeToFirstFrame_cold(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="2_4_timeToFirstFrame_cold",
            nodename=nodename,
            filename=filename,
            header="time_to_first_frame\n",
        )

    @staticmethod
    def yolo_detection_warm(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="3_1_yolo_warm",
            nodename=nodename,
            filename=filename,
            header="model_loading_time_ms,model_inference_ms, model_nms_ms, model_preprocess_ms, model_total_process_ms, total_server_time_ms, response_time_ms\n",
        )

    @staticmethod
    def yolo_detection_cold(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="3_2_yolo_cold",
            nodename=nodename,
            filename=filename,
            header="model_loading_time_ms,model_inference_ms, model_nms_ms, model_preprocess_ms, model_total_process_ms, total_server_time_ms, response_time_ms\n",
        )


def get_curl_metrics(url: str) -> dict | None:
    """
    Executes a curl command and returns the timing metrics as a dictionary.

    Args:
        url: The URL to test.

    Returns:
        A dictionary containing the timing metrics as floats.
    """
    # A machine-readable format: key:%{variable}\n
    # This makes parsing the output trivial.
    curl_format = """
    time_namelookup:%{time_namelookup}
    time_connect:%{time_connect}
    time_appconnect:%{time_appconnect}
    time_pretransfer:%{time_pretransfer}
    time_redirect:%{time_redirect}
    time_starttransfer:%{time_starttransfer}
    time_total:%{time_total}
    """
    # Set a 10-minute (600 seconds) timeout for the entire operation
    timeout_seconds = "600"
    command = [
        "curl",
        "--max-time",
        timeout_seconds,
        "-s",
        "-o",
        "/dev/null",
        "-w",
        curl_format,
        url,
    ]

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)

        # Parse the output into a dictionary
        metrics_dict = {}
        output_lines = result.stdout.strip().splitlines()
        for line in output_lines:
            key, value = line.split(":", 1)
            # Convert the value to a float
            metrics_dict[key.strip()] = float(value)

        logging.info("Collecting metrics successfully!")
        return metrics_dict

    except FileNotFoundError:
        logging.error(
            "Error: 'curl' command not found. Please ensure it is installed and in your PATH."
        )
        return None
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing curl command for {url}: {e.stderr}")
        return None
    except (ValueError, IndexError) as e:
        logging.error(f"Error parsing curl output: {e}")
        return None


def get_curl_metrics_and_body(url: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Executes a curl command and returns the timing metrics as a dictionary
    and the response body as a string.

    Args:
        url: The URL to test.

    Returns:
        A tuple containing (metrics_dict, response_body).
        (None, None) on failure.
    """

    # We define the format *precisely* with newlines.
    # The initial \n is CRITICAL to separate the metrics
    # from the response body, even if the body has no trailing newline.
    curl_format = (
        "\n"  # Ensures a new line before metrics start
        "time_namelookup:%{time_namelookup}\n"
        "time_connect:%{time_connect}\n"
        "time_appconnect:%{time_appconnect}\n"
        "time_pretransfer:%{time_pretransfer}\n"
        "time_redirect:%{time_redirect}\n"
        "time_starttransfer:%{time_starttransfer}\n"
        "time_total:%{time_total}"  # No final newline needed here
    )

    # We know this format has 7 metric lines
    num_metric_lines = 7

    # Set a 10-minute (600 seconds) timeout for the entire operation
    timeout_seconds = "600"
    command = [
        "curl",
        "--max-time",
        timeout_seconds,
        "-s",  # Silent mode
        # "-o", "/dev/null",  # <-- This line is REMOVED
        "-w",  # Write-out format
        curl_format,  # Pass the precise format string
        url,
    ]

    try:
        # Use encoding for reliable text conversion
        result = subprocess.run(
            command, capture_output=True, text=True, check=True, encoding="utf-8"
        )

        # stdout now contains:
        # 1. The response body
        # 2. The metrics (at the very end)

        all_output_lines = result.stdout.splitlines()

        if len(all_output_lines) < num_metric_lines:
            logging.error(
                f"Error parsing curl output: not enough lines. Got: {result.stdout}"
            )
            return None, None

        # Separate the body from the metrics
        body_lines = all_output_lines[:-num_metric_lines]
        metric_lines = all_output_lines[-num_metric_lines:]

        # Re-join the body. .splitlines() drops newlines, so we add them back.
        response_body = "\n".join(body_lines)

        # Parse the metrics into a dictionary
        metrics_dict = {}
        for line in metric_lines:
            key, value = line.split(":", 1)
            metrics_dict[key.strip()] = float(value)

        logging.info("Collecting metrics and body successfully!")
        return metrics_dict, response_body

    except FileNotFoundError:
        logging.error(
            "Error: 'curl' command not found. Please ensure it is installed and in your PATH."
        )
        return None, None
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing curl command for {url}: {e.stderr}")
        return None, None
    except (ValueError, IndexError) as e:
        # Log the end of the output, which is where parsing failed
        logging.error(
            f"Error parsing curl output: {e}. Output tail: {result.stdout[-200:]}"
        )
        return None, None


def query_url_post_image(url: str, image_path: str) -> dict | None:
    """
    Makes a POST request with an image file and retries on failure.

    Args:
        url: The url you want to query (e.g., the /predict endpoint).
        image_path: The local path to the image file to send.

    Returns:
        A dictionary with the JSON data on success, or None on failure.
    """
    logging.debug(f"Requesting URL: {url} with image {image_path}")

    while True:
        try:
            # Open the file in binary read mode ('rb')
            with open(image_path, "rb") as f:
                # 'image' is the key your Flask server will look for
                # in request.files. You can change 'image' to 'file'
                # or whatever your API expects.
                files = {"image": f}

                # Use requests.post() and pass the files
                response = requests.post(url, files=files)

            # Raise an exception for bad status codes (like 4xx or 5xx)
            response.raise_for_status()

            data = response.json()
            logging.debug(f"Successfully received response: {data}")
            return data  # Return the parsed JSON data

        except FileNotFoundError:
            # If the image isn't found, stop retrying.
            logging.error(f"Image file not found at {image_path}. Stopping.")
            return None

        except requests.exceptions.ConnectionError as e:
            # Network-level error, retry
            logging.error(f"A connection error occurred: {e}. Retrying...")
            time.sleep(2)

        except requests.exceptions.HTTPError as e:
            # Server returned a 4xx or 5xx error, retry
            logging.error(f"HTTP error: {e}. Retrying...")
            logging.error(f"Response text: {response.text}")
            time.sleep(2)

        except requests.exceptions.JSONDecodeError as e:
            # Server response wasn't valid JSON, retry
            logging.error(f"Failed to decode JSON: {e}. Retrying...")
            logging.error(f"Response text: {response.text}")
            time.sleep(2)

        except requests.exceptions.RequestException as e:
            # Catch any other request-related errors
            logging.error(f"An unexpected error occurred: {e}. Retrying...")
            time.sleep(2)


def get_time_to_first_frame_warm(
    url: str,
    wait_timeout: float = 6000.0,
) -> bool:
    """
    Polls an RTMP URL using ffmpeg until it successfully captures the first frame.

    Args:
        url: The RTMP URL (e.g., rtmp://host:1935/app/stream).
        wait_timeout: Maximum seconds to wait for the stream to become decodable.

    Returns:
        True if the stream was found and ffmpeg processed the first frame.
        False otherwise.
    """
    wait_start_time = time.monotonic()
    logging.debug(f"Polling for RTMP stream at: {url}")

    while True:
        # 1. Check global timeout
        elapsed_total = time.monotonic() - wait_start_time
        if elapsed_total > wait_timeout:
            logging.error(
                f"Timeout: Waited {elapsed_total:.2f}s, but RTMP stream at {url} never became decodable."
            )
            return False

        # 2. Calculate dynamic timeout for this specific ffmpeg attempt
        #    We give ffmpeg the remaining time, but minimally 2.0 seconds to attempt a handshake.
        current_attempt_timeout = max(2.0, wait_timeout - elapsed_total)

        command = [
            "ffmpeg",
            "-i", url,
            "-vframes", "1",       # Exit after processing the first video frame
            "-f", "null", "-",     # Discard output
            "-rw_timeout", str(int(wait_timeout * 1_000_000))
        ]

        try:
            # Attempt to grab the frame
            subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,
                timeout=current_attempt_timeout
            )
            
            # If we reach here, ffmpeg succeeded (exit code 0)
            logging.info(f"RTMP Stream found and decoded after {elapsed_total:.2f}s.")
            return True

        except subprocess.CalledProcessError as e:
            logging.warning(f"ffmpeg failed (Exit code {e.returncode}). Retrying...")
            
            # Log the specific error message from FFmpeg (if available)
            if e.stderr:
                # .strip() removes extra newlines at the end
                logging.warning(f"FFmpeg Error Log: {e.stderr.strip()}")
            
            time.sleep(0.5)
            continue

        except subprocess.TimeoutExpired:
            # FFmpeg hung (likely socket opened but no data flow)
            logging.warning("ffmpeg timed out waiting for data. Retrying...")
            continue

        except Exception as e:
            logging.error(f"Unexpected error running ffmpeg: {e}")
            return False


def get_time_to_first_frame(url: str, wait_timeout: float = 6000.0) -> float | None:
    """
    Waits for a stream to become available (HTTP 200) and then
    measures the time it takes for ffmpeg to connect and decode the first frame.

    Args:
        url: The URL of the video stream to test.
        wait_timeout: The maximum time in seconds to wait for the stream to become available before giving up.

    Returns:
        The time to the first frame in seconds (float),
        or None if an error occurred or the wait timed out.
    """
    # --- Part 1: Wait for the stream to be ready ---
    # This loop replicates: `while [ $(curl...) -ne 200 ]; do sleep 0.1; done`
    start_time = time.monotonic()
    logging.info(f"Waiting for stream to be ready at {url}...")
    i = 0
    while True:
        logging.info(f"Try connecting to streaming service [{i} times]")
        i += 1
        # Check if we've waited too long
        if time.monotonic() - start_time > wait_timeout:
            logging.error(
                f"Timeout: Waited longer than {wait_timeout}s for URL to be ready."
            )
            return None
        try:
            # Use a HEAD request for efficiency, just like `curl --head`
            response = requests.head(url, timeout=2)
            if response.status_code == 200:
                logging.info("Stream is ready (HTTP 200 OK). Proceeding with ffmpeg.")
                break  # Exit the loop and continue to ffmpeg
        except requests.RequestException as e:
            logging.warning(f"Connection failed, retrying in 1 second... Error: {e}")
            time.sleep(1)
            # Ignore connection errors and just retry
            pass

        time.sleep(2)

    # --- Part 2: Time the ffmpeg process ---
    # This is the `&& time ffmpeg ...` part
    command = [
        "ffmpeg",
        "-i",
        url,
        "-vframes",
        "1",  # Exit after processing the first frame
        "-f",
        "null",  # Discard the frame data
        "-",
    ]

    try:
        subprocess.run(
            command,
            capture_output=True,  # Captures stdout and stderr
            text=True,  # Decodes output as text
            check=True,  # Raises an error if ffmpeg fails
            timeout=30,  # Add a timeout for the ffmpeg process itself
        )

        end_time = time.monotonic()
        duration = end_time - start_time

        logging.info(
            f"Successfully processed first frame from '{url}' in {duration:.4f} seconds."
        )
        return duration

    except subprocess.CalledProcessError as e:
        logging.error(f"ffmpeg failed for URL: {url}")
        logging.error(f"FFmpeg STDERR:\n{e.stderr}")
        return None
    except subprocess.TimeoutExpired:
        logging.error(f"ffmpeg timed out for URL: {url}")
        return None
    except FileNotFoundError:
        logging.error(
            "ffmpeg command not found. Is it installed and in your system's PATH?"
        )
        return None

def get_fps_bitrate(stream_url, num_samples=100):
    """
    Analyzes a stream URL with FFmpeg and captures both FPS and Bitrate samples.

    Args:
        stream_url (str): The URL of the video stream to analyze.
        num_samples (int): The number of valid samples to collect.

    Returns:
        tuple: (fps_values, bitrate_values)
               - fps_values (list of floats): Captured FPS.
               - bitrate_values (list of floats): Captured Bitrate in kbits/s.
               Returns ([], []) if the process fails or times out.
    """

    # --- 1. Pre-flight Check: Wait for the stream to be ready ---
    # NOTE: urllib only supports HTTP/HTTPS. We skip this check for RTMP/RTSP
    # to prevent "unknown url type" errors.
    if stream_url.startswith(("http://", "https://")):
        STREAM_CHECK_TIMEOUT_SECONDS = 60  # Adjusted to 60s for sanity
        STREAM_CHECK_INTERVAL_SECONDS = 5
        stream_ready = False
        check_start_time = time.time()

        logging.info(f"Verifying stream URL is ready: {stream_url}")

        while time.time() - check_start_time < STREAM_CHECK_TIMEOUT_SECONDS:
            try:
                req = urllib.request.Request(stream_url, method="HEAD")
                req.add_header("User-Agent", "Mozilla/5.0")

                with urllib.request.urlopen(req, timeout=5) as response:
                    if response.status == 200:
                        logging.info("  -> Stream is ready (HTTP 200 OK).")
                        stream_ready = True
                        break
                    else:
                        logging.info(f"  -> Stream not ready (HTTP {response.status}). Retrying...")
            except (urllib.error.HTTPError, urllib.error.URLError, Exception) as e:
                logging.info(f"  -> Stream check failed ({e}). Retrying...")

            time.sleep(STREAM_CHECK_INTERVAL_SECONDS)

        if not stream_ready:
            logging.error("FATAL: Stream not ready (HTTP check failed). Aborting.")
            return [], []
    else:
        logging.info(f"Skipping HTTP pre-flight check for non-HTTP protocol: {stream_url}")

    # --- 2. FFmpeg Analysis ---
    STARTUP_TIMEOUT_SECONDS = 30
    INACTIVITY_TIMEOUT_SECONDS = 30

    command = [
        "ffmpeg",
        "-loglevel", "error",
        "-stats",       # Essential for getting progress info on stderr
        "-i", stream_url,
        "-f", "null",   # Null muxer (discard output)
        "-"
    ]

    logging.info(f"🚀 Analyzing stream to capture {num_samples} samples...")

    # Regex for FPS: matches "fps= 30" or "fps=30.5"
    fps_pattern = re.compile(r"fps=\s*([\d.]+)")
    # Regex for Bitrate: matches "bitrate= 1200.5kbits/s"
    bitrate_pattern = re.compile(r"bitrate=\s*([\d.]+)\s*kbits/s")

    fps_samples = []
    bitrate_samples = []
    all_stderr_lines = []

    process = None

    try:
        process = subprocess.Popen(
            command,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            encoding="utf-8",
            errors="replace",
            universal_newlines=True 
        )

        start_time = time.time()
        last_output_time = start_time

        while len(fps_samples) < num_samples:
            if process.poll() is not None:
                logging.warning("⚠️ FFmpeg process terminated unexpectedly.")
                break

            current_time = time.time()

            # Timeouts
            if not fps_samples and (current_time - start_time > STARTUP_TIMEOUT_SECONDS):
                logging.error("Timeout: No data received during startup.")
                break
            if fps_samples and (current_time - last_output_time > INACTIVITY_TIMEOUT_SECONDS):
                logging.warning("Timeout: Stream inactive.")
                break

            # Non-blocking read is hard in simple Python without threads/select, 
            # but readline() usually blocks until a newline or buffer fills. 
            # Given FFmpeg updates stats via \r, this relies on Python handling \r as newlines 
            # or FFmpeg outputting often enough.
            line = process.stderr.readline()
            
            if line:
                last_output_time = time.time()
                line_content = line.strip()
                all_stderr_lines.append(line_content)

                # Extract Data
                fps_match = fps_pattern.search(line_content)
                bitrate_match = bitrate_pattern.search(line_content)

                if fps_match and bitrate_match:
                    try:
                        val_fps = float(fps_match.group(1))
                        val_bitrate = float(bitrate_match.group(1))

                        # Simple filter: ignore 0 fps or 0 bitrate which often happens at start
                        if val_fps > 0 and val_bitrate > 0:
                            fps_samples.append(val_fps)
                            bitrate_samples.append(val_bitrate)

                            # Progress Bar
                            msg = f" -> Sample {len(fps_samples)}/{num_samples}: {val_fps:.1f} fps, {val_bitrate:.0f} kbits/s"
                            sys.stdout.write(f"\r{msg:<80}")
                            sys.stdout.flush()

                    except ValueError:
                        continue
            else:
                # Check if process is still alive if we got no line
                if process.poll() is not None:
                    break
                time.sleep(0.05) # Prevent busy loop if waiting for output

        print() # Newline after progress
        logging.info("✅ Sample collection finished.")

    except FileNotFoundError:
        logging.error("Error: 'ffmpeg' command not found.")
        return [], []
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return [], []
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()

    return fps_samples, bitrate_samples


def query_url(url: str, max_retries: int = 3) -> dict | None:
    """
    Makes a GET request with a retry limit and returns data or None.
    """
    logging.info(f"Requesting URL: {url}")

    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Check for 4xx/5xx errors
            data = response.json()
            logging.info(f"Successfully received response: {data}")
            return data  # Success!

        except requests.exceptions.JSONDecodeError as e:
            logging.error(
                f"Failed to decode JSON: {e}. Retrying ({retry_count + 1}/{max_retries})..."
            )
            logging.error(f"Server response text: {response.text}")

        except requests.exceptions.RequestException as e:
            logging.error(
                f"An unexpected error occurred: {e}. Retrying ({retry_count + 1}/{max_retries})..."
            )

        retry_count += 1
        time.sleep(2)

    logging.error(
        f"Failed to get a valid response from {url} after {max_retries} attempts."
    )
    return None  # Return None on total failure


def create_curl_result_file(nodename: str, filename: str) -> str:
    """
    Creates a directory and file structure for storing curl results.
    The final path will be: result/1_1_curl/{nodename}/{filename}.csv

    Args:
        nodename: The name of the node, used as a subfolder.
        filename: The base name for the output .csv file.

    Returns:
        The full path to the created file.

    Raises:
        OSError: If the directory or file cannot be created due to permissions
                 or other OS-level issues.
    """
    # 1. Construct the full desired file path
    file_path = os.path.join("result", "1_1_curl", nodename, f"{filename}")

    # 2. Extract the directory portion of the path
    directory = os.path.dirname(file_path)

    # 3. Create the directories. If an error occurs, let it raise.
    # We log our intent before the operation that might fail.
    logging.info(f"Ensuring directory exists: '{directory}'")
    os.makedirs(directory, exist_ok=True)

    # 4. Create the file. The 'with' statement handles potential IOErrors.
    # This is a safe way to "touch" a file.
    with open(file_path, "a") as f:
        header = "time_namelookup,time_connect,time_appconnect,time_pretransfer,time_redirect,time_starttransfer,time_total\n"
        f.write(header)
    logging.info(f"File ensured: '{file_path}'")

    return file_path


def create_resource_web_result_file(nodename: str, filename: str) -> str:
    """
    Creates a directory and file structure for storing curl results.
    The final path will be: result/1_2_resource_web/{nodename}/{filename}.csv

    Args:
        nodename: The name of the node, used as a subfolder.
        filename: The base name for the output .csv file.

    Returns:
        The full path to the created file.

    Raises:
        OSError: If the directory or file cannot be created due to permissions
                 or other OS-level issues.
    """
    # 1. Construct the full desired file path
    file_path = os.path.join("result", "1_2_resource_web", nodename, f"{filename}")

    # 2. Extract the directory portion of the path
    directory = os.path.dirname(file_path)

    # 3. Create the directories. If an error occurs, let it raise.
    # We log our intent before the operation that might fail.
    logging.info(f"Ensuring directory exists: '{directory}'")
    os.makedirs(directory, exist_ok=True)

    # 4. Create the file. The 'with' statement handles potential IOErrors.
    # This is a safe way to "touch" a file.
    with open(file_path, "a") as f:
        header = "timestamp,cpu_usage(%),mem_usage(%),network(MBps)\n"
        f.write(header)
    logging.info(f"File ensured: '{file_path}'")

    return file_path


def create_timeToFirstFrame_file(nodename: str, filename: str) -> str:
    """
    Creates a directory and file structure for storing curl results.
    The final path will be: result/2_1_timeToFirstFrame/{nodename}/{filename}.csv

    Args:
        nodename: The name of the node, used as a subfolder.
        filename: The base name for the output .csv file.

    Returns:
        The full path to the created file.

    Raises:
        OSError: If the directory or file cannot be created due to permissions
                 or other OS-level issues.
    """
    # 1. Construct the full desired file path
    file_path = os.path.join("result", "2_1_timeToFirstFrame", nodename, f"{filename}")

    # 2. Extract the directory portion of the path
    directory = os.path.dirname(file_path)

    # 3. Create the directories. If an error occurs, let it raise.
    # We log our intent before the operation that might fail.
    logging.info(f"Ensuring directory exists: '{directory}'")
    os.makedirs(directory, exist_ok=True)

    # 4. Create the file. The 'with' statement handles potential IOErrors.
    # This is a safe way to "touch" a file.
    with open(file_path, "a") as f:
        header = "time_namelookup,time_connect,time_appconnect,time_pretransfer,time_redirect,time_starttransfer,time_total\n"
        f.write(header)
    logging.info(f"File ensured: '{file_path}'")

    return file_path


def create_bitRate_fps_file(nodename: str, filename: str) -> str:
    """
    Creates a directory and file structure for storing curl results.
    The final path will be: result/2_1_timeToFirstFrame/{nodename}/{filename}.csv

    Args:
        nodename: The name of the node, used as a subfolder.
        filename: The base name for the output .csv file.

    Returns:
        The full path to the created file.

    Raises:
        OSError: If the directory or file cannot be created due to permissions
                 or other OS-level issues.
    """
    # 1. Construct the full desired file path
    file_path = os.path.join("result", "2_2_bitrate_fps", nodename, f"{filename}")

    # 2. Extract the directory portion of the path
    directory = os.path.dirname(file_path)

    # 3. Create the directories. If an error occurs, let it raise.
    # We log our intent before the operation that might fail.
    logging.info(f"Ensuring directory exists: '{directory}'")
    os.makedirs(directory, exist_ok=True)

    # 4. Create the file. The 'with' statement handles potential IOErrors.
    # This is a safe way to "touch" a file.
    with open(file_path, "a") as f:
        header = "bitrate, fps\n"
        f.write(header)
    logging.info(f"File ensured: '{file_path}'")

    return file_path


def create_streaming_prom_file(nodename: str, filename: str) -> str:
    """
    Creates a directory and file structure for storing curl results.
    The final path will be: result/2_1_timeToFirstFrame/{nodename}/{filename}.csv

    Args:
        nodename: The name of the node, used as a subfolder.
        filename: The base name for the output .csv file.

    Returns:
        The full path to the created file.

    Raises:
        OSError: If the directory or file cannot be created due to permissions
                 or other OS-level issues.
    """
    # 1. Construct the full desired file path
    file_path = os.path.join("result", "2_3_streaming_prom", nodename, f"{filename}")

    # 2. Extract the directory portion of the path
    directory = os.path.dirname(file_path)

    # 3. Create the directories. If an error occurs, let it raise.
    # We log our intent before the operation that might fail.
    logging.info(f"Ensuring directory exists: '{directory}'")
    os.makedirs(directory, exist_ok=True)

    # 4. Create the file. The 'with' statement handles potential IOErrors.
    # This is a safe way to "touch" a file.
    with open(file_path, "a") as f:
        header = "timestamp,cpu_usage(%),mem_usage(%),network(MBps)\n"
        f.write(header)
    logging.info(f"File ensured: '{file_path}'")

    return file_path


def create_streaming_stats_file(nodename: str, filename: str) -> str:
    """
    Creates a directory and file structure for storing curl results.
    The final path will be: result/2_1_timeToFirstFrame/{nodename}/{filename}.csv

    Args:
        nodename: The name of the node, used as a subfolder.
        filename: The base name for the output .csv file.

    Returns:
        The full path to the created file.

    Raises:
        OSError: If the directory or file cannot be created due to permissions
                 or other OS-level issues.
    """
    # 1. Construct the full desired file path
    file_path = os.path.join("result", "2_2_streaming_stats", nodename, f"{filename}")

    # 2. Extract the directory portion of the path
    directory = os.path.dirname(file_path)

    # 3. Create the directories. If an error occurs, let it raise.
    # We log our intent before the operation that might fail.
    logging.info(f"Ensuring directory exists: '{directory}'")
    os.makedirs(directory, exist_ok=True)

    # 4. Create the file. The 'with' statement handles potential IOErrors.
    # This is a safe way to "touch" a file.
    with open(file_path, "a") as f:
        header = "bitrate,fps\n"
        f.write(header)
    logging.info(f"File ensured: '{file_path}'")

    return file_path
