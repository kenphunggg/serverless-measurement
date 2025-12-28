import logging
from math import log
import os
import re
import subprocess
import sys
import time
import urllib.error
import threading
import urllib.request
from typing import Dict, List, Optional, Tuple, Any
import select
import tempfile

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
        worker_nodes: List[Node],
        database_info: DatabaseInfo,
        streaming_info: StreamingInfo,
        prom_server: PrometheusServer,
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

    @staticmethod
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
            header="fps,bitrate(kbit/s)\n",
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

    @staticmethod
    def yolo_resource(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="3_3_yolo_resource",
            nodename=nodename,
            filename=filename,
            header="timestamp,cpu_usage(mCPU),mem_usage(MB),network_in(MBps), network_out(MBps)\n",
        )

    @staticmethod
    def text2text_warm(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="4_1_text2text",
            nodename=nodename,
            filename=filename,
            header="input_tokens,output_tokens,total_tokens,tokens_per_second, processing_time_second, response_time_ms\n",
        )

    @staticmethod
    def text2image_warm(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="4_2_text2image",
            nodename=nodename,
            filename=filename,
            header="image_size_bytes,processing_time_second,response_time_ms\n",
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


class KnativePinger:
    """
    Manages a background thread that continuously pings a Knative service
    to prevent it from scaling to zero.
    """

    def __init__(self, url: str, ping_interval: int = 5):
        self.url = url
        # Ping interval in seconds (default: 5s)
        self.ping_interval = ping_interval
        # Event used to signal the background thread to stop
        self._stop_event = threading.Event()
        # The actual thread object
        self._thread = threading.Thread(target=self._run_pinger, daemon=True)
        logging.info(
            f"Knative Pinger initialized for URL: {self.url} with interval: {self.ping_interval}s"
        )

    def _run_pinger(self):
        """The main loop executed by the background thread."""
        while not self._stop_event.is_set():
            try:
                # Send a non-blocking GET request
                response = requests.get(self.url, timeout=15)

                if response.status_code == 200:
                    logging.info(
                        f"Keep-Alive successful. Status: {response.status_code}"
                    )
                else:
                    logging.warning(
                        f"Keep-Alive received non-200 status: {response.status_code}"
                    )

            except requests.exceptions.RequestException as e:
                # Log an error if the request fails completely (connection refused, DNS error, etc.)
                logging.error(f"Keep-Alive request failed: {e}")

            # Wait for the interval OR until the stop event is set.
            # wait() returns True if the event is set, False if the timeout occurs.
            self._stop_event.wait(self.ping_interval)

    def start(self):
        """Starts the background pinging thread."""
        if not self._thread.is_alive():
            self._thread.start()
            logging.info("Knative Pinger thread started.")
        else:
            logging.warning("Pinger thread is already running.")

    def stop(self):
        """Signals the background thread to stop and waits for it to join."""
        logging.info("Stopping Knative Pinger thread...")
        self._stop_event.set()  # Set the event to break the while loop in _run_pinger

        # Wait for the thread to finish its current loop and terminate
        if self._thread.is_alive():
            self._thread.join()
            logging.info("Knative Pinger thread stopped successfully.")
        else:
            logging.warning("Pinger thread was not running.")


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
            "-i",
            url,
            "-vframes",
            "1",  # Exit after processing the first video frame
            "-f",
            "null",
            "-",  # Discard output
            "-rw_timeout",
            str(int(wait_timeout * 1_000_000)),
        ]

        try:
            # Attempt to grab the frame
            subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True,
                timeout=current_attempt_timeout,
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
    Measures the time it takes for ffmpeg to connect and decode the first frame,
    including the time spent waiting for the stream to become available.
    """
    start_time = time.monotonic()

    # --- Part 1 & 2 Combined: Time the polling ffmpeg process ---
    command = [
        # Ensure you use the absolute path if 'ffmpeg' is not in PATH
        "ffmpeg",
        "-i",
        url,
        "-vframes",
        "1",
        "-f",
        "null",
        "-",
        # Use ffmpeg's internal timeout to limit connection attempts, set it to the total remaining time
        "-rw_timeout",
        str(int(wait_timeout * 1_000_000)),
    ]

    try:
        logging.info(f"Attempting to connect and decode the first frame from {url}...")

        subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            # Set the Python timeout to the maximum allowed wait_timeout
            timeout=wait_timeout,
        )

        end_time = time.monotonic()
        duration = end_time - start_time

        logging.info(
            f"Successfully processed first frame from '{url}' in {duration:.4f} seconds."
        )
        return duration

    except subprocess.CalledProcessError as e:
        # ffmpeg failed (e.g., stream never appeared)
        logging.error(
            f"ffmpeg failed after {time.monotonic() - start_time:.2f}s for URL: {url}"
        )
        logging.error(f"FFmpeg STDERR:\n{e.stderr.strip()}")
        return None
    except subprocess.TimeoutExpired:
        # Python subprocess timeout exceeded the wait_timeout
        logging.error(f"ffmpeg timed out after {wait_timeout}s for URL: {url}")
        return None
    except FileNotFoundError:
        logging.error("ffmpeg command not found. Install it or use the absolute path.")
        return None


def get_fps_bitrate(
    stream_url: str, detection_time: int
) -> Tuple[List[float], List[float]]:
    """
    Runs FFmpeg for 'curltime' seconds, logs to a unique temp file,
    parses that file for FPS/Bitrate, returns data, and cleans up the file.
    """
    command = [
        "ffmpeg",
        "-i",
        stream_url,
        "-stats_period",
        "1",
        "-vf",
        "mpdecimate",
        "-fps_mode",
        "vfr",
        "-f",
        "mp4",
        "/dev/null",
        "-y",
    ]

    fps_list = []
    bitrate_list = []

    # 1. Create a temporary file
    # mkstemp returns a low-level file handle (fd) and the absolute path
    fd, log_filename = tempfile.mkstemp(suffix=".txt")
    os.close(
        fd
    )  # Close the low-level handle immediately; we'll let Python/subprocess open it by path

    try:
        logging.info(f"Created temporary log file at: {log_filename}")

        # 2. Run FFmpeg and write output to the temp file
        with open(log_filename, "w") as log_file:
            process = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT, text=True
            )

            try:
                logging.info(f"Collecting data for {detection_time} seconds...")
                process.wait(timeout=detection_time)
                logging.info("FFmpeg finished naturally.")
            except subprocess.TimeoutExpired:
                logging.warning(
                    f"Timeout of {detection_time}s reached. Terminating FFmpeg..."
                )
                process.terminate()
                process.wait()  # Wait for process to die to avoid zombies

        # 3. Read the file back to extract data
        if os.path.exists(log_filename):
            with open(log_filename, "r", errors="ignore") as f:
                content = f.read()

            # Regex to find fps=... and bitrate=...
            fps_matches = re.findall(r"fps=\s*([\d\.]+)", content)
            bitrate_matches = re.findall(r"bitrate=\s*([\d\.]+)", content)

            # Convert to floats
            fps_list = [float(x) for x in fps_matches]
            bitrate_list = [float(x) for x in bitrate_matches]

            logging.info(f"Parsed {len(fps_list)} samples.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        # Ensure subprocess is killed if main logic fails
        if "process" in locals() and process.poll() is None:
            process.terminate()

    finally:
        # 4. Cleanup: Delete the temporary file
        if os.path.exists(log_filename):
            try:
                os.remove(log_filename)
                logging.info(f"Deleted temporary file: {log_filename}")
            except OSError as e:
                logging.error(f"Error deleting temp file: {e}")

    return fps_list, bitrate_list


def query_url(url: str, max_retries: int = 3) -> dict | None:
    """
    Makes a GET request with a retry limit and returns data or None.
    """
    logging.info(f"Requesting URL: {url}")

    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get(url, timeout=None)
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
