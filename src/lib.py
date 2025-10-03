import logging
import re
import subprocess
import sys
import os
import requests
from typing import List, Optional, Dict, Iterator

import time


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
    def __init__(self, streaming_source, streaming_uri):
        self.streaming_source = streaming_source
        self.streaming_uri = streaming_uri


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
            header="timestamp,cpu_usage(%),mem_usage(%),network(MBps)\n",
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
            header="timestamp,cpu_usage(%),mem_usage(%),network_in(MBps), network_out(MBps)\n",
        )

    @staticmethod
    def streaming_timeToFirstFrame_cold(nodename: str, filename: str):
        return CreateResultFile.create(
            test_case="2_4_timeToFirstFrame_cold",
            nodename=nodename,
            filename=filename,
            header="time_to_first_frame\n",
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
    command = ["curl", "-s", "-o", "/dev/null", "-w", curl_format, url]

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


def old_get_time_to_first_frame(url: str) -> float | None:
    """
    Measures the time it takes for ffmpeg to connect to a stream
    and decode the first frame.

    Args:
        url: The URL of the video stream to test.

    Returns:
        The time to the first frame in seconds (float),
        or None if an error occurred.
    """
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
        start_time = time.monotonic()

        subprocess.run(
            command,
            capture_output=True,  # Captures stdout and stderr
            text=True,  # Decodes output as text
            check=True,  # Raises an error if ffmpeg fails
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
    except subprocess.TimeoutExpired as e:
        logging.error(f"ffmpeg timed out for URL: {url}")
        return None
    except FileNotFoundError:
        logging.error(
            "ffmpeg command not found. Is it installed and in your system's PATH?"
        )
        return None


def get_time_to_first_frame(url: str, wait_timeout: float = 30.0) -> float | None:
    """
    Waits for a stream to become available (HTTP 200) and then
    measures the time it takes for ffmpeg to connect and decode the first frame.

    Args:
        url: The URL of the video stream to test.
        wait_timeout: The maximum time in seconds to wait for the stream
                      to become available before giving up.

    Returns:
        The time to the first frame in seconds (float),
        or None if an error occurred or the wait timed out.
    """
    # --- Part 1: Wait for the stream to be ready ---
    # This loop replicates: `while [ $(curl...) -ne 200 ]; do sleep 0.1; done`
    wait_start_time = time.monotonic()
    logging.info(f"Waiting for stream to be ready at {url}...")
    while True:
        # Check if we've waited too long
        if time.monotonic() - wait_start_time > wait_timeout:
            logging.error(
                f"Timeout: Waited longer than {wait_timeout}s for URL to be ready."
            )
            return None
        try:
            # Use a HEAD request for efficiency, just like `curl --head`
            response = requests.head(url, timeout=2)
            if response.status_code == 200:
                logging.info(f"Stream is ready (HTTP 200 OK). Proceeding with ffmpeg.")
                break  # Exit the loop and continue to ffmpeg
        except requests.RequestException:
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
        start_time = time.monotonic()

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
    Analyzes a stream URL with FFmpeg and captures FPS samples from its output.

    This function builds the FFmpeg command internally and includes timeouts
    to handle slow-starting or stalling streams.

    Args:
        stream_url (str): The URL of the video stream to analyze.
        num_samples (int): The number of FPS samples to collect.

    Returns:
        list: A list of floats containing the captured FPS values. Returns an empty
              list if the process fails or times out before capturing any data.
    """
    # Construct the full FFmpeg command from the URL
    STARTUP_TIMEOUT_SECONDS = 30.0
    INACTIVITY_TIMEOUT_SECONDS = 15.0
    command = [
        "ffmpeg",
        # Suppress verbose logs but keep the one-line progress stats
        "-loglevel",
        "error",
        "-stats",
        "-i",
        stream_url,
        "-f",
        "null",  # Don't encode or save any output file
        "-",  # Pipe to stdout (though we discard it)
    ]

    logging.info(f"🚀 Analyzing stream to capture {num_samples} FPS samples...")
    logging.info(f"   URL: {stream_url}")
    logging.info(
        f"   (Startup timeout: {STARTUP_TIMEOUT_SECONDS}s, Inactivity timeout: {INACTIVITY_TIMEOUT_SECONDS}s)"
    )

    fps_pattern = re.compile(r"fps=\s*([\d.]+)")
    fps_samples = []

    try:
        # Start the FFmpeg process with the constructed command
        process = subprocess.Popen(
            command,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            encoding="utf-8",
            errors="replace",
        )

        start_time = time.time()
        last_output_time = start_time

        while len(fps_samples) < num_samples:
            if process.poll() is not None:
                logging.warning("⚠️ FFmpeg process terminated unexpectedly.")
                break

            current_time = time.time()
            if not fps_samples and (
                current_time - start_time > STARTUP_TIMEOUT_SECONDS
            ):
                logging.error(
                    f"Timeout: No FPS data received within the {STARTUP_TIMEOUT_SECONDS}s startup period. Stopping."
                )
                break
            if fps_samples and (
                current_time - last_output_time > INACTIVITY_TIMEOUT_SECONDS
            ):
                logging.warning(
                    f"Timeout: Stream inactive for over {INACTIVITY_TIMEOUT_SECONDS}s. Stopping."
                )
                break

            line = process.stderr.readline()

            if line:
                last_output_time = time.time()

                match = fps_pattern.search(line.strip())
                if match:
                    try:
                        fps_value = float(match.group(1))
                        fps_samples.append(fps_value)
                        progress_msg = f"  -> Captured sample {len(fps_samples)}/{num_samples}: {fps_value:.2f} fps"
                        sys.stdout.write(f"\r{progress_msg:<80}")
                        sys.stdout.flush()
                    except (ValueError, IndexError):
                        continue
            else:
                break

        print()
        logging.info("✅ Sample collection finished or was interrupted.")

    except FileNotFoundError:
        logging.error(
            "Error: 'ffmpeg' command not found. Make sure FFmpeg is installed and in your PATH."
        )
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        return []

    finally:
        if "process" in locals() and process.poll() is None:
            logging.info("🔪 Terminating FFmpeg process...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logging.warning("FFmpeg did not terminate gracefully, killing it.")
                process.kill()

    return fps_samples


def query_url(url: str) -> dict | None:
    """
    Makes a GET request and returns the response data as a dictionary.

    Args:
        url: The url you want to query.

    Returns:
        A dictionary with the JSON data on success, or None on failure.
    """

    logging.debug(f"Requesting URL: {url}")

    start_time = time.time()
    while True:
        try:
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()
            logging.debug(f"Successfully received response: {data}")
            return data  # Return the parsed JSON data

        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(2)


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
