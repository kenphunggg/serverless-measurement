# applications used in the measurement
## Abnomal detection app
### Data streaming
data streaming are docker images that constantly broadcast a flow of sensor data.  
Images:
+ for x86: ```mc0137/source_data:v1.4 sha256:277a9795e355933ced7097d6be29ef8fc83bbe330eee495bff4729dc70c7e658```
+ for ARM: ```mc0137/source_data:arm1.1 sha256:5e76454ad9926fc1864770433b75cd8a4745a47028ecb2a1c9255892623a1215``` 

Example of running cmds:
```
docker run -p 12345:12345 mc0137/source_data:v1.4 ( log)
docker run -d -p 12345:12345 mc0137/source_data:v1.4 (no log)
```
### Abnomal detection
this app uses ML to detect abnormal from the fetched sensor data, output is 1 or 0
Image:
+ for x86: mc0137/detect_abnormal:v1.4 sha256:3ba0c98c26a48d6afe4df6945551f4ac956f8c1fcb9f1837b3e9a8187f09d2d8
+ for ARM: mc0137/detect_abnormal:arm1.1 sha256:ea4866fffee1c5536c59e0850b4d8acbbdb655a4a575f6fbbe904a0e38e23a27
  
Example of running cmds:
```
docker run -p 8080:8080 mc0137/detect_abnormal:v1.4
```
  
APIs:
```
Detection:
http://<ip>:<port>/api/stream/<ip>:<port>/time_detect/time_sleep
examples:
http://<ip>:<port>/api/stream/<ip>:<port>/1/1
Termination:
http://<ip>:<port>/api/terminate
```

## Object detection app
### Video streaming
This app broadcast a stored video by ffmpeg to different qualities
Images:
+ for x86: `mc0137/video_stream:v1.2`
+ for ARM: `mc0137/video_stream:arm1.3`
  
How to run:
`docker run -p 1936:1935 -e RESOLUTION=<quali> mc0137/video_stream:v1.0`  
with:

```
<quali> as: 
4k => 2160p
2k => 2048p
fullHD => 1080p
HD => 720p
```  
### Object detection app
This app detects what kind of object in the fetched image/video using YOLO-v4 model  
Images:
+ for x86: ```docker.io/kiemtcb/detection-object:4.5x86@sha256:a3f295760c9b4d31f6455d013f1542c61ff1bfedf6042a125c70ed3c71f318cd```
+ for ARM: ```docker.io/kiemtcb/detection-object:4.5arm@sha256:71dca3f048f124cb395f5a165aef43c058cff94adffe44437af5d933d3d39c10```
+ version 4.7 may have updated png image processing! pls double check it  
  
This app releases a log file containing FPS counted every one second inside, check API to know how to retrieve it.  

APIs:
```
Terminate: http://<DNS or IP:PORT>/api/terminate
Checks container ready: http://<DNS or IP:PORT>/api/active
Stream with slow response: http://<DNS or IP:PORT>/api/stream/<streamingIP>:<streamingPort>/<time>
Stream with immediate  response: http://<DNS or IP:PORT>/api/stream/active/<streamingIP>:<streamingPort>/<time>  
Get log file: http://<DNS or IP:PORT>/download -o file{}.log
Detect picture: curl -F upload=@image.jpg http://<DNS or IP:PORT>/api/picture (only works with .jpg file format)  
Detect picture with detection result: curl -F upload=@image.jpg http://mec.default.svc.cluster.local/api/picture/return
```