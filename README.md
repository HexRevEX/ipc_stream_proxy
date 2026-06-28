# V380, Xiong Mai IPC Streams Proxy
V380, XM IP camera video/audio stream extractor/converter

This script enables connections to multiple IP cameras on a local network using the V380 and XM proprietary protocols. It forwards the captured audio and video streams to FFmpeg, allowing them to be streamed, recorded, transcoded, or processed using FFmpeg's extensive capabilities.

Requirements: FFmpeg must be installed and available in your system's PATH.

## Configuration

Application settings are stored in `settings.json`.

### `cameraSettings`

The `cameraSettings` dictionary contains the configuration for all IP cameras used by the application. Each camera entry must define the following fields:

| Field             | Description                                                               |
| ----------------- | ------------------------------------------------------------------------- |
| `applicationIP`   | Local IP address of the Python application that FFmpeg connects to.       |
| `videoPort`       | Local TCP port used by FFmpeg to receive the video stream.                |
| `audioPort`       | Local TCP port used by FFmpeg to receive the audio stream.                |
| `camIP`           | Local IP address of the camera.                                           |
| `camPort`         | Camera TCP port.                                                          |
| `camId`           | Camera identifier (used by the V380 protocol).                            |
| `camProtocol`     | Camera protocol (e.g. `v380` or `xm`).                                    |
| `camUserName`     | Camera username.                                                          |
| `camUserPassword` | Camera password.                                                          |
| `camHD`           | Video quality. Set to `true` to request the highest available resolution. |
| `camEnabled`      | Set to `true` to enable and connect to this camera.                       |

---

### `ffmpegSettings`

The `ffmpegSettings` dictionary contains the configuration for one or more FFmpeg instances. Each FFmpeg instance connects to the corresponding TCP ports defined in `cameraSettings`.

| Field                | Description                                                                     |
| -------------------- | ------------------------------------------------------------------------------- |
| `applicationIP`      | Local IP address of the Python application that FFmpeg connects to.             |
| `videoPort`          | Local TCP port from which FFmpeg receives the video stream.                     |
| `audioPort`          | Local TCP port from which FFmpeg receives the audio stream.                     |
| `ffmpegOutputSuffix` | String appended to the output filename to uniquely identify the generated file. |
| `ffmpegEnabled`      | Set to `true` to launch FFmpeg with this configuration.                         |

## ❤️ Support

This project is developed in my free time.

If it saves you time or helps your work or if you want to add new ipc support, you can support further development with a donation

### Cryptocurrency

- **USDT (TRC20)** TUUZ8joXMZffHJ8nzDjVXNschHVcra5b2n
- **Ethereum / ERC-20:** 0xC0C5d618A19042440B032e374FfFfAfF5328C9C3
- **Bitcoin (BTC):** bc1q0ckgp3mpddkl2077s3w0kjgkdlj9eyr9dfq5k7

Every contribution helps improve the project. Thank you!



