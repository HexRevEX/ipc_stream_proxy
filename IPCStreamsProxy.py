#!/usr/bin/env python3

"""
MIT License

Copyright (c) 2026 HexRevEx

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import asyncio
import base64
from Crypto.Cipher import AES  
from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA
from dataclasses import dataclass, field, asdict
from dataclasses_json import dataclass_json
from datetime import datetime
import hashlib
import json
import os
import struct
import sys

__version__ = "1.0.2"
__app_header__ = f"\n V380/XM IPC Streams Proxy v{__version__}\n"

if sys.version_info < (3, 12):
    raise RuntimeError("This script requires Python 3.12 or higher")

class SafeDict(dict):
    def __missing__(self, key):
        return f'{{{key}}}'  # save placeholder in a string

@dataclass_json
@dataclass
class Settings:
    applicationIP: str = field(default="192.168.1.2")
    videoPort: int = field(default=9765)
    audioPort: int = field(default=9766)

@dataclass_json
@dataclass
class CameraSettings(Settings):
    camIP: str = field(default="192.168.1.3")
    camPort: int = field(default=8800)
    camId:str = field(default="12345678")
    camProtocol:str = field(default = "v380")
    camUserName:str = field(default="admin")
    camUserPassword: str = field(default="admin")
    camHD: bool = field(default=True)
    camEnabled:bool = field(default =True)

@dataclass_json
@dataclass
class FFMpegSettings(Settings):
    ffmpegOutputSuffix:str = field(default = "av")
    ffmpegEnabled:bool = field(default=True)
    ffmpegOutputFolder:str = field(default = os.path.dirname(os.path.abspath(__file__)))
    ffmpegOutputFileFormat:str = field(default = "%03d.mp4")
    ffmpegCommand: str = field(default="ffmpeg")
    ffmpegEnabled:bool = field(default =True)

    async def get_real_command(self):
        # m3u8 output must have fixed path for URL
        date_time = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_" if "m3u8" not in self.ffmpegOutputFileFormat.lower() else ""
        output = os.path.join(self.ffmpegOutputFolder, f"{self.ffmpegOutputSuffix}_{date_time}{self.ffmpegOutputFileFormat}")
        result = self.ffmpegCommand.format_map(SafeDict(asdict(self))).format(output_file=output).replace("\'","\"")
        print(f"ffmpeg cmd: {result}")
        return result

@dataclass_json
@dataclass
class AppSettings:

    cameraSettings:dict[str, CameraSettings] = field(default_factory=lambda: {"Camera1": CameraSettings()})   
    ffmpegSettings:dict[str, FFMpegSettings] = field(default_factory=lambda: {"FFMpeg1": FFMpegSettings()})   

    def saveSettings(self, file_path: str) -> None:
        with open(file_path, "w") as f:
            f.write(self.to_json(indent=4))

    @classmethod
    def loadSettings(cls, file_path: str) -> "Settings":
        with open(file_path, "r") as f:
            return cls.from_json(f.read())


class CamAgent:
    
    def __init__(self, firstKey: bytes, cameraSettings:CameraSettings):

        self.firstKey = firstKey
        self.stopEvent = asyncio.Event()
        self.cameraSettings = cameraSettings
        self.tcpClients = set()
        self.videoFramesQueues = set()
        self.audioFramesQueues = set()
        self.lockVideoFramesQueues = asyncio.Lock()
        self.lockAudioFramesQueues = asyncio.Lock()
        self.framesQueque = asyncio.Queue()
    
    async def send(self, data: bytes, writer):
        writer.write(data)
        await writer.drain()

    async def recv(self, reader, size: int = 4096) -> bytes:
        return await reader.read(size)

    async def exchange(self, data: bytes, reader, writer) -> bytes:
        await self.send(data, writer)
        return await self.recv(reader)

    async def handleClient(self, writer, framesQueue: asyncio.Queue):
        try:
           while not self.stopEvent.is_set():

                frame = await framesQueue.get()
                framesQueue.task_done()

                if frame is None:
                   continue

                writer.write(frame)  
                await writer.drain()
            
        except Exception as e:
           print(f"Handle client error: {e}")

    async def acceptClient(self, reader, writer, framesQueues, lockFramesQueues):
        try:
            addr = writer.get_extra_info("peername")
            print(f"[+] New connection: {addr}")

            newFramesQueue = asyncio.Queue()

            async with lockFramesQueues:
                framesQueues.add(newFramesQueue)
            senderTask = asyncio.create_task( self.handleClient(writer, newFramesQueue) )
            self.tcpClients.add((writer, senderTask))
       
        except Exception as e:
           print(f"Accept Connection error: {e}")

        try:
            while not reader.at_eof():
                data = await reader.read(4096)

                if not data:
                    break

                print(f"{addr}: {data.hex()}")

        except Exception as e:
            print(f"{addr}: {e}")

        finally:
            print(f"Disconnected: {addr}")
            senderTask.cancel()

        try:
            await senderTask
        except asyncio.CancelledError:
            pass

        self.tcpClients.discard((writer, senderTask))
        
        async with lockFramesQueues:
            framesQueues.discard(newFramesQueue)

        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()

    async def acceptAudioClient(self, reader, writer):
        await self.acceptClient( reader, writer, self.audioFramesQueues, self.lockAudioFramesQueues )

    async def acceptVideoClient(self, reader, writer):
        await self.acceptClient( reader, writer, self.videoFramesQueues, self.lockVideoFramesQueues )

    async def stop(self):
        self.stopEvent.set()

        for writer, task in list(self.tcpClients):
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def startServers(self):
        try:
            videoServer = await asyncio.start_server( self.acceptVideoClient, self.cameraSettings.applicationIP, self.cameraSettings.videoPort)
            audioServer = await asyncio.start_server( self.acceptAudioClient, self.cameraSettings.applicationIP, self.cameraSettings.audioPort)

            while  not self.stopEvent.is_set() :
               (frame, isVideoFrame) = await self.framesQueque.get()
               self.framesQueque.task_done()
               
               if isVideoFrame:
                        for q in self.videoFramesQueues:
                            async with self.lockVideoFramesQueues:
                                await q.put(frame)
               else:
                        for q in self.audioFramesQueues:
                            async with self.lockAudioFramesQueues:
                                await q.put(frame)

            async with audioServer, videoServer:
                await self.stopEvent.wait()           
        except Exception as e:
           print(f"Start server error: {e}")
        finally:
           await self.stop()
    
    async def initStream(self):
        raise NotImplementedError("initStream() method must be implemented")
    
class CamAgentV380(CamAgent):

    def __init__(self, cameraSettings:CameraSettings):

        super().__init__(firstKey = b'macrovideo+*#!^@',cameraSettings=cameraSettings)
        self.streamKey = None 
        self.STREAMKEYPART2 = b'\x00\x00\x5C\x79\x14\x2C\x46\x23\x81\x61\xF0\x0D\x80\x82'
        self.rawQueue   = asyncio.Queue()
        self.encryptedFramesQueque = asyncio.Queue()

    async def decryptVideoFrame(self, data: bytes, key: bytes) -> bytes:

        return bytes([
            x for i in range(0,len(data),80) 
            for x in 
                (
                    AES.new(key, AES.MODE_ECB).decrypt(data[i:i+64]) if len(data[i:i+64]) == 64 
                    else data[i:i+64]
                ) 
                + data[i+64:i+80]
            ])

    async def decryptAudioFrame(self, audioPacket:bytes,key:bytes) -> bytes:
        return AES.new(key, AES.MODE_ECB).decrypt(audioPacket)

    async def encrypt(self, data:bytes, key:bytes) -> bytes:
        return AES.new(key, AES.MODE_ECB).encrypt(data)

    async def encryptPassword(self,password:str) -> bytes:
        bPassword=password[:31] # ??? max password length is 31 chars 
        authKey = os.urandom(16)
        paddingChar   = '\x10' if len(bPassword) == 16 else '\x00'
        paddingLength = (32 if len(bPassword) < 16 else 48) - len(bPassword)
        paddedPassword = (bPassword + paddingChar * paddingLength).encode('ascii')
        return authKey + await self.encrypt( await self.encrypt(paddedPassword,self.firstKey),authKey)

    async def writeRawToQueue(self, sleepBetweenTries:int = 5):
        try:
            while not self.stopEvent.is_set() :
                try:
                    try:
                        data = await self.recv(self.streamReader)
                        await self.rawQueue.put(data)
                    except Exception as e:
                        print(f"Convert raw data to buffer error: {e}")
                        
                        # remove stream data for the next connection
                        while not self.rawQueue.empty():
                            self.rawQueue.get_nowait()
                            self.rawQueue.task_done()
                        await self.disconnect()
                        await self.initConnection()

                except:
                    await asyncio.sleep(sleepBetweenTries)
                    pass
        finally:
            await self.disconnect()
            await self.stopEvent.set()

    async def convertRawQueueToEncryptedFrames(self):
        
        previousFrameID = -1
        packetData = bytearray()
        frameData = bytearray()
        
        try:
            while not self.stopEvent.is_set():
                
                # read packet header
                while len(packetData)<12:
                    packetData.extend(await self.rawQueue.get())
                    self.rawQueue.task_done()
                
                header = bytes(packetData[:12])    
                packetDataSize  = struct.unpack('<H', header[7:9])[0]
                frameID   = header[2]

                # read packet payload                        
                while len(packetData)-12<packetDataSize:
                    packetData.extend(await self.rawQueue.get())
                    self.rawQueue.task_done()

                packetPayload = bytes(packetData[12:12+packetDataSize])
                
                del packetData[:12+packetDataSize] 
               
                if previousFrameID not in (-1, frameID): # new frame
                    await self.encryptedFramesQueque.put(frameData)

                    previousFrameID = frameID
                    frameData = bytearray(packetPayload)
                else:
                    frameData.extend(packetPayload)
                    previousFrameID = frameID

        except Exception as e:
            print(f"Convert Buffer To Encrypted Frames error: {e}")
        finally:
            await self.stopEvent.set()

    async def convertEncryptedFramesToDecryptedFrames(self):
        try:
            while not self.stopEvent.is_set() :
                try:
                    item = await self.encryptedFramesQueque.get()
                    self.encryptedFramesQueque.task_done()
                except:
                    continue
                
                if not len(item):
                    continue

                header = item[:16]
                frameTypeID = int.from_bytes(header[4:5], byteorder='little')
                frameData = item[16:]
                frameDataDecrypted = b''

                # videoframes
                if frameTypeID in (0, 1, 40, 41): # v380 H264 and HEVC I-frame and P-frame type IDs
                    if len(frameData)>=64:
                        frameDataDecrypted = await self.decryptVideoFrame(frameData, self.streamKey)
                    else:
                        frameDataDecrypted = frameData

                    await self.framesQueque.put((frameDataDecrypted, True)) # True - videoframe

                # audioframes
                elif frameTypeID == 22: 
                    frameDataDecrypted = await self.decryptAudioFrame(frameData, self.streamKey)

                    if len(frameDataDecrypted)>0:
                        await self.framesQueque.put((frameDataDecrypted[3:], False)) # False - audioframe
                    else:
                        print(f"Null audio packet of  {frameData}")

        except Exception as e:
            print(f"[3]-{e}")
        finally:
            await self.stopEvent.set()    

    async def createAuthenticationRequest(self):
        camera_password = await self.encryptPassword(self.cameraSettings.camUserPassword)
        dateTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S").encode('ascii')
        camId = struct.pack('<I', int(self.cameraSettings.camId)) 
        cameraUser =   self.cameraSettings.camUserName.encode('ascii')

        return  b'\x8f\x04\x00\x00\xfe\x03\x00\x00\x1f\x01\x00\x00\x00'+\
                camId+\
                dateTime+\
                b'\x00' * 13+\
                cameraUser+\
                b'\x00' * 27+\
                camera_password+\
                b'\x00'*127

    def createStartStreamRequest(self,code:bytes) -> bytes:
        return  b'\x2f\x01\x00\x00'+\
                code+\
                b'\x01'+\
                b'\x00'*223

    def createInitStreamRequest(self):
        camId = struct.pack('<I', int(self.cameraSettings.camId))
        isHD = b'\x01' if self.cameraSettings.camHD else b'\x00'
        return  b'\x2d\x01\x00\x00'+\
                camId+\
                b'\x00\x00\x00\x00\x14\x00'+\
                self.authTiket+\
                b'\x00\x00'+self.authCode+\
                b'\x00\x00\x01\x00\x00\x00'+\
                isHD+\
                b'\x00\x00\x00\x01\x01\x01'+\
                b'\x00'*223 

    async def connect(self, timeout = 5):
        self.streamReader, self.streamWriter = await asyncio.wait_for(
            asyncio.open_connection(
                self.cameraSettings.camIP,
                self.cameraSettings.camPort
            ),
            timeout=timeout
    )

    async def disconnect(self):
        if self.streamWriter is not None:
            self.streamWriter.close()
            await self.streamWriter.wait_closed()
            self.streamReader = None
            self.streamWriter = None

    async def authenticate(self):
        try:
            await self.connect()
            request = await self.createAuthenticationRequest()
            response   = await self.exchange(request,self.streamReader,self.streamWriter)
            self.authTiket,self.authCode = response[13:15],response[17:19]

            if b'\x00\x00' in (self.authTiket,self.authCode):
                raise Exception("Wrong username/password")

            self.streamKey = self.authTiket + self.STREAMKEYPART2

        finally:
            await self.disconnect()

    async def initConnection(self):
        await self.authenticate()
        await self.connect()
        request1  = self.createInitStreamRequest()
        response1 = await self.exchange(request1, self.streamReader, self.streamWriter)
        request2  = self.createStartStreamRequest(response1[4:])
        await self.send(request2,self.streamWriter)

    async def initStream(self):
        try:
            await self.initConnection()
            tasks =  [
                asyncio.create_task(self.writeRawToQueue()),
                asyncio.create_task(self.convertRawQueueToEncryptedFrames()),
                asyncio.create_task(self.convertEncryptedFramesToDecryptedFrames()),
                asyncio.create_task(self.startServers())
                ]
            await asyncio.gather(*tasks)
        except Exception as e:
            print(e)
        finally:
            await self.stopEvent.set()
            await self.disconnect()

class CamAgentXM(CamAgent):
 
    def __init__(self, cameraSettings:CameraSettings):

        super().__init__(firstKey = b'dashoiahfarqdasr',cameraSettings=cameraSettings)
        self.serviceReader = None
        self.serviceWriter = None
        self.rawQueue        = asyncio.Queue()
        self.packetsQueue   = asyncio.Queue()
        self.stopEvent       = asyncio.Event()
        
        self.lockMainAndKeepAlive = asyncio.Lock()
        self.sessionID = 0
        self.communicationKey = b''
        self.keepAliveInterval = 0 

    def encrypt(self, data:bytes, key:bytes, block_size:int = 16) -> str:
        padding_length = (block_size - len(data) % block_size)
        padded_data = data + b'\x00' * padding_length
        cipher = AES.new(key, AES.MODE_CBC, b'\x00'*16)
        return base64.b64encode(cipher.encrypt(padded_data))

    def decrypt(self, base64_str:str, key:bytes):
        decoded_data = base64.b64decode(base64_str)
        cipher = AES.new(key, AES.MODE_CBC, b'\x00'*16)
        return cipher.decrypt(decoded_data).rstrip(b'\x00')

    def getPasswordHash(self, password=""):
        chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        md5 = hashlib.md5(bytes(password, "utf-8")).digest()
        return "".join([chars[sum(x) % 62] for x in zip(md5[::2], md5[1::2])]).encode('ascii')

    def encryptRSA(self, hex_modulus:str, hex_exponent:str, data:bytes) -> bytes:
        modulus = int(hex_modulus, 16)
        exponent = int(hex_exponent, 16)
        public_key = RSA.construct((modulus, exponent))
        cipher_rsa = PKCS1_v1_5.new(public_key)
        return cipher_rsa.encrypt(data)

    async def keepAlive(self):
        
        while not self.stopEvent.is_set():
            try:        
                
                if self.keepAliveInterval == 0:
                    await asyncio.sleep(1)
                    continue
                
                await asyncio.sleep(self.keepAliveInterval-1)
                
                string_json_data = (json.dumps({"Name" : "KeepAlive" ,"SessionID" : "0x{:08X}".format(self.sessionID)})).encode('ascii')
                pkg_data = self.encrypt(string_json_data, self.communicationKey)
                pkt = (
                    struct.pack(
                        "BB2xIIHHI",255,0, self.sessionID, 0,0, 1006, len(pkg_data)+1
                    )
                    + pkg_data
                    + b'\x00'
                )
               
                async with self.lockMainAndKeepAlive:
                    response = await self.exchange(pkt, self.serviceReader, self.serviceWriter)
        
            except Exception as e:
                print(f"keep alive exception: {e}")

    async def convertBufferToFrames(self):
        try:
            while not self.stopEvent.is_set():

                header = bytes(await self.sharedBuffer1.readData(20))
                ( head, version, session, sequence_number, total, cur, msgid, len_data ) = struct.unpack("BB2xIIBBHI", header)
                
                pkg = bytes(await self.sharedBuffer1.readData(len_data))
                if len(pkg)<4:
                    continue
                
                packetType = struct.unpack(">I", pkg[:4])[0]
                pkg_data_len = 0

                # I-frame
                if packetType == 0x01FC:
                    (media, fps, w, h, dt, pkg_data_len,) = struct.unpack("BBBBII", pkg[4:16])
                    await self.framesQueque.put((pkg[16:16+pkg_data_len], True))

                # P-frame
                elif packetType == 0x01FD:
                    b_pkg_data_len = pkg[:4]
                    pkg_data_len = struct.unpack("<I", b_pkg_data_len)[0]
                    await self.framesQueque.put((pkg[4:4+pkg_data_len], True))
                
                # Audio     
                elif packetType == 0x01FA:
                    b_pkg_data_len = pkg[:4]
                    (media, smpl_rate, pkg_data_len,) = struct.unpack("BBH", b_pkg_data_len)  
                    await self.framesQueque.put((pkg[4:4+pkg_data_len], False))

        except Exception as e:
            print(f"Convert Buffer To Frames : {e}")
        finally:
            self.stopEvent.set()
        
    async def writeRawToQueue(self, sleepBetweenTries:int = 5):
        try:
            while not self.stopEvent.is_set() :
                try:
                    try:
                        data = await self.recv(self.streamReader)
                        await self.rawQueue.put(data)
                    except Exception as e:
                        print(f"Convert raw data to buffer error: {e}")
                        # remove stream data for the next connection
                        while not self.rawQueue.empty():
                            self.rawQueue.get_nowait()
                            self.rawQueue.task_done()
                        await self.disconnect()
                        await self.initConnection()

                except:
                    await asyncio.sleep(sleepBetweenTries)
                    pass
        finally:
            await self.disconnect()
            await self.stopEvent.set()


    async def convertRawQueueToPackets(self):

        packetData = bytearray()

        try:
            while not self.stopEvent.is_set():

                while len(packetData)<20:
                    packetData.extend(await self.rawQueue.get())
                    self.rawQueue.task_done()
                
                header = bytes(packetData[:20])
                (
                    head,
                    version,
                    session,
                    sequence_number,
                    total,
                    cur,
                    msgid,
                    packetDataSize,
                ) = struct.unpack("BB2xIIBBHI", header)
                
                # read packet payload                   
                while len(packetData)-20<packetDataSize:
                    packetData.extend(await self.rawQueue.get())
                    self.rawQueue.task_done()

                packetPayload = bytes(packetData[20:20+packetDataSize])
                del packetData[:20+packetDataSize] 
                await self.packetsQueue.put(packetPayload)

        except Exception as e:
            print(f"Convert Buffer To Packets : {e}")
        finally:
            self.stopEvent.set()
        
    async def convertPacketsToFrames(self):
        
        try:
            packetData = bytearray()
            while not self.stopEvent.is_set():
                
                while len(packetData)<4: 
                    packetData.extend(await self.packetsQueue.get())
                    self.packetsQueue.task_done()

                packetType = struct.unpack(">I", bytes(packetData[:4]))[0]
                del packetData[:4]
                pkg_data_len = 0
                # I-frame
                if packetType == 0x01FC:
                    (media, fps, w, h, dt, pkg_data_len,) = struct.unpack("BBBBII", bytes(packetData[:12]))
                    del packetData[:12]
                    while len(packetData)<pkg_data_len:
                        packetData.extend(await self.packetsQueue.get())
                        self.packetsQueue.task_done()
                    await self.framesQueque.put((bytes(packetData[:pkg_data_len]), True))
                    del packetData[:pkg_data_len]
                # P-frame
                elif packetType == 0x01FD:

                    while len(packetData)<4:
                        packetData.extend(await self.packetsQueue.get())
                        self.packetsQueue.task_done()
                    
                    pkg_data_len = struct.unpack("<I", packetData[:4])[0]
                    del packetData[:4]

                    while len(packetData)<pkg_data_len:
                        packetData.extend(await self.packetsQueue.get())
                        self.packetsQueue.task_done()
                        
                    await self.framesQueque.put((bytes(packetData[:pkg_data_len]), True))
                    del packetData[:pkg_data_len]
                # Audio     
                elif packetType == 0x01FA:

                    while len(packetData)<4:
                        packetData.extend(await self.packetsQueue.get())
                        self.packetsQueue.task_done()
                    
                    (media, smpl_rate, pkg_data_len,) = struct.unpack("BBH", packetData[:4])  
                    del packetData[:4]

                    while len(packetData)<pkg_data_len:
                        packetData.extend(await self.packetsQueue.get())
                        self.packetsQueue.task_done()
                    
                    await self.framesQueque.put((bytes(packetData[:pkg_data_len]), False))
                    del packetData[:pkg_data_len]
                
        except Exception as e:
            print(f"Convert Packets To Frames : {e}")
        finally:
            self.stopEvent.set()

    async def connect(self, timeout = 5):
        
        self.streamReader, self.streamWriter = await asyncio.wait_for(
            asyncio.open_connection(
                self.cameraSettings.camIP,
                self.cameraSettings.camPort
            ),
            timeout=timeout
        )
        
        self.serviceReader, self.serviceWriter = await asyncio.wait_for(
            asyncio.open_connection(
                self.cameraSettings.camIP,
                self.cameraSettings.camPort
            ),
            timeout=timeout
        )

    async def disconnect(self):
        if self.streamWriter is not None:
            self.streamWriter.close()
            await self.streamWriter.wait_closed()
            self.streamReader = None
            self.streamWriter = None

        if self.serviceWriter is not None:
            self.serviceWriter.close()
            await self.serviceWriter.wait_closed()
            self.serviceReader = None
            self.serviceWriter = None

    async def authenticate(self):
        try:
            
            await self.connect()

            pkg_data = json.dumps({ "Name" : "OPMonitor", "OPMonitor" : { "Action" : "Claim", "Parameter" : { "Channel" : 0, "CombinMode" : "CONNECT_ALL", "StreamType" : "Main" if self.cameraSettings.camHD else "Extra" , "TransMode" : "TCP" } }, "SessionID" : "0x1" }).encode('ascii')
            pkt = (
            struct.pack(
                "BB2xIIHHI",255,0, 99999, 0, 99, 1413, len(pkg_data)+1
            )
            + pkg_data
            + b'\x0a'
            )
            
            async with self.lockMainAndKeepAlive:   
                response = await self.exchange(pkt,self.serviceReader,self.serviceWriter)
            
            if response is None or len(response) < 20:
                return None
            
            (
                head,
                version,
                self.session,
                self.sequence_number,
                msgid,
                qcode,
                len_data,
            ) = struct.unpack("BB2xIIHHI", response[:20])
            
            response_data = response[20:]
            
            decrypted_response_data = self.decrypt(response_data,self.firstKey)    
            
            json_response = json.loads(decrypted_response_data)            
            (self.PublicKey_modulus,self.PublicKey_exponent) = json_response["PublicKey"].split(',', 1)
            
            ret_code= int(json_response["Ret"])
            
            self.communicationKey = os.urandom(16) #random AES128 key
            encrypted_communicate_key = self.encryptRSA(self.PublicKey_modulus,self.PublicKey_exponent,self.communicationKey)
            encrypted_password_hash = self.encryptRSA(self.PublicKey_modulus,self.PublicKey_exponent,self.getPasswordHash(self.cameraSettings.camUserPassword))
            encrypted_username = self.encryptRSA(self.PublicKey_modulus,self.PublicKey_exponent,self.cameraSettings.camUserName.encode('ascii'))
            
            pkg_data = b''
            json_data = {
                "CommunicateKey" : encrypted_communicate_key.hex().upper(), 
                "EncryptType" : "MD5", 
                "LoginType" : "DVRIP-Web", 
                "PassWord" : encrypted_password_hash.hex().upper(), 
                "UserName" : encrypted_username.hex().upper() 
            }
            string_json_data = (json.dumps(json_data)+"\n").encode('ascii')
            encrypted_string_json_data = self.encrypt(string_json_data,self.firstKey)
            
            pkt = (
            struct.pack(
                "BB2xIIHHI",255,0, 0, 0, 99, 1000, len(encrypted_string_json_data)+1
            )
            + encrypted_string_json_data
            + b'\x00'
            )
            
            async with self.lockMainAndKeepAlive:   
                second_response = await self.exchange(pkt,self.serviceReader,self.serviceWriter)
            
            if second_response is None or len(second_response) < 20:
                return None
            (
                head,
                version,
                self.session,
                self.sequence_number,
                msgid,
                qcode,
                len_data,
            ) = struct.unpack("BB2xIIHHI", second_response[:20])
            
            json_response = json.loads(second_response[20:].decode('ascii').rstrip('\n\r\x00'))
            
            ret = int(json_response["Ret"])
            
            if ret not in [100,515]:
                raise "Wrong username/password"
             
            self.keepAliveInterval = int(json_response["AliveInterval"])
            self.sessionID = int(json_response["SessionID"],16)
            
        except Exception as e:
            print(e)

    async def initConnection(self):

        await self.authenticate()
        
        pkg_data = json.dumps({ "Name" : "OPMonitor", "OPMonitor" : { "Action" : "Claim", "Parameter" : { "Channel" : 0, "CombinMode" : "NONE", "StreamType" : "Main" if self.cameraSettings.camHD else "Extra", "TransMode" : "TCP" } }, "SessionID" : "0x{:08X}".format(self.sessionID) }).encode('ascii')
        pkt = (
            struct.pack(
                "BB2xIIHHI",255,0, self.sessionID, 0,0, 1413, len(pkg_data)+1
            )
            + pkg_data
            + b'\x0a'
        )
     
        response = await self.exchange(pkt, self.streamReader, self.streamWriter)
        json_get_video_response = json.loads(response[20:].decode('ascii').rstrip('\n\r\x00'))

        string_json_data = json.dumps({ "Name" : "OPMonitor" , "OPMonitor" : { "Action" : "Start" , "Parameter" : { "Channel":0, "CombinMode": "NONE", "StreamType": "Main" if self.cameraSettings.camHD else "Extra", "TransMode":"TCP"}},"SessionID":"0x{:08X}".format(self.sessionID)}).encode('ascii')
        pkg_data = self.encrypt(string_json_data, self.communicationKey)
    
        pkt = (
        
            struct.pack(
                "BB2xIIHHI",255,0, self.sessionID, 0,0, 1410, len(pkg_data)+1
            )
            + pkg_data
            + b'\x00'
        )
        
        async with self.lockMainAndKeepAlive:  
            response = await self.exchange(pkt, self.serviceReader, self.serviceWriter)
            
    async def initStream(self):
        try:
            await self.initConnection()
            tasks =  [
                asyncio.create_task(self.keepAlive()),
                asyncio.create_task(self.writeRawToQueue()),
                asyncio.create_task(self.convertRawQueueToPackets()),
                asyncio.create_task(self.convertPacketsToFrames()),
                asyncio.create_task(self.startServers()),
                ]
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"Init Stream Error: {e}")
        finally:
            await self.stopEvent.set()
            await self.disconnect()

    
class Streamer:

    def __init__(self):

        self.CLASSMAP = {
            "xm"  : CamAgentXM ,
            "v380": CamAgentV380,
        }

        self.settings = AppSettings()
        settings_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.json")

        try:
            self.settings = AppSettings.loadSettings(settings_file)
        except Exception as e:
            print(e)
            self.settings.saveSettings(settings_file)

    async def run(self):

        self.camAgents =    [self.CLASSMAP[c.camProtocol.lower()](c) for c in self.settings.cameraSettings.values() if isinstance(c, CameraSettings)]
        self.fFMpegAgents  = [FFMpegAgent(f) for f in self.settings.ffmpegSettings.values() if isinstance(f, FFMpegSettings)]

        tasks =   [ asyncio.create_task(ca.initStream()) for ca in self.camAgents if ca.cameraSettings.camEnabled] +\
                  [ asyncio.create_task(fa.run()) for fa in self.fFMpegAgents if fa.ffmpeg_settings.ffmpegEnabled]

        await asyncio.gather(*tasks)


class FFMpegAgent:

    def __init__(self, ffmpeg_settings:FFMpegSettings):
        self.ffmpeg_settings  = ffmpeg_settings

    async def run(self, wait_time: int = 7):
        cmd = await self.ffmpeg_settings.get_real_command()

        await asyncio.sleep(wait_time) # sleep is required to wait for ipc-app interconnection

        process = await asyncio.create_subprocess_shell(
            cmd,
            stdin=asyncio.subprocess.PIPE
        )

        await process.wait()


if __name__ == '__main__':
    print(__app_header__)
    try:
        streamer = Streamer()
        asyncio.run(streamer.run())
    except Exception as e:
        print(f"Main error: {e}")
