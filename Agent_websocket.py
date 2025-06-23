import argparse
import asyncio
from concurrent import futures
from dataclasses import dataclass, field
import json
import logging
import os
import queue
import struct
import sys
import threading
import time
from typing import Annotated, Dict, Optional

from fastapi import FastAPI, Form, Request
import grpc
import requests
import uvicorn
import websockets
import gesture_pb2
import gesture_pb2_grpc
import pose_pb2
import pose_pb2_grpc
import base64


@dataclass
class Service:
    name: str
    stub_class: type
    ip: str = ""
    port: int = 0
    frequency: float = 0.0
    channel: Optional[grpc.Channel] = None
    stub: Optional[object] = None
    executor: futures.ThreadPoolExecutor = field(default_factory=lambda: futures.ThreadPoolExecutor(max_workers=30))


@dataclass
class FPSCounter:
    recv: int = 0
    pose_send: int = 0
    pose_result: int = 0
    gesture_send: int = 0
    gesture_result: int = 0
    ret: int = 0

    def reset(self) -> None:
        self.recv = self.pose_send = self.pose_result = 0
        self.gesture_send = self.gesture_result = self.ret = 0

    def log(self) -> None:
        if any(
            [
                self.recv,
                self.pose_send,
                self.pose_result,
                self.gesture_send,
                self.gesture_result,
                self.ret,
            ]
        ):
            logging.info(
                "FPS: [receive from AR: %d, send to pose service: %d, get pose result: %d, "
                "send to gesture service: %d, get gesture result: %d, return to AR: %d]",
                self.recv,
                self.pose_send,
                self.pose_result,
                self.gesture_send,
                self.gesture_result,
                self.ret,
            )
        self.reset()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("agent_ip_outside", nargs="?", default="10.52.52.50")
    parser.add_argument("agent_port", nargs="?", type=int, default=8888)
    parser.add_argument("ws_port", nargs="?", type=int, default=8889)
    parser.add_argument("pose_ip", nargs="?", default="")
    parser.add_argument("pose_port", nargs="?", type=int, default=0)
    parser.add_argument("pose_freq", nargs="?", type=float, default=0)
    parser.add_argument("gesture_ip", nargs="?", default="")
    parser.add_argument("gesture_port", nargs="?", type=int, default=0)
    parser.add_argument("gesture_freq", nargs="?", type=float, default=0)
    return parser.parse_args()


args = parse_args()
AgentIP_outside = args.agent_ip_outside
AgentIP = "0.0.0.0"
AgentPort = args.agent_port
AgentWebsocketPort = args.ws_port

log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(filename=os.path.join(log_dir, f"Agent_{AgentIP_outside}_{AgentPort}.log"),
                    format='%(asctime)s %(levelname)s: %(message)s',
                    level=logging.INFO)

app = FastAPI()

@app.middleware("http")
async def log_requests(request: Request, call_next):
    log_data = {
        "client_host": request.client.host,
        "client_port": request.client.port,
        "method": request.method,
        "url": str(request.url),
    }

    logging.info(f"HTTP Request: {log_data}")

    response = await call_next(request)
    return response

@app.post("/subscribe")
async def subscribe(servicename: Annotated[str, Form()]):
    response = requests.post(f'http://{ControllerIP}:{ControllerPort}/subscribe', {"ip": AgentIP_outside, "port": AgentPort, "serviceType": servicename})
    response = response.json()

    service = services[servicename]
    service.ip = response.get("IP", "")
    service.port = int(response.get("Port", 0))
    service.frequency = float(response.get("Frequency", 0))

    connect_to_service(servicename)

    hasGotService[servicename] = True

    #start_adjust_freq(svcidx)

    print({name: svc.ip for name, svc in services.items()})
    print({name: svc.port for name, svc in services.items()})
    print({name: svc.frequency for name, svc in services.items()})
    print(hasGotService)

    return response

@app.post("/servicechange")
async def service(request: Request):
    data = await request.json()
    servicename = data["servicename"]
    ip = data["ip"]
    port = data["port"]
    frequency = data["frequency"]

    logging.info(f"change {servicename} service to IP: {ip}, Port: {port}, Frequency: {frequency}")

    service = services[servicename]
    if ip != "null":
        service.ip = ip
        service.port = int(port)
        connect_to_service(servicename)
    service.frequency = float(frequency)

    print({name: svc.ip for name, svc in services.items()})
    print({name: svc.port for name, svc in services.items()})
    print({name: svc.frequency for name, svc in services.items()})
    print(hasGotService)
    return {"status": "200", "message": "OK"}

@app.delete("/subscribe")
async def unsubscribe():
    for name, svc in services.items():
        svc.ip = ""
        svc.port = 0
        svc.frequency = 0
        hasGotService[name] = False
    print({name: svc.ip for name, svc in services.items()})
    print({name: svc.port for name, svc in services.items()})
    print({name: svc.frequency for name, svc in services.items()})
    print(hasGotService)

    body = {'port': AgentPort}

    response = requests.post(f'http://{ControllerIP}:{ControllerPort}/unsubscribe', json.dumps(body))
    return {"status": "200", "message": "OK"}

def run_http_server():
    logging.info(f"Http server started on {AgentIP_outside}:{AgentPort}")
    uvicorn.run(app, host = AgentIP, port = AgentPort)


def connect_to_service(servicename: str) -> None:
    service = services[servicename]
    service.executor.shutdown(wait=False, cancel_futures=True)

    if service.channel:
        service.channel.close()

    service.channel = grpc.insecure_channel(f"{service.ip}:{service.port}")
    service.stub = service.stub_class(service.channel)
    logging.info(
        f"connected to {servicename} service, {service.ip}:{service.port}"
    )

    service.executor = futures.ThreadPoolExecutor(max_workers=30)

def start_adjust_freq(servicename: str) -> None:
    if servicename == "gesture":
        threading.Thread(target=gesture_det_freq, daemon=True).start()
    elif servicename == "pose":
        threading.Thread(target=pose_det_freq, daemon=True).start()

def pose_det_callback(future):
    try:
        result = future.result()
        print(result)
    except futures.CancelledError:
        pass
    except grpc.RpcError as e:
        logging.error(e)
        # 如果需要，可以在這裡重新連線或做其他錯誤處理
        #time.sleep(1)
        #connect_to_service("pose")
        #logging.info("Try to connect to Pose service again")

def pose_det_freq() -> None:
    service = services["pose"]
    while True:
        try:
            request = input_request_pose_queue.get(timeout=1 / service.frequency)
            while not input_request_pose_queue.empty():
                request = input_request_pose_queue.get_nowait()
        except queue.Empty:
            continue

        t = time.time()
        try:
            future = service.executor.submit(forward_to_pose_detection, request)
            future.add_done_callback(pose_det_callback)
        except Exception:
            logging.error("threadpool shutting down")
        sleeptime = 1 / service.frequency - (time.time() - t)
        if sleeptime > 0:
            time.sleep(sleeptime)
        else:
            logging.info("pose sleep time = %s", sleeptime)

            
def forward_to_pose_detection(request: bytes) -> None:
    service = services["pose"]

    req = pose_pb2.FrameRequest(image_data=request[:-4])
    fps_counter.pose_send += 1
    try:
        t = time.time()
        response = service.stub.SkeletonFrame(req, timeout=pose_timeout)
        logging.info("pose detection inference time = %s", time.time() - t)
    except Exception as e:
        logging.error(e)
        raise grpc.RpcError("gRPC transmission failed")
    fps_counter.pose_result += 1

    retstr = response.skeletons
    retstr += f"{struct.unpack('i', request[-4:])[0]:04}" + " "
    responses.append(retstr[:-1])

def gesture_det_callback(future):
    try:
        result = future.result()
        print(result)
    except futures.CancelledError:
        pass
    except grpc.RpcError as e:
        logging.error(e)
        # 若需要，可在此處理重新連線或其他錯誤處理
        #time.sleep(1)
        #connect_to_service("gesture")
        #logging.info("Try to connect to Gesture service again")

def gesture_det_freq() -> None:
    service = services["gesture"]
    while True:
        try:
            request = input_request_gesture_queue.get(timeout=1 / service.frequency)
            while not input_request_gesture_queue.empty():
                request = input_request_gesture_queue.get_nowait()
        except queue.Empty:
            continue

        t = time.time()
        try:
            future = service.executor.submit(forward_to_gesture_detection, request)
            future.add_done_callback(gesture_det_callback)
        except Exception:
            logging.error("threadpool shutting down")
        sleeptime = 1 / service.frequency - (time.time() - t)
        if sleeptime > 0:
            time.sleep(sleeptime)
        else:
            logging.info("gesture sleep time = %s", sleeptime)

def forward_to_gesture_detection(request: bytes) -> None:
    service = services["gesture"]
    req = gesture_pb2.RecognitionRequest(image=base64.b64encode(request[:-4]))
    fps_counter.gesture_send += 1
    try:
        t = time.time()
        response = service.stub.Recognition(req, timeout=gesture_timeout)
        logging.info("gesture detection inference time = %s", time.time() - t)
    except Exception as e:
        logging.error(e)
        raise grpc.RpcError("gRPC transmission failed")
    fps_counter.gesture_result += 1
    result = json.loads(response.action)
    retstr = result["Left"] + " " + result["Right"] + " "
    retstr += f"{struct.unpack('i', request[-4:])[0]:04}" + " "
    responses.append(retstr[:-1])

def counting_FPS() -> None:
    while True:
        time.sleep(1)
        fps_counter.log()

async def handle_connection(websocket, path):
    print("Client connected")
    client_ip, client_port = websocket.remote_address
    logging.info(f"WebSocket Client connected from {client_ip}:{client_port}")

    threading.Thread(target=counting_FPS, daemon=True).start()
    #start_adjust_freq(1)
    
    receive_task = asyncio.create_task(receive_messages(websocket))
    send_task = asyncio.create_task(send_messages(websocket))

    await asyncio.gather(receive_task, send_task)

    print("Client disconnected")

async def receive_messages(websocket):
    try:
        async for message in websocket:
            fps_counter.recv += 1
            #print(f"Received message: {message}")
            # 可以在這裡處理接收到的消息，例如存儲或進行某些操作
            #print(struct.unpack('i', message[-4:])[0])
            #recv[struct.unpack('i', message[-4:])[0]] = datetime.datetime.now().strftime("%H_%M_%S_%f")[:-3]

            input_request_pose_queue.put(message)
            input_request_gesture_queue.put(message)

    except Exception as e:
        print("Exception while receiving")
        print(e)
        logging.error(e)

async def send_messages(websocket):
    try:
        while True:
            if responses:
                idx = int(responses[0][-4:])
                fps_counter.ret += 1
                await websocket.send(responses[0].encode("utf-8"))
                responses.pop(0)
            await asyncio.sleep(0.001)
    except Exception as e:
        print("Exception while sending")
        print(e)
        logging.error(e)
        

services: Dict[str, Service] = {
    "pose": Service("pose", pose_pb2_grpc.MirrorStub),
    "gesture": Service("gesture", gesture_pb2_grpc.GestureRecognitionStub),
}
hasGotService = {name: False for name in services}

input_request_pose_queue = queue.Queue()
input_request_gesture_queue = queue.Queue()
responses: list[str] = []

fps_counter = FPSCounter()

pose_lowest_FPS = 10
gesture_lowest_FPS = 15

pose_timeout = 1 / pose_lowest_FPS + 0.008           # 0.008 is network average transfer latency plus std_dev
gesture_timeout = 1 / gesture_lowest_FPS + 0.008     # 0.008 is network average transfer latency plus std_dev

try:
    if args.pose_ip and args.pose_port:
        svc = services["pose"]
        svc.ip = args.pose_ip
        svc.port = args.pose_port
        svc.frequency = round(args.pose_freq, 5)
        logging.info(
            "Try to connect to Pose service, IP = %s, Port = %s, Freq = %s",
            svc.ip,
            svc.port,
            svc.frequency,
        )
        hasGotService["pose"] = True
        connect_to_service("pose")
        start_adjust_freq("pose")
    else:
        logging.info("no Pose service subscribed")

    if args.gesture_ip and args.gesture_port:
        svc = services["gesture"]
        svc.ip = args.gesture_ip
        svc.port = args.gesture_port
        svc.frequency = round(args.gesture_freq, 5)
        logging.info(
            "Try to connect to Gesture service, IP = %s, Port = %s, Freq = %s",
            svc.ip,
            svc.port,
            svc.frequency,
        )
        hasGotService["gesture"] = True
        connect_to_service("gesture")
        start_adjust_freq("gesture")
    else:
        logging.info("no Gesture service subscribed")
except Exception as e:
    logging.error(e)

print(AgentIP)
print(AgentPort)
print(AgentWebsocketPort)
ControllerIP = '10.52.52.126'
ControllerPort = 30004

# 啟動 WebSocket 伺服器
async def start_server():
    try:
        websocket_server = await websockets.serve(handle_connection, AgentIP, AgentWebsocketPort)

        print(f"WebSocket server started on ws://{AgentIP}:{AgentWebsocketPort}")
        logging.info(f"WebSocket server started on ws://{AgentIP}:{AgentWebsocketPort}")
        
        await websocket_server.wait_closed()
        
    except Exception as e:
        logging.error(f"Failed to start WebSocket server: {e}")

if __name__ == '__main__':
    print("start")

    app.debug = False
    threading.Thread(target = run_http_server).start()

    #recv = ['0'] * 10000
    #send = ['0'] * 10000
    #get = ['0'] * 10000
    #ret = ['0'] * 10000

    

    # 啟動WebSocket伺服器
    try:
        asyncio.run(start_server())
    except Exception as e:
        logging.error(f"Failed to start WebSocket server: {e}")
