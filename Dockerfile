FROM python:3.12.1-slim
WORKDIR /app
RUN mkdir -p /app/logs
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
COPY Agent_websocket.py inference_pb2.py inference_pb2_grpc.py status_pb2.py status_pb2_grpc.py gesture_pb2.py gesture_pb2_grpc.py ./ 
ENTRYPOINT ["python3", "Agent_websocket.py"]
