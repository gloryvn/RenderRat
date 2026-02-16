from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict

app = FastAPI()

clients: Dict[str, WebSocket] = {}
latest_images: Dict[str, str] = {}   # Lưu ảnh base64

@app.get("/")
async def root():
    return {"status": "Server is running"}

# =========================
# WebSocket Client
# =========================
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket
    print(f"{client_id} connected")

    try:
        while True:
            data = await websocket.receive_text()

            # Heartbeat
            if data == "ping":
                await websocket.send_text("pong")

            # Nếu không phải ping -> coi như ảnh base64
            else:
                print(f"Received image from {client_id}")
                latest_images[client_id] = data

    except WebSocketDisconnect:
        print(f"{client_id} disconnected")
        clients.pop(client_id, None)

# =========================
# Admin gửi lệnh chụp ảnh
# =========================
@app.post("/admin/request-photo/{client_id}")
async def request_photo(client_id: str):
    if client_id in clients:
        await clients[client_id].send_text("take_photo")
        return {"status": "sent"}
    return {"status": "client offline"}

# =========================
# Admin lấy ảnh
# =========================
@app.get("/admin/get-photo/{client_id}")
async def get_photo(client_id: str):
    if client_id in latest_images:
        return {"image": latest_images[client_id]}
    return {"image": None}
