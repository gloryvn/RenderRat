from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict

app = FastAPI()

clients: Dict[str, WebSocket] = {}
admins: list[WebSocket] = []

@app.get("/")
async def root():
    return {"status": "Server is running"}

# =========================
# CLIENT CONNECT
# =========================
@app.websocket("/ws/client/{client_id}")
async def client_ws(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket
    print(f"Client {client_id} connected")

    try:
        while True:
            data = await websocket.receive_text()

            # Nếu client gửi ảnh → forward cho admin
            for admin in admins:
                await admin.send_json({
                    "client_id": client_id,
                    "image": data
                })

    except WebSocketDisconnect:
        clients.pop(client_id, None)
        print(f"Client {client_id} disconnected")


# =========================
# ADMIN CONNECT
# =========================
@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    await websocket.accept()
    admins.append(websocket)
    print("Admin connected")

    try:
        while True:
            data = await websocket.receive_json()

            # data = {"action": "take_photo", "client_id": "pc01"}

            if data["action"] == "take_photo":
                client_id = data["client_id"]
                if client_id in clients:
                    await clients[client_id].send_text("take_photo")

    except WebSocketDisconnect:
        admins.remove(websocket)
        print("Admin disconnected")
