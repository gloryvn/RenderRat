import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict

app = FastAPI()

# Lưu client đang online
clients: Dict[str, WebSocket] = {}

# Healthcheck cho Railway
@app.get("/")
async def root():
    return {"status": "server running"}

# Client kết nối
@app.websocket("/ws/client/{client_id}")
async def client_ws(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket
    print(f"Client {client_id} connected")

    try:
        while True:
            data = await websocket.receive_text()
            print(f"From {client_id}: {data}")

    except WebSocketDisconnect:
        print(f"Client {client_id} disconnected")
        clients.pop(client_id, None)

# Admin gửi lệnh tới client
@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    await websocket.accept()
    print("Admin connected")

    try:
        while True:
            data = await websocket.receive_json()
            client_id = data.get("client_id")
            message = data.get("message")

            if client_id in clients:
                await clients[client_id].send_text(message)
                await websocket.send_text("Sent")
            else:
                await websocket.send_text("Client not found")

    except WebSocketDisconnect:
        print("Admin disconnected")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
