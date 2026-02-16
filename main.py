from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict

app = FastAPI()

clients: Dict[str, WebSocket] = {}

@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket

    try:
        while True:
            data = await websocket.receive_text()

            # Heartbeat
            if data == "ping":
                await websocket.send_text("pong")

    except WebSocketDisconnect:
        clients.pop(client_id, None)

# Admin yêu cầu chụp ảnh
@app.post("/admin/request-photo/{client_id}")
async def request_photo(client_id: str):
    if client_id in clients:
        await clients[client_id].send_text("take_photo")
        return {"status": "sent"}
    return {"status": "client offline"}
