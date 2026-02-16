from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict
import base64

app = FastAPI()

# Lưu yêu cầu chụp ảnh
photo_requests: Dict[str, bool] = {}

# Lưu ảnh mới nhất
latest_images: Dict[str, str] = {}

class ImageData(BaseModel):
    client_id: str
    image: str

# 🔵 Admin yêu cầu client chụp ảnh
@app.post("/admin/request-photo/{client_id}")
def request_photo(client_id: str):
    photo_requests[client_id] = True
    return {"status": "photo requested"}

# 🔵 Client hỏi xem có cần chụp ảnh không
@app.get("/client/check/{client_id}")
def check_request(client_id: str):
    if photo_requests.pop(client_id, False):
        return {"take_photo": True}
    return {"take_photo": False}

# 🔵 Client gửi ảnh lên
@app.post("/client/upload")
def upload_photo(data: ImageData):
    latest_images[data.client_id] = data.image
    return {"status": "image received"}

# 🔵 Admin lấy ảnh
@app.get("/admin/get-photo/{client_id}")
def get_photo(client_id: str):
    img = latest_images.get(client_id)
    return {"image": img}
