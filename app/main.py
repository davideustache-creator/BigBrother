# app/main.py
from fastapi import FastAPI

app = FastAPI(title="BigBrother API")

@app.get("/")
def read_root():
    return {"Project": "BigBrother-RediSearch"}