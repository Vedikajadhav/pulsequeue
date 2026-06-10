from fastapi import FastAPI

app = FastAPI(
    title="PulseQueue",
    description="Distributed Message Queue & Analytics Platform",
    version="1.0.0"
)

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "PulseQueue"}
