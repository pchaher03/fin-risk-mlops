from fastapi import FastAPI

app = FastAPI(title="fin-risk-mlops API Gateway")

@app.get("/health")
def health_check():
    return {"status": "ok"}