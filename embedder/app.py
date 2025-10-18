# embedder/app.py
from fastapi import FastAPI, Request, HTTPException
from sentence_transformers import SentenceTransformer
import uvicorn
from typing import List, Union

app = FastAPI(title="Local Embedding Server")
model = SentenceTransformer("intfloat/multilingual-e5-small")

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

def _normalize_input(payload) -> List[str]:
    if isinstance(payload, str):
        return [payload]
    if isinstance(payload, dict):
        if "text" in payload and isinstance(payload["text"], str):
            return [payload["text"]]
        if "texts" in payload and isinstance(payload["texts"], list):
            texts = [t for t in payload["texts"] if isinstance(t, str)]
            if texts:
                return texts

        return [str(payload)]
    if isinstance(payload, list):
        texts = [t for t in payload if isinstance(t, str)]
        if texts:
            return texts
        raise HTTPException(status_code=400, detail="Provide 'text' or 'texts' (or raw string/list[str])")

@app.post("/embed")
async def embed(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = await request.body()
        payload = payload.decode("utf-8", errors="ignore")

    texts = _normalize_input(payload)
    if not texts:
        raise HTTPException(status_code=400, detail="Empty input")
    v = model.encode(texts[0], normalize_embeddings=True)
    return {"embedding": v.tolist()}

@app.post("/embed-batch")
async def embed_batch(request: Request):
    try:
        payload = await request.json()
        print(f"[DEBUG] Received payload type: {type(payload)}")
        print(f"[DEBUG] Payload keys: {payload.keys() if isinstance(payload, dict) else 'Not a dict'}")
        if isinstance(payload, dict) and "texts" in payload:
            print(f"[DEBUG] texts length: {len(payload.get('texts', []))}")
            print(f"[DEBUG] First 3 texts: {payload['texts'][:3] if payload.get('texts') else 'None'}")
    except Exception as e:
        print(f"[DEBUG] JSON parse error: {e}")
        payload = await request.body()
        payload = payload.decode("utf-8", errors="ignore")

    texts = _normalize_input(payload)
    print(f"[DEBUG] After normalize: {len(texts)} texts")

    if not texts:
        raise HTTPException(status_code=400, detail="Empty input")

    vs = model.encode(texts, normalize_embeddings=True)
    print(f"[DEBUG] Encoded {len(texts)} texts -> {len(vs)} embeddings")

    return {"embeddings": [v.tolist() for v in vs]}

if __name__ == "__main__": uvicorn.run(app, host="0.0.0.0", port=8081)