"""
Implement the TODO sections below using Redis commands.
Each endpoint corresponds to one graded task.

Run locally:
    uvicorn app:app --reload

Run tests:
    pytest tests/ -v
"""

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import redis
import uuid

app = FastAPI(title="Redis Assignments")

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

class LoginRequest(BaseModel):
    user_id: str

class TaskRequest(BaseModel):
    task: str


# ============================================================
# Task 1: Session Storage
# ============================================================
#
# POST /login
#   - Accept JSON body: {"user_id": "alice"}
#   - Generate a unique session_id (use uuid.uuid4())
#   - Store in Redis:  SET session:<session_id> <user_id> EX 3600
#   - Return JSON:     {"session_id": "<session_id>"}
#
# GET /me
#   - Read the X-Session-Id header
#   - Look up in Redis: GET session:<session_id>
#   - If found  → return {"user_id": "<user_id>"}
#   - If missing → return 401 Unauthorized
#
# ============================================================

@app.post("/login")
def login(body: LoginRequest):
    session_id = str(uuid.uuid4())
    r.set(f"session:{session_id}", body.user_id, ex=3600)
    return {"session_id": session_id}


@app.get("/me")
def me(x_session_id: str = Header()):
    user_id = r.get(f"session:{x_session_id}")
    if user_id is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return {"user_id": user_id}


# ============================================================
# Task 2: Rate Limiter (Fixed Window)
# ============================================================
#
# GET /request?user_id=<id>
#   - Key pattern: requests:user:<user_id>
#   - Use counter variable which expires in 60 seconds 
#   - If count > 5  → return 429 with {"error": "rate limit exceeded"}
#   - Otherwise     → return 200 with {"status": "ok", "remaining": 5 - count}
# ============================================================

@app.get("/request")
def rate_limited_request(user_id: str):
    key = f"requests:user:{user_id}"
    count = r.incr(key)
    if count == 1:
        r.expire(key, 60)
    if count > 5:
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    remaining = 5 - count
    return {"status": "ok", "remaining": remaining}


# ============================================================
# Task 3: Task Queue (FIFO)
# ============================================================
#
# POST /task
#   - Accept JSON body: {"task": "send_email"}
#   - Push to the LEFT of a Redis list called "task_queue":
#         LPUSH task_queue <task>
#   - Return: {"status": "queued", "queue_length": <length>}
#
# GET /task
#   - Pop from the RIGHT of the list 
#   - If a task was returned → {"task": "<task>"}
#   - If the queue is empty  → 404 with {"error": "queue is empty"}
# ============================================================

@app.post("/task")
def add_task(body: TaskRequest):
    r.lpush("task_queue", body.task)
    queue_length = r.llen("task_queue")
    return {"status": "queued", "queue_length": queue_length}


@app.get("/task")
def get_task():
    task = r.rpop("task_queue")
    if task is None:
        raise HTTPException(status_code=404, detail="queue is empty")
    return {"task": task}


# ============================================================
# Bonus Challenge: Sliding Window Rate Limiter
# ============================================================
#
# GET /sliding_request?user_id=<id>
#   - Use Redis Sorted Set: requests:user:<user_id>
#   - Add current timestamp as score, unique ID as member
#   - Remove entries older than 60 seconds
#   - Count remaining entries
#   - If count > 5 → return 429
#   - Otherwise   → return 200 with remaining
# ============================================================

@app.get("/sliding_request")
def sliding_rate_limited_request(user_id: str):
    import time
    key = f"requests:user:{user_id}"
    now = time.time()
    request_id = str(uuid.uuid4())
    r.zadd(key, {request_id: now})
    r.zremrangebyscore(key, 0, now - 60)
    count = r.zcard(key)
    if count > 5:
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    remaining = 5 - count
    return {"status": "ok", "remaining": remaining}


# ============================================================
# BONUS: Sliding Window Rate Limiter 
# ============================================================

@app.get("/request_sliding")
def rate_limited_request_sliding(user_id: str):
    pass