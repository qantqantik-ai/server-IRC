import asyncio
import json
import os
import logging
import websockets
from websockets.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ── XOR cipher (same key as Java client: 0x15) ──────────────────────────────
def cypher(data: str) -> str:
    encoded = data.encode('utf-8')
    xored = bytes(b ^ 0x15 for b in encoded)
    return xored.decode('utf-8')


# ── State ────────────────────────────────────────────────────────────────────
clients: dict = {}   # websocket → {"clientId", "username", "prefix"}
prefixes: dict = {}  # clientId  → prefix string
muted: set = set()   # clientId of muted users


# ── Helpers ──────────────────────────────────────────────────────────────────
async def send_to(ws, obj: dict):
    try:
        await ws.send(cypher(json.dumps(obj, ensure_ascii=False)))
    except Exception as e:
        logger.warning(f"Send error: {e}")


async def broadcast(obj: dict, exclude=None):
    msg = cypher(json.dumps(obj, ensure_ascii=False))
    for ws in list(clients.keys()):
        if ws is not exclude:
            try:
                await ws.send(msg)
            except Exception:
                pass

def find_ws_by_username(username: str):
    for ws, info in clients.items():
        if info.get("username", "").lower() == username.lower():
            return ws
    return None


# ── Connection handler ───────────────────────────────────────────────────────
async def handle(ws: WebSocketServerProtocol):
    client_id = ws.request_headers.get("Sec-WebSocket-Key", str(id(ws)))
    clients[ws] = {"clientId": client_id, "username": "Unknown", "prefix": ""}
    logger.info(f"+ Connected  | id={client_id[:8]}...")

    try:
        async for raw in ws:
            try:
                data = json.loads(cypher(raw))
            except Exception:
                logger.warning("Received non-JSON or bad cipher, skipping")
                continue

            msg_type = data.get("type", "")
            cid = data.get("clientId", client_id)
            
            if "author" in data and clients[ws]["username"] == "Unknown":
                clients[ws]["username"] = data.get("author")
                logger.info(f"[HWID-CHECK] {data.get('author')} connected with HWID: {cid}")

            # ── get_prefix ───────────────────────────────────────────────────
            if msg_type == "get_prefix":
                prefix = prefixes.get(cid, "")
                await send_to(ws, {"type": "prefix_info", "prefix": prefix})

            # ── set_prefix ───────────────────────────────────────────────────
            elif msg_type == "set_prefix":
                new_prefix = data.get("new_prefix", "")
                prefixes[cid] = new_prefix
                clients[ws]["prefix"] = new_prefix
                await send_to(ws, {"type": "prefix_updated", "prefix": new_prefix})
                logger.info(f"  Prefix set | {data.get('author', '?')} → '{new_prefix}'")

            # ── text message ─────────────────────────────────────────────────
            elif msg_type == "text":
                if cid in muted:
                    # Tell the sender they are muted
                    await send_to(ws, {
                        "type": "mute_attempt",
                        "reason": "Спам",
                        "duration_minutes": 0
                    })
                    continue

                author  = data.get("author", "Unknown")
                message = data.get("message", "")
                prefix  = prefixes.get(cid, "")
                clients[ws]["username"] = author

                logger.info(f"  Message    | [{prefix}] {author}: {message}")
                await broadcast({
                    "type":    "text",
                    "message": message,
                    "author":  author,
                    "prefix":  prefix
                })

            elif msg_type == "dm":
                target = data.get("target", "")
                message = data.get("message", "")
                author = data.get("author", "Unknown")
                prefix = prefixes.get(cid, "")
                
                target_ws = find_ws_by_username(target)
                if target_ws:
                    await send_to(target_ws, {
                        "type": "dm",
                        "message": message,
                        "author": author,
                        "target": target,
                        "prefix": prefix
                    })

            elif msg_type == "friend_request":
                target = data.get("target", "")
                author = data.get("author", "Unknown")
                target_ws = find_ws_by_username(target)
                if target_ws:
                    await send_to(target_ws, {
                        "type": "friend_request",
                        "author": author,
                        "target": target
                    })

            elif msg_type == "friend_accept":
                target = data.get("target", "")
                author = data.get("author", "Unknown")
                target_ws = find_ws_by_username(target)
                if target_ws:
                    await send_to(target_ws, {
                        "type": "friend_accept",
                        "author": author,
                        "target": target
                    })

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        info = clients.pop(ws, {})
        logger.info(f"- Disconnected | {info.get('username', '?')}")


# ── Entry point ──────────────────────────────────────────────────────────────
async def main():
    port = int(os.environ.get("PORT", 8081))
    logger.info(f"IRC server starting on 0.0.0.0:{port}")
    async with websockets.serve(handle, "0.0.0.0", port):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
