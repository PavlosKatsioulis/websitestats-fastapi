from typing import Dict, Set, Optional
from fastapi import WebSocket
from starlette.websockets import WebSocketState
import asyncio
import logging

log = logging.getLogger("ws_manager")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(h)
    log.setLevel("INFO")


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: Dict[int, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, user_id: int, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.setdefault(user_id, set()).add(websocket)
        log.info("User %s connected (now %d sockets)", user_id, await self.user_connection_count(user_id))

    async def disconnect(self, user_id: int) -> None:
        async with self._lock:
            sockets = self._connections.pop(user_id, set())
        for ws in list(sockets):
            await self._safe_close(ws)
        log.info("User %s disconnected (all)", user_id)

    async def disconnect_socket(self, user_id: int, websocket: WebSocket) -> None:
        async with self._lock:
            s = self._connections.get(user_id)
            if s and websocket in s:
                s.remove(websocket)
                if not s:
                    self._connections.pop(user_id, None)
        await self._safe_close(websocket)
        log.info("User %s disconnected (one socket)", user_id)

    async def send_personal_message(self, message: dict, user_id: int) -> None:
        async with self._lock:
            sockets = list(self._connections.get(user_id, []))
        if not sockets:
            log.warning("⚠No active connection for user %s", user_id)
            return

        dead: list[WebSocket] = []
        for ws in sockets:
            if not self._is_open(ws):
                dead.append(ws)
                continue
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                s = self._connections.get(user_id)
                if s:
                    for ws in dead:
                        s.discard(ws)
                    if not s:
                        self._connections.pop(user_id, None)
            for ws in dead:
                await self._safe_close(ws)
        log.info("Sent personal message to user %s (sockets: %d ok, %d pruned)", user_id, len(sockets) - len(dead), len(dead))

    async def broadcast(self, message: dict) -> None:
        async with self._lock:
            snapshot = {uid: list(socks) for uid, socks in self._connections.items()}

        total = sum(len(v) for v in snapshot.values())
        log.info("Broadcasting to %d sockets (%d users)", total, len(snapshot))

        for user_id, sockets in snapshot.items():
            dead: list[WebSocket] = []
            for ws in sockets:
                if not self._is_open(ws):
                    dead.append(ws); continue
                try:
                    await ws.send_json(message)
                except Exception:
                    dead.append(ws)
            if dead:
                async with self._lock:
                    s = self._connections.get(user_id)
                    if s:
                        for ws in dead:
                            s.discard(ws)
                        if not s:
                            self._connections.pop(user_id, None)
                for ws in dead:
                    await self._safe_close(ws)

    async def user_connection_count(self, user_id: int) -> int:
        async with self._lock:
            return len(self._connections.get(user_id, []))

    async def total_connection_count(self) -> int:
        async with self._lock:
            return sum(len(s) for s in self._connections.values())

    @staticmethod
    def _is_open(ws: WebSocket) -> bool:
        return ws.application_state == WebSocketState.CONNECTED

    @staticmethod
    async def _safe_close(ws: WebSocket) -> None:
        try:
            if ws.application_state == WebSocketState.CONNECTED:
                await ws.close()
        except Exception:
            pass


manager = ConnectionManager()
