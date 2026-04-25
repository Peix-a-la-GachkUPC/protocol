import asyncio
import json
from collections import deque
from typing import Any, Awaitable, Callable, Optional

from websockets.asyncio.server import ServerConnection, serve


Message = Any
MessageHandler = Callable[[Message], Optional[Awaitable[None]]]


class ExtensionBridge:
    def __init__(self, host: str = "127.0.0.1", port: int = 42069) -> None:
        self.host = host
        self.port = port
        self._pending_outgoing: deque[Message] = deque()
        self._clients: set[ServerConnection] = set()
        self._handler: Optional[MessageHandler] = None
        self._server: Any = None
        self._lock = asyncio.Lock()

    def on_message(self, handler: MessageHandler) -> None:
        self._handler = handler

    async def start(self) -> None:
        if self._server is not None:
            return

        self._server = await serve(self._handle_client, self.host, self.port)

    async def stop(self) -> None:
        if self._server is None:
            return

        for client in list(self._clients):
            await client.close()

        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def send(self, payload: Message) -> None:
        async with self._lock:
            if not self._clients:
                self._pending_outgoing.append(payload)
                return

            message = json.dumps(payload, ensure_ascii=False)
            disconnected: list[ServerConnection] = []
            for client in self._clients:
                try:
                    await client.send(message)
                except Exception:
                    disconnected.append(client)

            for client in disconnected:
                self._clients.discard(client)

    async def _flush_pending(self) -> None:
        while self._pending_outgoing and self._clients:
            payload = self._pending_outgoing.popleft()
            await self.send(payload)

    async def _handle_client(self, socket: ServerConnection) -> None:
        self._clients.add(socket)
        await self._flush_pending()

        try:
            async for raw in socket:
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if self._handler is None:
                    continue

                result = self._handler(message)
                if result is not None:
                    await result
        finally:
            self._clients.discard(socket)


async def _main() -> None:
    bridge = ExtensionBridge()

    async def printer(message: Message) -> None:
        print(f"[recv] {json.dumps(message, ensure_ascii=False)}")

    bridge.on_message(printer)
    await bridge.start()

    print(f"Listening on ws://{bridge.host}:{bridge.port}")
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        pass
    finally:
        await bridge.stop()


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
