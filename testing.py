import asyncio, json, websockets
async def t():
    async with websockets.connect("ws://127.0.0.1:42070") as ws:
        await ws.send(json.dumps({
            "file": "demo/test.py",
            "changes": [{"insert":"hola","index":0}]
        }))
        for _ in range(5):
            print(await ws.recv())
asyncio.run(t())