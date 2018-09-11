import asyncio
import websockets
from sdp import sdp, method, sub, db

@method
async def add(a, b):
    return a + b

@sub
def x_less_than(max):
    return db.test, {"$lt": {'x': max}}

def main():
    start_server = websockets.serve(sdp, 'localhost', 8888)

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()