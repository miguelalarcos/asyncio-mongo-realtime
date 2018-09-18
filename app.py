import asyncio
import websockets
from sdp import sdp, method, sub
from schema import Doc, Schema, public

class XSchema(Schema):
    schema = {
        "__set_default": public,
        'x': {
            'type': int,
            'validation': lambda v: v > -1000
        }
    }  

    def can_update(self):
        return self['user_id'] == self.user_id

class XDoc(Doc):
    collection = 'test'
    schema = XSchema

@method
async def add(user_id, a, b):
    return a + b

@method 
async def set_x(user_id, id, value):
    #await update(XDoc, id, {'x': value}, can=lambda old: old.user_id == user_id)
    doc = await XDoc(id, user_id).load()
    #doc = await XDoc.create(id, user_id)
    await doc.set({'x': value})

@sub
def x_less_than(user_id, max):
    return 'test', {'x': {'$lt': max}}

def main():
    start_server = websockets.serve(sdp, 'localhost', 8888)

    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()