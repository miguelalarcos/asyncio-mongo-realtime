from asyncio import get_event_loop
import asyncio
import motor.motor_asyncio
from bson.timestamp import Timestamp
from itertools import chain, repeat

#client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/?replicaSet=foo')
client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://miguel-alarcos:liberal78@cluster0-oo7pa.mongodb.net/test?retryWrites=true')

db = client.test
c = db.test

data = {}

async def watch_collection(c, query):
    change_stream = None
    done = False
    global data

    async def find_with_sleep(query={}):
        for x in chain([1, 2, 4, 8, 16], repeat(32)):
            if not done:
                await do_find(query)
            else:
                break
            await asyncio.sleep(x)


    async def do_find(query={}):
        print('send initializing')
        global data
        data = {}
        async for document in c.find(query):
            data[str(document['_id'])] = document
            print('send document', document)
        print('ready')
        print(data)

    async with c.watch([{"$match": query}]) as change_stream:
        r = asyncio.create_task(find_with_sleep(query))
        print(r, dir(r))
        async for change in change_stream:
            if not done:
                done = True
                await do_find(query)
                
            print('send delta', change)
            _id = str(change['fullDocument']['_id'])
            type_ = change['operationType']
            if type_ == 'replace':
                timestamp = change['fullDocument']['__timestamp']
                if timestamp > data[_id]['__timestamp']:
                    data[_id] = change['fullDocument']
            elif type_ == 'insert':
                if _id not in data.keys():
                    data[_id] = change['fullDocument']
            elif type_ == 'delete':
                if _id in data.keys():
                    del data[_id]
        change_stream.close()

def main():
    loop = get_event_loop()
    loop.run_until_complete(watch_collection(c, {}))
    