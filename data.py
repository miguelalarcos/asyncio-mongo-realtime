from asyncio import get_event_loop
import asyncio
import motor.motor_asyncio
from bson.timestamp import Timestamp
from time import time

client = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://miguel-alarcos:liberal78@cluster0-oo7pa.mongodb.net/test?retryWrites=true')

db = client.test
c = db.test

async def main():
    #k = await c.insert_one({'x': 5, '$currentDate': {'__timestamp': { '$type': "timestamp" }}})
    k = await c.insert_one({'x': 5})
    _id = k.inserted_id
    await c.update_one({'_id': _id}, {'$set': {'x': 579}, '$currentDate': {'__timestamp': { '$type': "timestamp" }}})

loop = get_event_loop()
loop.run_until_complete(main())
    
