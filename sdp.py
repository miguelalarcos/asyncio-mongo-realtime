# SDP: Subscription Data Protocol

from asyncio import get_event_loop
import asyncio
import websockets
import motor.motor_asyncio
from bson.timestamp import Timestamp
from bson import ObjectId
from itertools import chain, repeat
import json
from datetime import datetime
#import pytz

from flatten_dict import flatten, unflatten
from dotenv import load_dotenv
import os

load_dotenv()
URI_DATABASE = os.getenv("URI_DATABASE")

def point_reducer(k1, k2):
    if k1 is None:
        return k2
    else:
        return k1 + "." + k2

def point_splitter(flat_key):
    return flat_key.split(".")

client = motor.motor_asyncio.AsyncIOMotorClient(URI_DATABASE)
db = client.test

methods = {}

def method(f):
    #methods.append(f.__name__)
    async def helper(*args, **kwargs):
        return await f(*args, **kwargs)
    methods[f.__name__] = helper
    return helper

subs = {}

def sub(f):
    #subs.append(f.__name__)
    subs[f.__name__] = f
    return f

def check(attr, type):
    if not isinstance(attr, type):
        raise CheckError(attr + ' is not of type ' + str(type))

hooks = {'before_insert': [],
         'before_update': []
         }

def before_insert(collection=None):
  def decorator(f):
    def helper(coll, doc):
      if collection == coll or collection is None:
        f(doc)
    hooks['before_insert'].append(helper)
    return f # does not matter, it's not going to be used directly, but helper in hooks
  return decorator

def before_update(collection=None):
  def decorator(f):
    def helper(coll, doc):
      if collection == coll or collection is None:
        f(doc)
    hooks['before_update'].append(helper)
    return f # does not matter, it's not going to be used directly, but helper in hooks
  return decorator


class MethodError(Exception):
  pass

class CheckError(Exception):
  pass

can = {'update': [], 'insert': [], 'delete': []}

def can_insert(table):
    def decorate(f):
        def helper(t, doc):
            if t == table:
                return f(doc)
            else:
                return True
        can['insert'].append(helper)
        return f # does not matter f or helper, it's not going to be used directly
    return decorate

def can_update(table):
    def decorate(f):
        def helper(t, doc, old_doc):
            if t == table:
                return f(doc, old_doc)
            else:
                return True
        can['update'].append(helper)
        return f # does not matter f or helper, it's not going to be used directly
    return decorate

def can_delete(table):
    def decorate(f):
        def helper(t, old_doc):
            if t == table:
                return f(old_doc)
            else:
                return True
        can['delete'].append(helper)
        return f # does not matter f or helper, it's not going to be used directly
    return decorate


async def sdp(websocket, path):

    async def watch(c, sub_id, query): #, name): 
        change_stream = None
        done = False

        async def _find_with_sleep(query={}):
            for x in chain([4, 8, 16], repeat(32)):
                if not done:
                    await do_find(query)
                else:
                    break
                await asyncio.sleep(x)

        async def find_with_sleep(query={}):
            if not done:
                await do_find(query)
            await asyncio.sleep(4)
            if not done:
                done = True
                await do_find(query)

        async def do_find(query={}):
            print('send initializing', query)
            await send_initializing(sub_id)
            async for document in c.find(query):
                await send_added(sub_id, document)
                print('send document', document)
            print('send ready')
            await send_ready(sub_id)

        watch_query = {}
        for key in query.keys():
            watch_query['fullDocument.' + key] = query[key]
        async with c.watch([{"$match": watch_query}]) as change_stream:
            try:
                asyncio.create_task(find_with_sleep(query))
                #await do_find(query)
                async for change in change_stream:
                    print('send delta', change)
                    if not done:
                        done = True
                        await do_find(query)
                    type_ = change['operationType']
                    _id = str(change['fullDocument']['_id'])

                    if type_ == 'replace':
                        await send_changed(sub_id, change['fullDocument'])
                    elif type_ == 'insert':
                        await send_added(sub_id, change['fullDocument'])
                    elif type_ == 'delete':
                        await send_removed(sub_id, _id)

            except Exception as e:
                print('closing stream', e)
                change_stream.close()

    async def send(data):
        def helper(x):
            if isinstance(x, datetime):
                return {'$date': x.timestamp()*1000}
            elif isinstance(x, ObjectId):
                return str(x)
            elif isinstance(x, Timestamp):
                return x.time
            else:
                return x
        message = json.dumps(data, default=helper)
        await websocket.send(message)

    async def send_result(id, result):
        await send({'msg': 'result', 'id': id, 'result': result})

    async def send_error(id, error):
        await send({'msg': 'error', 'id': id, 'error': error})

    async def send_added(sub_id, doc):
        doc['id'] = doc['_id']
        del doc['_id']
        await send({'msg': 'added', 'id': sub_id, 'doc': doc})

    async def send_changed(sub_id, doc):
        doc['id'] = doc['_id']
        del doc['_id']
        await send({'msg': 'changed', 'id': sub_id, 'doc': doc})

    async def send_removed(sub_id, doc_id):
        await send({'msg': 'removed', 'id': sub_id, 'doc_id': doc_id})

    async def send_ready(sub_id):
        await send({'msg': 'ready', 'id': sub_id})

    async def send_initializing(sub_id):
        await send({'msg': 'initializing', 'id': sub_id})    

    async def send_nosub(sub_id, error):
        await send({'msg': 'nosub', 'id': sub_id, 'error': error})

    async def send_nomethod(method_id, error):
        await send({'msg': 'nomethod', 'id': method_id, 'error': error})

    registered_feeds = {}
    #feeds_with_observers = []
    user_id = 'miguel@aaa.com' #None
    #remove_observer_from_item = {}
    
    try:
        async for msg in websocket:
            #if msg == 'stop':
            #    return
            def helper(dct):
                if '$date' in dct.keys():
                    d = datetime.utcfromtimestamp(dct['$date']/1000.0)
                    return d
                    #return d.replace(tzinfo=pytz.UTC)
                return dct
            data = json.loads(msg, object_hook=helper)
            print('>>>', data)
            try:
                message = data['msg']
                id = data['id']

                if message == 'method':
                    params = data['params']
                    method = data['method']
                    if method not in methods.keys():
                        await send_nomethod(id, 'method does not exist')
                    else:
                        #try:
                            method = methods[method]
                            result = await method(**params)
                            await send_result(id, result)
                        #except Exception as e:
                        #  self.send_error(id, str(e) + ':' + str(e.__traceback__))
                elif message == 'sub':
                    #name = data['name']
                    params = data['params']
                    if id not in subs.keys():
                        await send_nosub(id, 'sub does not exist')
                    else:
                        c, query = subs[id](**params)
                        c = db[c]
                        registered_feeds[id] = asyncio.create_task(watch(c, id, query))  #, name))
                elif message == 'unsub':
                    feed = registered_feeds[id]
                    feed.cancel()
                    #if remove_observer_from_item.get(id):
                    #    for remove in remove_observer_from_item[id].values():
                    #        remove()
                    #    del remove_observer_from_item[id]
                    #del registered_feeds[id]
            except KeyError as e:
                await send_error(id, str(e))
            #
    finally:
        #for k in remove_observer_from_item.keys():
        #    for remove in remove_observer_from_item[k].values():
        #        remove()
        for feed in registered_feeds.values():
            print('cancelling feed')
            feed.cancel()
   

async def insert(table, doc):
    cans = [c(table, doc) for c in can['insert']]
    if not all(cans):
        raise MethodError('can not insert ' + table)
    else:
        before_insert(table, doc)
        result = await table.insert_one(doc)
        return result.inserted_id

def before_insert(collection, doc):
    for hook in hooks['before_insert']:
        hook(collection, doc)

async def update(table, id, doc):
    table = db[table]
    old_doc = await table.find_one({'_id': ObjectId(id)})
    cans = [c(table, doc, old_doc) for c in can['update']]
    if not all(cans):
        raise MethodError('can not update ' + table + ', id: ' + str(id))
    else:
        before_update(table, doc)
        #doc = old_doc.update(doc) # improve this with dot notation
        old_doc = flatten(old_doc, reducer=point_reducer)
        old_doc.update(doc)
        doc = unflatten(doc, splitter=point_splitter)
        result = await table.replace_one({'_id': ObjectId(id)}, doc) 
        return result.modified_count

def before_update(collection, subdoc):
    for hook in hooks['before_update']:
        hook(collection, subdoc)

"""
@gen.coroutine
def soft_delete(self, table, id):
    conn = yield self.conn
    old_doc = yield r.table(table).get(id).run(conn)
    cans = [c(self, table, old_doc) for c in can['delete']]
    if not all(cans):
    raise MethodError('can not delete ' + table + ', id: ' + str(id))
    else:
    result = yield r.table(table).get(id).update({'deleted': True}).run(conn)

async def update_many(self, table, f, u, limit=None):
    conn = yield self.conn
    result = 0
    if limit:
        result = yield r.table(table).filter(f).limit(limit).update(lambda item: r.branch(f(item), u, {})).run(conn)
    else:
        result = yield r.table(table).filter(f).update(lambda item: r.branch(f(item), u, {})).run(conn)
    return result['replaced']
"""
