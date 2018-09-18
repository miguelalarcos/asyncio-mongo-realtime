from bson import ObjectId
from sdp import db

class SetError(Exception):
    pass

class ValidationError(Exception):
    pass

class PathError(Exception):
    pass

public = lambda *args: True
never  = lambda *args: False

class Schema:
    pass
  

class Doc:
    reserved_kw = ['__get', '__set', '__set_document', '__get_document', \
                   '__create_document', '__ownership', '__set_default', \
                   '__get_default']
    def __init__(self, id=None, doc=None, user_id=None):
        self.id = id
        self.doc = doc
        self.user_id = user_id
        table = self.collection
        self.table = db[table]

    async def insert(self):
        self.id = await self.insert(doc)
        
    async def load(self):
        #table = self.collection
        #table = db[table]
        self.doc = await self.table.find_one({'_id': ObjectId(self.id)})
        if self.doc is None:
            raise SetError('document not found', self.id, self.collection)
        return self

    def __repr__(self):
        return str(self.doc)

    def __getitem__(self, key):
        return self.doc[key]

    def __setitem__(self, key, value):
        self.doc[key] = value

    def can_update(self):
        return True

    async def set(self, doc):
        if not self.can_update():
            raise SetError('can not update ' + self.collection + ', id: ' + str(self.id))
        else:
            for key, value in doc.items():
                self.update(key, value)
            #updated_doc = unflatten(doc_tmp.doc, splitter=point_splitter)
            result = await self.table.replace_one({'_id': ObjectId(self.id)}, self.doc) 
            return result.modified_count

    def update(self, path, value):
        doc = self.doc
        schema = self.schema.schema
        set_default = schema.get('__set_default', never)
        validation_default = public
        paths = path.split('.')
        last = paths[-1]

        for key in paths:
            if key.isdigit():
                key = int(key)
            else:
                try:
                    schema[key]
                except KeyError:
                    raise PathError('path does not exist in schema', key)
                validation = schema[key].get('validation', validation_default)
                if key == last:
                    schema = schema[key]['type']  
                    break
                set_ = schema[key].get('set', set_default)
                
                if not set_():
                    raise SetError('can not set')     
                schema = schema[key]['type']          
            
            if type(schema) is list:
                schema = schema[0]
                try:
                    doc = doc[key]
                except IndexError:
                    raise PathError('path does not exist in doc', key)
            
            if issubclass(schema, Schema) and key != last:                
                try:                
                    doc = doc[key]
                    schema = schema.schema
                except KeyError:
                    raise PathError('path does not exist in doc', key)

        if type(schema) is list:
            raise SetError('can not set an array')
        if issubclass(schema, Schema):
            schema = schema.schema
            set_default = schema.get('__set_default', never)
            keys = [k  for k in schema.keys() if k not in self.reserved_kw] 
            for k in keys:
                try:
                    schema[k]
                    value[k]
                    set_ = schema[k].get('set', set_default)
                except KeyError:
                    raise PathError('path does not exist', k)
                #except TypeError:
                #    raise PathError('type error path does not exist', k)
                
                if not set_(): 
                    raise SetError('can not set', k, value)    
                if not schema[k]['type'] == type(value[k]) or not schema[k].get('validation', public)(value[k]):
                    raise ValidationError('can not set (validation)', k, value)
                doc[key].update({k: value[k]})

        elif isinstance(value, schema) and validation(value):
            try:
                doc[key] = value
            except IndexError:
                raise PathError('path does not exist in doc', key)
        else:
            raise ValidationError('not valid', key, value)

