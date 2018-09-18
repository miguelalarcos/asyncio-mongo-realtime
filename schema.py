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

    async def create(self):
        if not self.can_insert():
            raise SetError('can not insert ' + self.collection + ', doc: ' + str(self.doc))
        doc = self.insert()
        result = await self.table.insert_one(doc)
        self.id = result.inserted_id

    async def load(self):
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

    def can_insert(self):
        return False

    def can_update(self):
        return False

    async def set(self, doc):
        if not self.can_update():
            raise SetError('can not update ' + self.collection + ', id: ' + str(self.id))
        else:
            for key, value in doc.items():
                self.update(key, value)
            #updated_doc = unflatten(doc_tmp.doc, splitter=point_splitter)
            result = await self.table.replace_one({'_id': ObjectId(self.id)}, self.doc) 
            return result.modified_count

    def insert(self, document=None):
        if not document:
            document = self.doc
        schema = self.schema
        ret = {}
        set_document = set(document.keys())
        set_schema = set(schema.keys()) - set(self.reserved_kw)
        intersection =  set_document & set_schema 
        missing = set_schema - set_document
        if len(set_document - set_schema) > 0:
            raise Exception('keywords not in schema')

        for key in missing | intersection:            
            if schema[key]['type'].__class__ == Schema:                
                if document.get(key):                    
                    ret[key] = schema[key]['type'].insert(document[key])
            elif type(schema[key]['type']) is list:
                if document.get(key):
                    ret[key] = [schema[key]['type'][0].insert(k) for k in document[key]]
            elif 'computed' not in schema[key]:
                validation = schema[key].get('validation', public)
                required = schema[key].get('required', False)
                mtype = schema[key]['type']
                initial = schema[key].get('initial')
                initial = initial and initial() 
                v = document.get(key, initial)
                
                if required and v is None:
                    raise ValidationError('required')

                if v is not None and (not isinstance(v, mtype) or not validation(v)):
                    raise ValidationError('not valid prop or missing', key)
                if key in intersection or initial is not None: 
                    ret[key] = document.get(key, initial)
            else:
                create = schema[key].get('computed')
                val = create(document) 
                ret[key] = val
        return ret

    def update(self, path, value):
        doc = self.doc
        schema = self.schema
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
            
            if issubclass(schema, Doc) and key != last:                
                try:                
                    doc = doc[key]
                    schema = schema.schema
                except KeyError:
                    raise PathError('path does not exist in doc', key)

        if type(schema) is list:
            raise SetError('can not set an array')
        if issubclass(schema, Doc):
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

