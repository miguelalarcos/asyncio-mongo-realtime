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
    def __init__(self, doc):
        self.doc = doc

    def __repr__(self):
        return str(self.doc)

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

