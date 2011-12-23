from zope.interface import Interface

class ISchema(Interface):
    def has(name):
        "Returns True if schema has the specified field, otherwise False."
    def get(name):
        "Returns the specified Field."
    def add(name, field):
        "Adds a new field to the schema."

class BaseField(object):
    def __init__(self, options):
        self.options = options

    def terms(self, value):
        raise NotImplemented()

class IdentityField(BaseField):
    def terms(self, value):
        if not isinstance(value, list) and not isinstance(value, tuple):
            raise Exception("value '%s' is not of type list or tuple" % value)
        return iter([(unicode(t.strip()),None) for t in value)])

class TextField(BaseField):
    def terms(self, value):
        if not isinstance(value, unicode):
            raise Exception("value '%s' is not of type unicode or str" % value)
        return iter([(unicode(t),None) for t in value.split() if len(t) > 0])

class DatetimeField(BaseField):
    def terms(self, value):
        if not isinstance(value, datetime.datetime):
            raise Exception("value '%s' is not of type datetime.datetime" % value)
        # calculate the unix timestamp, with 1 second accuracy
        ts = int(time.mktime(value.timetuple()))
        # pack the int as a 32 bit big-endian integer
        ts = struct.pack(">I", ts)
        # convert the packed int to base64   
        ts = unicode(base64.standard_b64encode(ts))
        return iter([(ts, None)])

def fieldFactory(evalue, **options):
    if isinstance(evalue, str) or isinstance(evalue, unicode):
        return TextField(options)
    if isinstance(evalue, datetime.datetime):
        return DatetimeField(options)
    if isinstance(evalue, list) or isinstance(evalue, tuple):
        return IdentityField(options)
    raise TypeError("unknown event value type '%s'" % str(type(evalue)))
