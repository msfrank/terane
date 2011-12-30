import datetime
from zope.interface import Interface
from terane.loggers import getLogger

logger = getLogger('terane.bier.searching')

class IPostingList(Interface):
    def nextPosting():
        "Returns the next docId, or raises StopIteration if finished."
    def skipPosting(targetId):
        "Skips to the targetId, or raises StopIteration if finished."

class ISearcher(Interface):
    def __enter__():
        "Enter the transactional context."
    def postingsLength(fieldname, term):
        "Returns the maximum number of possible postings for the term in the specified field."
    def iterPostings(fieldname, term, reverse):
        "Returns an object implementing IPostingList which yields docIds for each posting of the term in the specified field."
    def getDocument(docId):
        "Returns the document specified by docId."
    def __exit__(excType, excValue, traceback):
        "Exit the transactional context."

class SearcherError(Exception):
    pass

class DateRange(object):
    def __init__(self, start, end, startexcl, endexcl):
        self.start = int(time.mktime(start.timetuple()))
        self.end = int(time.mktime(end.timetuple()))
        self.startexcl = startexcl
        self.endexcl = endexcl

class Term(object):
    def __init__(self, fieldname, value):
        self.fieldname = unicode(fieldname)
        self.value = unicode(value)
    def postingsLength(self, searcher):
        return searcher.postingsLength(self.fieldname, self.value)
    def iterPostings(self, searcher, daterange):
        return searcher.postings(self.fieldname, self.value, daterange)

class Prefix(object):
    def __init__(self, fieldname, value):
        self.fieldname = unicode(fieldname)
        self.value = unicode(value)
    def postingsLength(self, searcher):
        pass
    def iterPostings(self, searcher, daterange):
        pass

class AND(object):
    def __init__(self, *children):
        self.children = children
    def postingsLength(self, searcher):
        length = None
        for child in self.children:
            _length = child.postingsLength(searcher)
            if length == None or _length < length: length = _length
        return length
    def iterPostings(self, searcher, daterange):
        pass

class OR(object):
    def __init__(self, *children):
        self.children = children
    def postingsLength(self, searcher):
        length = 0
        for child in self.children:
            length += child.postingsLength(searcher)
        return length
    def iterPostings(self, searcher, daterange):
        pass

class _ExceededLimit(Exception):
    pass

class Results(object):
    def __init__(self, fields, limit):
        self.fields = fields
        self.limit = limit
        self._events = list()
    def __len__(self):
        return len(self._events)
    def add(self, event):
        if len(self._events) >= self.limit:
            raise _ExceededLimit()
        if self.fields != None:
            event = dict([(k,v) for k,v in event.items() if k in self.fields])
        self._events.append(event)

def searchIndex(index, query, daterange, reverse=False, fields=None, limit=100):
    results = Results(fields, limit)
    searcher = index.searcher()
    try:
        for event in query.iterPostings(searcher, daterange):
            results.add(event)
    except _ExceededLimit:
        pass
    return results
