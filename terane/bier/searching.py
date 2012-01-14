import time, datetime
from copy import deepcopy
from bisect import insort_right
from zope.interface import Interface, implements
from terane.bier.docid import DocID
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
    def postingsLength(fieldname, term, period):
        "Returns the maximum number of possible postings for the term in the specified field."
    def iterPostings(fieldname, term, period, reverse):
        "Returns an object implementing IPostingList which yields docIds for each posting of the term in the specified field."
    def getEvent(docId):
        "Returns the event specified by docId."
    def __exit__(excType, excValue, traceback):
        "Exit the transactional context."

class SearcherError(Exception):
    pass

class Period(object):
    def __init__(self, start, end, startexcl, endexcl, epoch=None):
        self.start = int(time.mktime(start.timetuple()))
        self.end = int(time.mktime(end.timetuple()))
        self.startexcl = startexcl
        self.endexcl = endexcl
        self._epoch = epoch
    def getEpoch(self):
        return self._epoch
    def setEpoch(self, epoch):
        self._epoch = epoch
    def startingID(self):
        if self.startexcl:
            return DocID(self.start + 1, 0, 0)
        return DocID(self.start, 0, 0)
    def endingID(self):
        if self.endexcl:
            return DocID(self.end - 1, 2**32 - 1, 2**64 - 1)
        return DocID(self.end, 2**32 - 1, 2**64 - 1)

class Term(object):
    implements(IPostingList)
    def __init__(self, fieldname, value):
        if fieldname == None:
            self.fieldname = 'default'
        else:
            self.fieldname = fieldname
        self.value = unicode(value)
    def __str__(self):
        return "<Term %s=%s>" % (self.fieldname,self.value)
    def optimizeQuery(self, index):
        schema = index.schema()
        if not schema.has(self.fieldname):
            return None
        field = schema.get(self.fieldname)
        terms = list([term for term,_ in field.terms(self.value)])
        if len(terms) == 0:
            return None
        elif len(terms) > 1:
            return OR([Term(self.fieldname,t).optimizeQuery(index) for t in terms])
        self.value = terms[0]
        return self
    def postingsLength(self, searcher, period):
        length = searcher.postingsLength(self.fieldname, self.value, period)
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length
    def iterPostings(self, searcher, period, reverse):
        self._postings = searcher.iterPostings(self.fieldname, self.value, period, reverse)
        return self
    def nextPosting(self):
        docId = self._postings.nextPosting()[0]
        logger.trace("%s: nextPosting() => %s" % (self, docId)) 
        return docId
    def skipPosting(self, targetId):
        docId = self._postings.skipPosting(targetId)[0]
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, docId))
        return docId

class AND(object):
    implements(IPostingList)
    def __init__(self, children):
        self.children = children
        self._lengths = None
    def __str__(self):
        return "<AND [%s]>" % ', '.join([str(child) for child in self.children])
    def optimizeQuery(self, index):
        children = []
        for child in self.children:
            child = child.optimizeQuery(index)
            if isinstance(child, AND):
                for subchild in child.children: children.append(subchild)
            elif child != None:
                children.append(child)
        self.children = children
        if len(self.children) == 0:
            return None
        return self
    def postingsLength(self, searcher, period):
        self._lengths = []
        for child in self.children:
            insort_right(self._lengths, (child.postingsLength(searcher, period),child))
        length = self._lengths[0][0]
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length
    def iterPostings(self, searcher, period, reverse):
        if self._lengths == None:
            self.postingsLength(searcher, period)
        self._smallest = self._lengths[0][1].iterPostings(searcher, period, reverse)
        self._others = [child[1].iterPostings(searcher, period, reverse) for child in self._lengths[1:]]
        return self
    def nextPosting(self):
        while True:
            docId = self._smallest.nextPosting()
            if docId == None:
                logger.trace("%s: nextPosting() => %s" % (self, docId)) 
                return docId
            for child in self._others:
                if child.skipPosting(docId) == None:
                    docId = None
                    break
            if docId != None:
                logger.trace("%s: nextPosting() => %s" % (self, docId)) 
                return docId
    def skipPosting(self, targetId):
        docId = self._smallest.skipPosting(targetId)
        if docId != None:
            for child in self._others:
                docId = child.skipPosting(targetId)
                if docId == None:
                    break
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, docId))
        return docId

class OR(object):
    implements(IPostingList)
    def __init__(self, children):
        self.children = children
    def optimizeQuery(self, index):
        children = []
        for child in self.children:
            child = child.optimizeQuery(index)
            if child != None:
                children.append(child)
        self.children = children
        if len(self.children) == 0:
            return None
        return self
    def postingsLength(self, searcher, period):
        length = 0
        for child in self.children:
            length += child.postingsLength(searcher, period)
        return length
    def iterPostings(self, searcher, period, reverse):
        pass

class Results(object):
    def __init__(self, results, fields):
        self.meta = dict()
        self._results = results
        self.fields = fields

    def __len__(self):
        return len(self._results)

    def __iter__(self):
        return iter(self._results)

    def __getitem__(self, i):
        return self._results[i]

def searchIndices(indices, query, period, reverse=False, fields=None, limit=100):
    postings = []
    for index in indices:
        _query = deepcopy(query).optimizeQuery(index)
        logger.debug("optimized query for index '%s': %s" % (index.name,str(_query)))
        if _query != None:
            searcher = index.searcher()
            piter = _query.iterPostings(searcher, period, reverse)
            i = 0
            while i < limit:
                docId = piter.nextPosting()
                if docId == None:
                    break
                logger.trace("found event %s" % docId)
                postings.append((docId, searcher))
                i += 1
    postings.sort()
    foundfields = []
    results = []
    for docId,searcher in postings[:limit]:
        event = searcher.getEvent(docId)
        for fieldname in event.keys():
            if fieldname not in foundfields: foundfields.append(fieldname)
        if fields != None:
            event = dict([(k,v) for k,v in event.items() if k in fields])
        results.append((str(docId), event))
    return Results(results, foundfields)
