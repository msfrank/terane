from zope.interface import Interface

class IIndex(Interface):
    def schema():
        "Returns an object implementing ISchema."
    def reader():
        "Returns an object implementing IReader."
    def writer():
        "Returns an object implementing IWriter."
    def newDocumentId():
        "Returns a new document ID."
