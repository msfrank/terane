from zope.interface import Interface
from terane.bier.schema import fieldFactory

class IWriter(Interface):
    def __enter__():
        "Enter the transactional context."
    def newDocument(docId, document):
        "Create a new document with the specified document ID."
    def newPosting(fieldname, term, docId, value):
        "Create a new posting for the field term with the specified document ID."
    def __exit__(excType, excValue, traceback):
        "Exit the transactional context."

class WriterError(Exception):
    pass

def writeEventToIndex(event, index):
    # verify required fields are present
    if not 'ts' in event:
        raise WriterError("missing required field 'ts'")
    if not 'input' in event:
        raise WriterError("missing required field 'input'")
    if not 'host' in event:
        raise WriterError("missing required field 'host'")

    # make sure ts timezone is UTC
    ts = event['ts']
    # if the ts field is not a datetime, then parse its string representation
    if not isinstance(ts, datetime.datetime):
        raise WriterError("field 'ts' is not of type datetime.datetime")
    # if no timezone is specified, then assume local tz
    if ts.tzinfo == None:
        ts = ts.replace(tzinfo=dateutil.tz.tzlocal())
    # convert to UTC, if necessary
    if not ts.tzinfo == dateutil.tz.tzutc():
        ts = ts.astimezone(dateutil.tz.tzutc())
    event['ts'] = ts
    # set the stored value of the 'ts' field to a pretty string
    fields['&ts'] = ts.isoformat()

    # create a list of valid field names from the passed in fields. a valid
    # field name is defined as any name that starts with an alphabetic character
    fieldnames = [fieldname for fieldname in fields.keys() if fieldname[0].isalpha()]
    # verify that each field name exists in the index schema
    schema = index.schema()
    for fieldname in fieldnames:
        if not schema.has(fieldname):
            schema.add(fieldname, fieldFactory(event[fieldname]))

    # update the index in the context of a transactional writer
    with index.writer() as writer:
       
        # create a document record
        docId = index.newDocumentId()
        logger.trace("created new document with id %s" % docId)
        
        document = {}

        # process the value of each field in the event
        for fieldname in fieldnames:
            # create a child transaction for each field in the document
            field = schema.get(fieldname)
            # update the field with the event value
            evalue = event.get(fieldname)
            for term,tvalue in field.terms(evalue):
                writer.newPosting(self.name, term, docId, tvalue)
            # if the full field value should be stored alongside the document,
            # then fill in the storedvalues array with the actual field value.
            # if the key '&<fieldname>' exists, then use its value as the stored
            # value instead.
            storedname = "&" + fieldname
            if storedname in fields:
                document[fieldname] = event[storedname]
            else :
                document[fieldname] = evalue

        # store the document data
        logger.trace("doc=%s: event=%s" % (docId,document))
        writer.newDocument(docId, document)
    # return the document id
    return docId