=========================
The Terane Query Language
=========================

The Terane query language (henceforth referred to in this document as TQL,
or just QL) is a declarative language similar to SQL.  TQL is used to
describe the events we want to retrieve from the terane server.  However,
in order to describe the events we are interested in, we need to have a
thorough understanding of what an event is.

Anatomy Of An Event
-------------------

An event is an occurance of something interesting.  This sounds vague, but
it is the user who derives meaning from the event; terane just stores it
and lets you retrieve it.  An event contains the four W's: who, where, when,
and what:

 * Who: The input which generated the event.
 * Where: The hostname of the system which generated the event.
 * When: The timestamp of the event.
 * What: The fields which describe the event context.

The 'What' of an event bears more explaining, as the term 'field' is ambiguous.
In Terane, a field is similar to a column in a relational database, and it
describes a certain aspect of an event.  The hostname and input aspects of an
event as described in Who and Where are both fields.  A field has a name and a
type, also like in an RDBMS.  However, fields differ from RDBMS columns in one
crucial respect: whereas the columns of a table in an RDBMS are dictated by a
schema and are fixed (unless you perform a table alter, but that is a costly
and infrequent operation), an event's schema is dynamic: not all fields need be
present in all events, and new fields may be added at any time.  There are some
fields though which are always required: input, hostname, and the default field.
The timestamp is a special case, it is part of the event identifier and is not
a field.

ITER Queries
------------

The basic ad-hoc query is an ITER query.  An ITER query consists of two clauses:
the subject expression and an optional date predicate.  The date predicate
defines the span of time within which events will be retrieved.  If the date
predicate is not specified, then it is implicitly set to 'one hour ago until now'.

The most basic ITER query is to retrieve all events within the date predicate::

 ALL

To change the date predicate to retrieve all events from the last twelve hours,
we add the WHERE clause::

 ALL WHERE DATE FROM 12 HOURS AGO

The date predicate can take a start and an end.  To retrieve all events
starting twelve hours ago until six hours ago::

 ALL WHERE DATE FROM 12 HOURS AGO TO 6 HOURS AGO

By default, both ends of the date are inclusive.  We can make the start or end
or both exclusive::

 ALL WHERE DATE FROM 12 HOURS AGO EXCLUSIVE TO 6 HOURS AGO
 ALL WHERE DATE FROM 12 HOURS AGO TO 6 HOURS AGO EXCLUSIVE
 ALL WHERE DATE FROM 12 HOURS AGO EXCLUSIVE TO 6 HOURS AGO EXCLUSIVE

The start and end in a date predicate can be relative, as we have seen above,
or they can be absolute.  Relative dates accept units of SECONDS, MINUTES,
HOURS, and DAYS::

 ALL WHERE DATE FROM 100 SECONDS AGO
 ALL WHERE DATE FROM 100 MINUTES AGO
 ALL WHERE DATE FROM 100 HOURS AGO
 ALL WHERE DATE FROM 100 DAYS AGO

Absolute dates can consist of a date only, or a date and time.  The time is
always assumed to be UTC.  The date-only format is <YYYY>/<MM>/<DD>, and the
date-time format is <YYYY>/<MM>/<DD>T<HH>:<MM>:<SS>::

 ALL WHERE DATE FROM 2000/1/1
 ALL WHERE DATE FROM 2000/1/1T12:00:00 TO 2000/1/1T18:00:00

Specifying Fields
~~~~~~~~~~~~~~~~~

Retrieving all events is not a very interesting query.  After all, a simple
grep of a log file could do the same thing right?  We want to restrict our
query to only events matching a criteria other than date range.  For example,
say we want to find events containing the term 'foobar' in the past day::

 foobar WHERE DATE FROM 1 DAY AGO

For most queries of this type, you can just put the term in the subject
expression as-is.  However, if the term happens to be a reserved word, you 
will need to put it in quotes.  For example, to find events containing the
term 'WHERE' in the past day::

 "WHERE" WHERE DATE FROM 1 DAY AGO

Quoting a term also indicates a phrase search, if applicable.  That means that
if your quoted term contains spaces, the entire phrase will be searched for::

 "now is the winter of our discontent" WHERE DATE FROM 1 YEAR AGO

When specifying only the bare term in the subject expression, you are
implicitly indicating to search the default field.  If you want to search a
different field, then you will need to be explicit, by specifying the field
name, an equals-sign, then the term.  For example, to find events originating
from 'foobar.com', we would search the hostname field::

 hostname="foobar.com"

The quoting rules explained above apply in this situation as well, so you can
search for a phrase in a field.

Using Operators
~~~~~~~~~~~~~~~

Often, queries are more complex than matching a single term.  Terane provides
the commonly understood operators AND, OR, and NOT, as well as operator
precedence using parentheses.  For example, to retrieve events matching 'foo'
and 'bar' (intersection) in the default field::

 foo AND bar

To retrieve events matching 'foo' or 'bar' (union) in the default field::

 foo OR bar

To retrieve events which do not match 'foo'::

 NOT foo

To retrieve events which match 'foo' and not 'bar'::

 foo AND NOT bar

To retrieve events which match 'foo' or do not match 'bar'::

 foo OR NOT bar

The natural precedence of operators, from highest precedence to lowest, is
AND, NOT, OR.  However, utilizing these implicit rules can make for a less
readable query, and furthermore sometimes it is necessary to explicitly state
the precedence of operators in a complex query.  We specify precedence by
using matching parentheses.  For example, to retrieve events matching either
'foo' and 'bar', or 'baz' and 'qux' in the default field::

 (foo AND bar) OR (baz AND qux)

Field Types And Functions
~~~~~~~~~~~~~~~~~~~~~~~~~

TAIL Queries
------------

The is another query type, the TAIL query.  Where an ITER query retrieves 
events in the past, a TAIL query is a request to retrieve events in real time
as they are received by the Terane server.  Thus, a TAIL query consists of
only one clause, the subject expression, and it is an error to specify a date
predicate.  In all other respects however, the subject expression of a TAIL
query is constructed the same way as an ITER query, and can contain the same
terms, operators, and functions.
