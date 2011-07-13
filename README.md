Postoffice: Simple threaded messaging using Cassandra
-----------------------------------------------------

Postoffice is a simple threaded messaging system on top of Cassandra 
meant to serve as a basic guide for using Cassandra from Java.

The system can either be accessed using the Java API or via an HTTP 
interface that returns JSON.

Example Java calls:
-------------------

    String alice = "1501571";
    String bob = "1501572";

    String from = alice;
    String to = bob;

    PostofficeUtil.startConversation(
      from, to, 
      "Postoffice project!", 
      "Hi " + to + ", I just wanted to tell you about Postoffice, a messaging system using Cassandra!"
    );

Example HTTP requests:
----------------------

Get first 10 messages in a the "inbox" folder for "1501572":

    http://localhost:8081/folder/?folder=inbox&owner=1501572&start=0&count=10

Create a new conversation:

    http://localhost:8081/new/?from=1501572&to=1501571&subject=hey&body=foobarbaz

Reply to an existing conversation given its id:

    http://localhost:8081/reply/?from=1501571&to=1501572&subject=hey&body=foobarbazreply&id=218779d1-ac51-11e0-8616-005056c00008

Cassandra schema:
-----------------

    create column family folders with column_type = 'Standard' and comparator = 'TimeUUIDType';
    create column family conversations with column_type = 'Standard' and comparator = 'TimeUUIDType';

