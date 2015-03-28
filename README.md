# essential-dynamo
A toy implementation of some of the essential parts of [Dynamo DB](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).
Use [MapDB](http://www.mapdb.org/) as the underlying persistent layer (while Dynamo uses Berkley DB).

Dynoma, although criticized due to its client-centered conflict resolution and its flawed architecture, is actually a very good resource to learn some great ideas in distributed systems, such as consistent hash, vector timestamp, availability-consistency trade-off, etc.
