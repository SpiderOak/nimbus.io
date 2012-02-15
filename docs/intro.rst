Introduction
============


.. toctree::
   :maxdepth: 10
   :numbered:

`Nimbus.io <https://nimbus.io/>`_ is a horizontally scalable open source
software for large data storage.

Nimbus.io is also a planned a commercial cloud storage service offered by
SpiderOak in Q1 of 2012, which will internally use the Nimbus.io software.

Providing a REST API over HTTP, Nimbus.io allows developers to make use of bulk
storage within their applications at reduced cost.

This documentation includes information for:

 * Developers building applications on top of Nimbus.io
 * Administrators wishing to operate Nimbus.io at their own site
 * Programmers wishing to participate in the development of the Nimbus.io
   storage software itself.

Nimbus.io differs from other distributed storage systems like Amazon's S3,
OpenStack's Swift, and Riak in that it uses parity instead of replication to
achieve highly redundant storage with less overhead.  Internally, the software
is designed around a Message and Component Based Architecture.  It is written
mostly in Python but can be accessed from any programming language.

Nimbus.io was created out of necessity by the storage engineers at SpiderOak
for servicing their consumer targeted cloud storage product.  It's a fresh
design based on everything we've learned since 2007 about large scale storage
management.

Parity architecture allows Nimbus.io to focuses on throughput, reliability, and
cost effectiveness over read latency.

