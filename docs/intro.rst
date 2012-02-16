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
storage within their applications at greatly reduced cost.

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

Parity architecture allows Nimbus.io to focuses on throughput, reliability, and
cost effectiveness instead of read latency.  This makes it most suitable for
archival class data.

Background
----------

Nimbus.io was created out of necessity by the storage engineers at `SpiderOak
<https://spideroak.com/>`_ for servicing their consumer oriented cloud storage
product at petabyte scale.  It's a fresh design based on everything we've
learned since 2007 about large scale storage management.  

History and Future
------------------

Events in the lifetime of Nimbus.io, past and future:

* Nov 2011: Announcement of Project, Posted on Hacker News & Reddit.
  Thousands of people signup for the announcement mailing list in 5 hours.  
  
  Hacker News discussion here: `Nimbus.io: Open-source alternative to Amazon S3
  <http://news.ycombinator.com/item?id=3209936>`_

* 15 Feb 2011: First development milestone reached: source code, documentation,
  `wiki <https://nimbus.io/dev/trac/wiki>`_, `road map
  <https://nimbus.io/dev/trac/wiki/DevelopmentRoadmap>`_, `mailing list
  <https://nimbus.io/dev/trac/wiki/NimbusIoMailingList>`_, made public.
  Developers invited to participate in the project.

* Q1 2012 - Commercial Nimbus.io storage service becomes available.

