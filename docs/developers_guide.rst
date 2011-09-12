Developer's Guide
=======================================================

Contents:

.. toctree::
   :maxdepth: 2

Overview
^^^^^^^^
This API is intended to offer the best possible value for bulk, long term 
archival of data. We don't attempt to deliver a low-latency solution, but 
rather focus on reliability and bulk-transfer throughput. It is not suitable, 
for example, for serving content on a high-performance web page, or streaming 
rich multimedia to many users.

Data is stored using an adaptation of the Reed-Solomon algorithm to distribute 
the data across many servers with minimal overhead. Using this storage method 
requires greater computational time for handling requests, and is one of the 
reasons that we focus on giving great cost-value rather than high performance.

The API itself is designed to be simple, adopting a RESTful interface to 
key-value storage. It is similar to Amazon's S3 API, and we plan on providing 
a drop-in module to help you migrate your application from S3 to nimbus.io.

Authentication
^^^^^^^^^^^^^^
Each request to the nimbus.io API must be authenticated using a key, 
assigned to you when you sign up for the service. You may request additional 
keys to be associated with your account if you wish.

Authentication is accomplished using the HTTP Authorization header with a 
special scheme name of NIMBUSIO. The format of this header is detailed below.

HTTP Authorization Header
#########################
The format of the HTTP Authorization header is:

Authorization: NIMBUS.IO <key_id>:<signature>

key_id
   An integer identifying the authentication key assigned to you by SpiderOak.


signature
   A hex string signature, generated for each request as described in Authentication Signature

Authentication Signature
########################
The authentication signature is a SHA256 HMAC hex string generated using a 
string representing the request and your authentication key. This string is 
made up of the following fields, separated by newline characters 
(ascii code 10):

username
   The username associated with the account. 


method
   The HTTP method being used for the request. At this time, 
   either GET or POST.


timestamp
   Integer number of seconds since the UNIX Epoch (1970-01-01 00:00:00 UTC).


uri
   the path part of the URI (minus the hostname) for example /data/my-key 

Important

The timestamp must agree within 10 minutes of that on the server, or the 
server will reject the authentication. Please synchronize your clock to an 
NTP server or otherwise make sure it is correct.

The following Python function will generate a valid signature for its inputs:

::

   import hmac, hashlib

   def make_signature(authenticaton_key, username, method, timestamp, uri_path):
       string_to_sign = '\n'.join((
           username,
           method,
           str(timestamp),
           uri_path
       ))
       hmac_value = hmac.new(authenticaton_key, string_to_sign, hashlib.sha256)
       return hmac_value.hexdigest()

X-NIMBUS-IO-Timestamp
#####################

To provide some protection against replay attacks, the authentication 
signature contains a timestamp field. In order for the server to verify the 
signature, a special header must be provided in the request with the same 
timestamp used to generate the signature. The header is X-NIMBUS-IO-Timestamp, 
and the value is the timestamp detailed in Authentication Signature. 

For example:

X-NIMBUS-IO-Timestamp: 1276808600

Important

The timestamp must agree within 10 minutes of that on the server, 
or the server will reject the authentication. Please synchronize your clock to 
an NTP server or otherwise make sure it is correct.

Authentication Example
######################
Using the following example credentials, we use the make_signature function 
from Authentication Signature to generate an HTTP Authorization header.

::

   Username:	alice
   Authentication Key ID:	5001
   Authentication Key:	DwWKayqqnWnmLouZQKfncsNj72x7TThMA3uO9Y/IBJg
   Timestamp:	1276808600
   >>> make_signature('alice', 'GET', 1276808600, '/list_collections')
   'e0942c34ee095825302dd6aede9ac7f7c8fc5985998f9eaacbe906b673855876'

Resulting HTTP Headers:

::

   Authorization: NIMBUS.IO 5001:e0942c34ee095825302dd6aede9ac7f7c8fc5985998f9eaacbe906b673855876
   X-NIMBUS.IO-Timestamp: 1276808600

API Usage
^^^^^^^^^
Storing data using the nimbus.io API follows the REST model. The 'resources' 
identified by URLs in the system represent your stored data. You act on that 
data by performing various actions against those URLs. 

Collections as Hostnames
########################

nimbus.io organizes the objects that you store into collections. Every 
nimbus.io action is associated with a collection. For efficient access to your 
collections, nimbus.io uses the collection name as part of the hostname_.

For example, to act on the collection 'my-temperature-readings', your HTTP 
query would be directed to hostname 'my-temperature-readings.nimbus.io'

This approach requires some restrictions on your collection names:

* collection names must be unique: you cannot use a colection name that 
  someone else is already using.
* Internet standards mandate that collection names may contain only 
  the ASCII letters 'a' through 'z' (in a case-insensitive manner), 
  the digits '0' through '9', and the hyphen ('-').
* collection names must be between 1 and 63 characters long

nimbus.io gives you a default collection name of 'dd-<your user name>'

* you don't need to create your default collection
* you cannot delete your default collection

This paces the same restrictions on user name that apply to collection name, 
except that user name must be between 1 and 60 characters long.

To reduce the inconvenience of creating a unique collection name, nimus.io 
provides a facility for creating guaranteed unique names of the form
'rr-<your user-name>-<collection name>'. Of course, this must comply with the
restrictons mentioned above.

.. _hostname: http://en.wikipedia.org/wiki/Hostname

Keys
####

Each object within a collection is uniquely identified by a key. The key must 
be between 1 and 1024 characters long. When used in an HTTP request, the key
must meet the standard HTTP restrictions. However, nimbus.io will recognize the
standard  %xx escapes, so effectively the key can be any unicode value that 
can be encoded to utf-8.

nimbus.io does not impose, or recognize, any structure or hierarchy among the 
keys in a collection. 

You can organize your data hierarchically by using a delimiter character in 
your keys. The API doesn't place any restrictions on what you use as a 
delimiter, but may people use the slash character ("/") for as that is the 
standard delimiter for URLs. For example:

* https://dd-alice.nimbus.io/data/california/trafficjam.jpg
* https://dd-alice.nimbus.io/data/maui/sunset.jpg
* https://dd-alice.nimbus.io/data/maui/beach.jpg

You can list the keys you have stored in a collection by performing a 
listmatch query (See Listing Data for details). The nimbus.io API uses the 
JSON format for transferring information about your data, but the data itself 
is transferred as it is stored.

Managing Collections
####################

Creating a Collection
+++++++++++++++++++++
.. http:get:: /create_collection

Create a new collection. 

If you try to create a collection with the same name as an existng collection,
This query will NOT fail. It will reuse the existing collection. If the 
existing collection has been deleted, it will be un-deleted.

Note that the hostname must contain a valid existing
collection. You can use your default collection name for this.

:query collection-name: The name of the new collection. 
:statuscode 200: no error
:statuscode 500: internal error
 
List Collections
++++++++++++++++
.. http:get:: /list_collections

List all collection names for this user. The reply body will contain a JSON 
list of pairs. Each pair will be collection name and date created.

Note that the hostname must contain a valid existing
collection. You can use your default collection name for this.

:statuscode 200: no error
:statuscode 500: internal error
 
Deleting a Collection
+++++++++++++++++++++
.. http:get:: /delete_collection

Delete an existing collection. 

Note that the hostname must contain a valid existing
collection. You can use your default collection name for this.

:query collection-name: The name of the collection to be deleted. 
:statuscode 200: no error
:statuscode 403: forbidden to delete the default collection, or a collection contaning data.
:statuscode 404: unknown collection
:statuscode 500: internal error

Getting Space Usage Information
+++++++++++++++++++++++++++++++
To get information on space usage by a colection 

.. http:get /usage

Get usage information on the collection specified in the hostname 

:statuscode 200: no error
:statuscode 500: internal error

Managing Keys
#############
 
Uploading to a Key
++++++++++++++++++
To upload data to a collection in your account using the nimbus.io API, issue 
a POST request containing the key you wish the data to be stored at, with the 
data to be storeed as the body of the request.

The data will be uploaded to the collection specified in the HTTP hostname.

.. http:post:: /data/<key>

Data to be uploaded comprises the body of the request.

:query __nimbus_io__<meta-key>=<meta-value>: metadata associated with the key 
:statuscode 200: no error
:statuscode 403: invalid data (probably zero size)
:statuscode 404: unknown collection
:statuscode 500: internal error

Listing Keys
++++++++++++
To list the keys in a collection, issue a listmatch request. 

.. http:get:: /data/<prefix>

<prefix> limits the query to keys starting with <prefix>
The API does not take any delimiters into account. This means that if you have 
data stored at "/maui/beach.jpg" and "/maui-documents/rental-car-invoice.pdf", 
then both will be returned if you do a listmatch on "maui". You can avoid this 
by making sure the trailing delimiter ("/") is included in the URL

:query action=listmatch: listmatch request

:statuscode 200: no error
:statuscode 500: internal error

Downloading From a Key
++++++++++++++++++++++
Downloading an object from a collection is just a simple GET request. 
The server will return exactly what you uploaded to that URL:

.. http:get:: /data/key

:statuscode 200: no error
:statuscode 500: internal error

Deleting a key
++++++++++++++
To delete a resource, 
* issue a DELETE request
* issue a POST request for delete

.. http:delete:: /data/key
.. http:post:: /data/key

:query action=delete: delete action for post request

:statuscode 200: no error
:statuscode 500: internal error

Getting Information about a key
+++++++++++++++++++++++++++++++
To some information about a key issue a 'stat' request.

* file_size
* adler32 sum of file contents
* md5 digest of file contents

.. http:get:: /data/key

:query action=stat: request stat
:statuscode 200: no error
:statuscode 404: not found
:statuscode 500: internal error

