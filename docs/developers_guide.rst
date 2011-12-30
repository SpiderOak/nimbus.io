Developer's Guide
=======================================================

Contents:

.. toctree::
   :maxdepth: 10

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
The basic idea of REST is that you have "resources" (URLs) that represent 
ideas or concepts in your problem domain, and then you use HTTP verbs to 
perform actions on the resources.

nimbus.io resources for a customer:

* The customer's account
* The collections owned by the customer
* The keys within a collection
* The data (and meta data) for a key

These are represented as:

::

   https://nimbus.io/customers/<username>
   https://nimbus.io/customers/<username>/collections
   https://<collection name>.nimbus.io/data/
   https://<collection name>.nimbus.io/data/<key>

Customer's Account
##################

List Collections
++++++++++++++++
.. http:get:: /customer/<username>/collections

    List all collection names for this customer. The reply body will contain a JSON 
    list of pairs. Each pair will be collection name and date created.

    :statuscode 200: no error

Collections
###########

Creating a Collection
+++++++++++++++++++++
.. http:post:: /customer/<username>/collections

    Create a new collection. 

    If you try to create a collection with the same name as an existng collection,
    This query will NOT fail. It will reuse the existing collection. If the 
    existing collection has been deleted, it will be un-deleted.

    :query action: create 
    :query name: the name of the new collection 
    :statuscode 200: no error
 
Deleting a Collection
+++++++++++++++++++++
.. http:delete:: /customer/<username>/collections/<collection-name>
.. http:post:: /customer/<username>/collections/<collection-name>

    Delete an existing collection. [1]_ 

    :query action: delete (POST only) 
    :statuscode 200: no error
    :statuscode 403: forbidden to delete the default collection, or a collection contaning data.
    :statuscode 404: unknown collection

Getting Space Usage Information
+++++++++++++++++++++++++++++++
To get information on space usage by a collection 

.. http:get:: /customer/username/collections/<collection-name>

    Get usage information on the collection specified in the hostname 

    :query action: space_usage
    :statuscode 200: no error

Keys
####

Collections as Hostnames
++++++++++++++++++++++++

nimbus.io organizes the objects that you store into collections. Every 
nimbus.io key is a member of a collection. For efficient access to your 
data nimbus.io uses the collection name as part of the hostname_.

For example, to act on objects in the collection 'my-temperature-readings', 
your HTTP query would be directed to hostname 'my-temperature-readings.nimbus.io'

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

Object Keys
+++++++++++

Each object within a collection is uniquely identified by a key. The key must 
be between 1 and 1024 characters long. When used in an HTTP request, the key
must meet the standard HTTP restrictions. 

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
listmatch query (See listing_keys_ for details). The nimbus.io API uses the 
JSON format for transferring information about your data, but the data itself 
is transferred as it is stored.

Uploading to a Key
++++++++++++++++++
To upload data to a collection in your account using the nimbus.io API, issue 
a POST request containing the key you wish the data to be stored at, with the 
data to be storeed as the body of the request.

The data will be uploaded to the collection specified in the HTTP hostname.

.. http:post:: /data/<key>

    Data to be uploaded comprises the body of the request.

    :query conjoined_identifier=<conjoined-identifier>: 
        * the value returned by conjoined start, 
        * for a non-conjoined key, set to "" or don't send at all
    :query conjoined_part=<sequence-number>: 
        * for conjoined archives, sequence number of this upload, starting at 1
        * for a non-conjoined key, set to zero or don't send at all
    :query __nimbus_io__<meta-key>=<meta-value>: metadata associated with the key 
    :statuscode 200: no error
    :statuscode 403: invalid data (probably zero size)
    :statuscode 404: unknown collection

.. _listing_keys:

Listing Keys
++++++++++++
To list the keys in a collection, issue a listmatch request. 

.. http:get:: /data/

    :query max_keys: The maximum number of keys to retrieve
    :query prefix: The prefix of the keys you want to retrieve
    :query marker: where you are in the result set
    :query delimiter: Keys that contain the same string between the prefix and the 
                first occurrence of the delimiter will be rolled up into a single 
                result element. 

    :statuscode 200: no error

Downloading From a Key
++++++++++++++++++++++
Downloading an object from a collection is just a simple GET request. 
The server will return exactly what you uploaded to that URL:

.. http:get:: /data/<key>

    :statuscode 200: no error

Deleting a key
++++++++++++++
To delete a resource, 
* issue a DELETE request
* issue a POST request for delete [1]_

.. http:delete:: /data/<key>


.. http:post:: /data/<key>

    :query action=delete: delete action for post request
    :statuscode 200: no error

Getting File Information About a Key
++++++++++++++++++++++++++++++++++++
To retrieve file information about a key issue a HEAD request.

* file_size
* md5 digest of file contents

.. http:head:: /data/<key>

    :statuscode 200: no error
    :statuscode 404: not found

Getting Meta Information About a Key
++++++++++++++++++++++++++++++++++++
To retrieve the meta data stored with a key issue a 'meta' request.

body will be a JSON list of key/value pairs

.. http:get:: /data/<key>/

    :query action=meta: request meta
    :statuscode 200: no error
    :statuscode 404: not found

Conjoined Archives:
###################
A conjoined archive enables multiple parts of a file to be uploaded in 
parallel. And/or for individual uploads to be restarted without restarting
the entire archive.

Listing Conjoined Archives
++++++++++++++++++++++++++
List the conjoined archives active for this collection 

Server will return a JSON dictionary containing:

 * conjoined_list (list of dictionaries)
 * truncated (boolean)

where the conjoined_list entries contain:

 * conjoined_identifier 
 * key
 * create_timestamp 
 * abort_timestamp
 * complete_timestamp 
 * delete_timestamp

.. http:get:: /conjoined/

    :query max_conjoined: 
        The maximum number of conjoined archives to retrieve. 
        Default value is 1000.

    :query key_marker:
        list keys greater than this key

    :query conjoined_identifier_marker:
        If key_marker is specified, only list keys with conjoined_identifier
        greater than this identifier.
    :statuscode 200: no error

Start a Conjoined Archive
+++++++++++++++++++++++++
Start a conjoined archive, where multiple uploads can combine to form the key
The body of the return contains a JSON conjoined_identified (UUID)

.. http:post:: /conjoined/<key>

    :query action=start: start a conjoined archive
    :statuscode 200: no error

Upload to a Conjoined Archive
+++++++++++++++++++++++++++++
Upload one part of the full key. Multiple uploads can be done in parallel.
This is a normal key upload with additional varaibles.

.. http:post:: /data/<key>

    :query conjoined_identifier=<conjoined-identifier>: returned by start
    :query conjoined_part=<sequence-number>: number of this upload
    :statuscode 200: no error

Finish Conjoined Archive
++++++++++++++++++++++++
Mark the archive as completed. 

.. http:post:: /conjoined/<key>

    :query action=finish: finish a conjoined archive
    :query conjoined_identifier=<conjoined-identifier>: returned by start
    :statuscode 200: no error

Abort Conjoined Archive
++++++++++++++++++++++++
Halt the conjoined archive and release all resources.

.. http:delete:: /conjoined/<key>

    :query action=abort: abort a conjoined archive
    :query conjoined_identifier=<conjoined-identifier>: returned by start
    :statuscode 200: no error

List Uploads of Conjoined Archive
+++++++++++++++++++++++++++++++++
List the known uploads in sequence

.. http:get:: /conjoined/<key>/<conjoined-identifier>/

    :statuscode 200: no error

.. [1] In an ideal world, we would just need DELETE for the this. But due to limited browser support for the DELETE verb, we also provide an alternative via POST with action=delete. 


