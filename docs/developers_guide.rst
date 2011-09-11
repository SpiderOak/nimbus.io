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

Authorization: NIMBUSIO <key_id>:<signature>
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

import hmac, hashlib

def make_signature(timestamp, key, username, method):
    string_to_sign = '\n'.join((
        username,
        method,
        str(timestamp),
        uri_path
    ))
    return hmac.new(key, string_to_sign, hashlib.sha256).hexdigest()

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
Using the following example credentials, we use the make_signature function from Authentication Signature to generate an HTTP Authorization header.

Username:	alice
Key ID:	5001
Key:	deadbeef
Timestamp:	1276808600
>>> make_signature(1276808600, 'deadbeef', 'alice', 'GET')
'9c8b5985c0c0c3f6771aa0581ec55542d2711edb52269c65761bcd82e7d9980b'
Resulting HTTP Headers:

Authorization: DIYAPI 5001:9c8b5985c0c0c3f6771aa0581ec55542d2711edb52269c65761bcd82e7d9980b
X-DIYAPI-Timestamp: 1276808600

API Usage
^^^^^^^^^
Storing data using the SpiderOak DIY API follows the REST model. The 'resources' identified by URLs in the system represent your stored data. You act on that data by performing various actions against those URLs. The API is rooted at a URL like the following:

https://alice.diy.spideroak.com/data/
Anything appearing after the /data/ part of the URL is a pathname identifying a piece of data you have stored using the API. For example, you may upload a JPEG image file to https://alice.diy.spideroak.com/data/picture1.jpg, where "picture1.jpg" would be the pathname.

You can organize your data hierarchically by using a delimiter character in your URLs. The API doesn't place any restrictions on what you use as a delimiter, but we suggest you use the slash character ("/") for new applications, as that is the standard delimiter for URLs. For example:

https://alice.diy.spideroak.com/data/california/trafficjam.jpg
https://alice.diy.spideroak.com/data/maui/sunset.jpg
https://alice.diy.spideroak.com/data/maui/beach.jpg
You can list the pathnames you have stored using the API by performing a listmatch query (See Listing Data for details). The DIY API uses the JSON format for transferring information about your data, but the data itself is transferred as it is stored.

Uploading Data
##############
To upload data to your account using the DIY API, issue a POST request to the URL you wish the data to be stored at, with the data to store as the body of the request. You must authenticate the request as detailed in Authentication. Example:

POST /data/hello-world HTTP/1.1
Host: alice.diy.spideroak.com
Authorization: DIYAPI 5001:b3d4773a78064db189bf955493c274c78833b04ea8281658f89f1a8b7fdcd475
X-DIYAPI-Timestamp: 1276808600

Hello, world!
A 200 OK return status indicates successful storage of your data.

Listing Data
############
To list the pathnames under which you have stored data, issue a listmatch request. This is a GET request with the query string "?action=listmatch" appended to the URL. You can limit the list by prefix. As in the example hierarchy above, in API Usage, you can list the files under "maui" by issuing the following request:

GET /data/maui/?action=listmatch HTTP/1.1
Host: alice.diy.spideroak.com
Authorization: DIYAPI 5001:9c8b5985c0c0c3f6771aa0581ec55542d2711edb52269c65761bcd82e7d9980b
X-DIYAPI-Timestamp: 1276808600
Which will return something like the following:

HTTP/1.1 200 OK
Server: diyapi/1.0
Date: Thu, 17 Jun 2010 22:28:22 GMT
Content-Length: 37

['maui/sunset.jpg', 'maui/beach.jpg']
Note

Currently, the API does not take any delimiters into account. This means that if you have data stored at "/maui/beach.jpg" and "/maui-documents/rental-car-invoice.pdf", then both will be returned if you do a listmatch on "maui". You can work around this by making sure the trailing slash ("/") is included in the URL you listmatch against, as in the above example.

Future versions of the API will honor delimiters.

Downloading Data
################
Downloading a resource is just a simple GET request. The server will return exactly what you uploaded to that URL:

GET /data/maui/beach.jpg HTTP/1.1
Host: alice.diy.spideroak.com
Authorization: DIYAPI 5001:9c8b5985c0c0c3f6771aa0581ec55542d2711edb52269c65761bcd82e7d9980b
X-DIYAPI-Timestamp: 1276808600

Deleting Data
#############
To delete a resource, issue a POST request with the query string "?action=delete" appended to the URL:

POST /data/hello-world?action=delete HTTP/1.1
Host: alice.diy.spideroak.com
Authorization: DIYAPI 5001:b3d4773a78064db189bf955493c274c78833b04ea8281658f89f1a8b7fdcd475
X-DIYAPI-Timestamp: 1276808600

Getting Metadata
################
To retrieve metadata for a resource, issue a GET request with the query string "?action=stat" appended to the URL:

GET /data/hello-world?action=stat HTTP/1.1
Host: alice.diy.spideroak.com
Authorization: DIYAPI 5001:b3d4773a78064db189bf955493c274c78833b04ea8281658f89f1a8b7fdcd475
X-DIYAPI-Timestamp: 1276808600
Which will return something like the following:

HTTP/1.1 200 OK
Server: diyapi/1.0
Date: Thu, 17 Jun 2010 22:28:22 GMT
Content-Length: 176

{"total_size": 3145728, "file_adler32": 948658718, "timestamp": 1263295969.7963581,
 "permissions": 0, "userid": 0, "groupid": 0, "file_md5": "2a9c556d64289c5c6758dc69781cf861"}

Getting Space Usage Information
###############################
To get information on space usage, issue a GET request to /usage:

https://alice.diy.spideroak.com/usage
This will return something like the following:

HTTP/1.1 200 OK
Server: diyapi/1.0
Date: Thu, 17 Jun 2010 22:28:22 GMT
Content-Length: 85

{"bytes_added": 640550929, "bytes_retrieved": 4582708985, "bytes_removed": 103452867}
You may compute the amount of storage currently in use by subtracting the bytes_removed value from the bytes_added value. (In the above example, resulting in 537098062 bytes.)

