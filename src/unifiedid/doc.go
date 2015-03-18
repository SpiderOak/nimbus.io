/*
package unifiedid supplies unique id's for nimbus.io objects

Based on the instagram sharded ids

http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram

41 bits for time in milliseconds
   (gives us 41 years of IDs with a custom epoch)
13 bits that represent the logical shard ID
10 bits that represent an auto-incrementing sequence, modulus 1024.
   This means we can generate 1024 IDs, per shard, per millisecond
*/
package unifiedid
