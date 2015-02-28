/*
webmonitor

 - Web monitor uses a configuration file to know which web services to check.
 - It starts one goroutine per service to monitor.
 - It updates Redis with the status of each check.
 - It does not run in the context of a storage node, but rather in the context
   of an infrastructure node
   (i.e. it runs from /etc/service/nimbus.io.web_monitor,
   not /etc.01/service/nimbus.io.web_monitor.)
   This is the same as the current web_director.
   (/etc/service/nimbus.io.web_director)

 - implements the timeouts via ?

 - Stores reachability results in a redis hash structure.
   The name of the hash is the "nimbus.io.web_monitor.$HOSTNAME".
   (i.e. one hash per monitor.)
   The keys of the hash are the HOST:PORT of each service we are monitoring.
   The values are the result of the check.
   (Can be a JSON string if more detail is needed.)

   http://redis.io/topics/data-types#hashes

 - Rather than try to mess with a Redis connection pool, just spawns a single
   goroutine to talk to Redis.  That goroutine connects to Redis, then blocks on
   popping from a channel of updates.  Every other goroutine just pushes check
   result updates onto that channel.  No fancy connection pool needed.
*/
package main
