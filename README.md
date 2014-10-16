Name
======
ngx_http_kafka_module

This is a nginx module for push message to kafka
*This module is not distributed with the Nginx source.* See [the installation instructions](#installation).


Table of Contents
=================

* [Status](#status)
* [Version](#version)
* [Synopsis](#synopsis)
* [Description](#description)
* [Directives](#directives)
	* [kfk.metadata.broker.list](#kfkmetadatabrokerlist)
	* [kfk.bootstrap.servers](#kfkbootstrapservers)
	* [kfk.request.required.acks](#kfkrequestrequiredacks)
	* [kfk.acks](#kfkacks)
	* [kfk.request.timeout](#kfkrequesttimeout)
	* [kfk.timeout](#kfktimeout)
	* [kfk.message.max.size](#kfkmessagemaxsize)
	* [kfk.compression.codec](#kfkcompressioncodec)
	* [kfk.compressed.topics](#kfkcompressedtopics)
	* [kfk.message.send.max.retries](#kfkmessagesendmaxretries)
	* [kfk.retries](#kfkretries)
	* [kfk.retry.backoff](#kfkretrybackoff)
	* [kfk.topic.metadata.refresh.interval](#kfktopicmetadatarefreshinterval)
	* [kfk.metadata.max.age](#kfkmetadatamaxage)
	* [kfk.linger](#kfklinger)
	* [kfk.queue.buffering.max.messages](#kfkqueuebufferingmaxmessages)
	* [kfk.block.on.buffer.full](#kfkblockonbufferfull)
	* [kfk.backpath](#kfkbackpath)
	* [kfk.batch.num.messages](#kfkbatchnummessages)
	* [kfk.send.buffer.bytes](#kfksendbufferbytes)
	* [kfk.client.id](#kfkclientid)
	* [kfk.reconnect.backoff](#kfkreconnectbackoff)
	* [kfk.topics](#kfktopics)
	* [kfk.log](#kfklog)
	* [kfk.rsp.max.size](#kfkrspmaxsize)
	* [kfk.buffers](#kfkbuffers)
	* [kfk.status](#kfkstatus)
	* [kafka_stub_status](#kafka_stub_status)
	* [kafka_read_body](#kafka_read_body)
	* [kafka](#kafka)
* [API for ngx_lua](#api-for-ngx_lua)
	* [kfk.log](#api-for-ngx_lua)
* [Embedded Variables](#embedded-variables)
	* [$pending_msg](#$pending_msg)
	* [$succ_msg](#$succ_msg)
	* [$fail_msg](#$fail_msg)
	* [$wait_buf](#$wait_buf)
	* [$out_buf](#$out_buf)
	* [$free_buf](#$free_buf)
	* [$free_chain](#$free_chain)
* [Installation](#installation)
* [TODO](#todo)


Status
======

This module is under early development and simple test

Version
=======

[v0.0.1] released on 16 Oct 2014.


Synopsis
========

```
http {
    
#    kfk.metadata.broker.list 10.210.228.89:9996 10.210.228.90:9997;
    kfk.bootstrap.servers 10.210.228.89:9996 10.210.228.90:9997;
    kfk.client.id ngx_kfk;
#    kfk.request.required.acks 1; 
    kfk.acks 1;
#    kfk.message.send.max.retries 0;
    kfk.retries 0;

    kfk.queue.buffering.max.messages 10k;
    kfk.batch.num.messages 500;
    kfk.linger 1s;
#    kfk.request.timeout 5s;
#    kfk.timeout ts;
#    kfk.topic.metadata.refresh.interval 120s;
    kfk.metadata.max.age 2m;
    kfk.reconnect.backoff 60s;
   
    kfk.topics test;
    kfk.message.max.size 1k;
    kfk.buffers 100 4k;
    kfk.rsp.max.size     8k;
    kfk.status on;
#    kfk.log logs/kfk.log debug;

    resolver 123.125.105.252;
    resolver_timeout 1s;

    access_log on;
    server {
        listen 8100;
        # curl --data 'test message' localhost:8100/test
        location /test {
            kafka_read_body on;
            set $kafka_topic test;
            set $kafka_key '';
            kafka $request_body;
        }
    }

    server {
        listen 8200;
        # status monitor
        location /stub {
            kafka_stub_status on;
        }

        # ngx_lua api
        location /lua {
            content_by_lua '
                local kfk = require "kfk"
                local succ, err = kfk.log("test", nil, "test message")
                if not succ then
                    ngx.say(err);
                end
            ';
        }

		# lua version's status monitor(Embedded Variables)
        location /lua_stub {
            content_by_lua '
                ngx.say("Pending messages: ", ngx.var.pending_msg);
                ngx.say("Succ messages: ", ngx.var.succ_msg);
                ngx.say("Fail messages: ", ngx.var.fail_msg);
                ngx.say("Wait bufs: ", ngx.var.wait_buf);
                ngx.say("Out bufs: ", ngx.var.out_buf);
                ngx.say("Free bufs: ", ngx.var.free_buf);
                ngx.say("Free chains: ", ngx.var.free_chain);
            ';
        }
    }
}

```
[Back to top](#table-of-contents)


Description
===========


Directives
==========


kfk.metadata.broker.list
--------------------
**syntax:** *kfk.metadata.broker.list host:port ...*

**default:** *-*

**context:** *http*

**note:** must config

A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. Data will be load balanced over all servers irrespective of which servers are specified here for bootstrapping—this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form host1:port1 host2:port2 .... Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers (you may want more than one, though, in case a server is down). If no server in this list is available sending data will fail until on becomes available.

[Back to top](#table-of-contents)


kfk.bootstrap.servers
--------------------
**syntax:** *kfk.bootstrap.servers host:port ...*

**default:** *-*

**context:** *http*

Same with [kfk.metadata.broker.list](#kfk.metadata.broker.list)

[Back to top](#table-of-contents)


kfk.request.required.acks
--------------------
**syntax:** *kfk.request.required.acks num*

**default:** *kfk.request.required.acks 1*

**context:** *http*


The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the durability of records that are sent. The following settings are common:

* acks=0 If set to zero then the producer will not wait for any acknowledgment from the server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be made that the server has received the record in this case, and the retries configuration will not take effect (as the client won't generally know of any failures). The offset given back for each record will always be set to -1. 

* acks=1 This will mean the leader will write the record to its local log but will respond without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after acknowledging the record but before the followers have replicated it then the record will be lost.

* acks>=2 are also possible, and will require the given number of acknowledgements but this is generally less useful.

* acks=-1 (Not support yet) This means the leader will wait for the full set of in-sync replicas to acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica remains alive. This is the strongest available guarantee.

[Back to top](#table-of-contents)


kfk.acks
--------------------
**syntax:** *kfk.acks num*

**default:** *kfk.acks 1*

**context:** *http*

Same with [kfk.request.required.acks](#kfk.request.required.acks)

[Back to top](#table-of-contents)


kfk.request.timeout
--------------------
**syntax:** *kfk.request.timeout time*

**default:** *kfk.request.timeout 5s*

**context:** *http*

The configuration controls the maximum amount of time the server will wait for acknowledgments from followers to meet the acknowledgment requirements the producer has specified with the acks configuration. If the requested number of acknowledgments are not met when the timeout elapses an error will be returned. This timeout is measured on the server side and does not include the network latency of the request.

[Back to top](#table-of-contents)


kfk.timeout
--------------------
**syntax:** *kfk.timeout time*

**default:** *kfk.timeout 5s*

**context:** *http*

Same with [kfk.request.timeout](#kfk.request.timeout)


[Back to top](#table-of-contents)


kfk.message.timeout
--------------------
**syntax:** *kfk.message.timeout time*

**default:** *kfk.message.timeout 10s*

**context:** *http*

**note:**  *[TODO](#todo)*

When a message come, the configuration means a message must sent to kafka cluster in setting time, or it will be treated as a failure

[Back to top](#table-of-contents)


kfk.message.max.size
--------------------
**syntax:** *kfk.message.max.size size*

**default:** *kfk.message.max.size 4k*

**context:** *http*

Config max message body（not include request headers) size. when a message exceed the size, we will throw a error.

[Back to top](#table-of-contents)


kfk.compression.codec
--------------------
**syntax:** *kfk.compression.codec none|gzip|snappy*

**default:** *kfk.compression.codec none*

**context:** *http*

**note:** *[TODO](#todo)*

The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid values are none, gzip, or snappy. Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression).


[Back to top](#table-of-contents)


kfk.compression.type
--------------------
**syntax:** *kfk.compression.codec none|gzip|snappy*

**default:** *kfk.compression.codec none*

**context:** *http*

**note:** *[TODO](#todo)*

Same with [kfk.compression.codec](#kfk.compression.codec)

[Back to top](#table-of-contents)


kfk.compressed.topics
--------------------
**syntax:** *kfk.compressed.topics name ...*

**default:** *-*

**context:** *http*

**note:** *[TODO](#todo)*

This parameter allows you to set whether compression should be turned on for particular topics. If the [kfk.compression.codec](#kfk.compression.codec) is anything other than none , enable compression only for specified topics if any. If the list of compressed topics is empty, then enable the specified compression codec for all topics. If the compression codec is none, compression is disabled for all topics


[Back to top](#table-of-contents)


kfk.message.send.max.retries
--------------------
**syntax:** *kfk.message.send.max.retries num*

**default:** *kfk.message.send.max.retries 1*

**context:** *http*

**note:** *[TODO](#todo)*

This property will cause the producer to automatically retry a failed send request. This property specifies the number of retries when such failures occur. Note that setting a non-zero value here can lead to duplicates in the case of network errors that cause a message to be sent but the acknowledgement to be lost.


[Back to top](#table-of-contents)


kfk.retries
--------------------
**syntax:** *kfk.retries num*

**default:** *kfk.retries 1*

**context:** *http*

**note:** *[TODO](#todo)*

Same with [kfk.message.send.max.retries][#kfk.message.send.max.retries]


[Back to top](#table-of-contents)


kfk.retry.backoff
--------------------
**syntax:** *kfk.retry.backoff time*

**default:** *kfk.retry.backoff 100ms*

**context:** *http*

**note:** *[TODO](#todo)*

Before each retry, the producer refreshes the metadata of relevant topics to see if a new leader has been elected. Since leader election takes a bit of time, this property specifies the amount of time that the producer waits before refreshing the metadata.

[Back to top](#table-of-contents)


kfk.topic.metadata.refresh.interval
--------------------
**syntax:** *kfk.topic.metadata.refresh.interval time*

**default:** *kfk.topic.metadata.refresh.interval 2m*

**context:** *http*

The producer generally refreshes the topic metadata from brokers when there is a failure (partition missing, leader not available...). It will also poll regularly (default: every 120s).

[Back to top](#table-of-contents)


kfk.metadata.max.age
--------------------
**syntax:** *kfk.metadata.max.age time*

**default:** *kfk.metadata.max.age 2m*

**context:** *http*

Same with [kfk.topic.metadata.refresh.interval](#kfk.topic.metadata.refresh.interval)

[Back to top](#table-of-contents)


kfk.linger
--------------------
**syntax:** *kfk.linger time*

**default:** *kfk.linger 1s*

**context:** *http*

Set maximum time to buffer data. For example a setting of 100 will try to batch together 100ms of messages to send at once. This will improve throughput but adds message delivery latency due to the buffering

[Back to top](#table-of-contents)


kfk.queue.buffering.max.messages
--------------------
**syntax:** *kfk.queue.buffering.max.messages size*

**default:** *kfk.queue.buffering.max.messages 10k*

**context:** *http*

Set the maximum number of unsent messages that can be queued up in producer, note that this configuation is per nginx worker semantic. when queued message exceed the size, later coming message request will throw a error.

[Back to top](#table-of-contents)


kfk.block.on.buffer.full
--------------------
**syntax:** *kfk.block.on.buffer.full block|discard|backup*

**default:** *kfk.block.on.buffer.full discard*

**context:** *http*

**note:** *[TODO](#todo)*

When our memory buffer is exhausted we must either stop accepting new records (block) or discard. By default this setting is discard and we discard messages. when set to block, the producer will block util there is some free buf.

[Back to top](#table-of-contents)


kfk.backpath
--------------------
**syntax:** *kfk.backpath path  path [level1 [level2 [level3]]];*

**default:** *kfk.backpath back*

**context:** *http*

**note:** *[TODO](#todo)*

When message send failure, we may bakcup failure message to local file system. This directive set path for backup. the useage of option is same with [proxy_temp_path](http://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_temp_path) of ngx_http_proxy_module

[Back to top](#table-of-contents)


kfk.batch.num.messages
--------------------
**syntax:** *kfk.batch.num.messages size*

**default:** *kfk.batch.num.message 500*

**context:** *http*

The number of messages to send in one batch. The producer will wait until either this number of messages are ready to send or kfk.linger is reached.


[Back to top](#table-of-contents)


kfk.send.buffer.bytes
--------------------
**syntax:** *kfk.send.buffer.bytes size*

**default:** *kfk.send.buffer.bytes 100k*

**context:** *http*

**note:** *[TODO](#todo)*

Socket write buffer size

[Back to top](#table-of-contents)


kfk.client.id
--------------------
**syntax:** *kfk.client.id str*

**default:** *-*

**context:** *http*

**note:** *client id string len at most 18*

The client id is a user-specified string sent in each request to help trace calls. It should logically identify the application making the request. default to null

[Back to top](#table-of-contents)


kfk.reconnect.backoff
--------------------
**syntax:** *kfk.reconnect.backoff time*

**default:** *kfk.reconnect.backoff 60s*

**context:** *http*

When broker down, we will try to reconnect broker in setting interval

[Back to top](#table-of-contents)


kfk.topics
--------------------
**syntax:** *kfk.topics str ...*

**default:** *-*

**context:** *http*

**note:** *must config*

This directive set topics we will produce to broker, if a message's topic is not in the set, we ignore it.

[Back to top](#table-of-contents)


kfk.log
--------------------
**syntax:** *kfk.log file | stderr | syslog:server=address[,parameter=value] [debug | info | notice | warn | error | crit | alert | emerg];*

**default:** *kfk.log logs/kfk.log warn*

**context:** *http*

Set kafka producer error log, the usage is same with [error_log](http://nginx.org/en/docs/ngx_core_module.html#error_log) directive

[Back to top](#table-of-contents)


kfk.rsp.max.size
--------------------
**syntax:** *kfk.rsp.max.size size*

**default:** *kfk.rsp.max.size 4k*

**context:** *http*

Set max response size(include metadata response and producer response), when a response exceed this size, we will close the connection and reconnect

[Back to top](#table-of-contents)


kfk.buffers
--------------------
**syntax:** *kfk.buffers num size*

**default:** *kfk.buffers 100 4k*

**context:** *http*

Sets the number and size of the buffers used for producer, a buffer is a message set. when a message come and there is no free buf, we will throw a error

[Back to top](#table-of-contents)


kfk.status
--------------------
**syntax:** *kfk.status on|off*

**default:** *kfk.status off*

**context:** *http*

This directive Enable/Disable kafka status statistics. now, we will record pending messages, success messages, fail message, wait buffers, out buffers, free buffers, free chains. this directive is useful for monitoring nginx-kafka running status. note that this directive is valid when we Enable NGX_KFK_STATUS option in compile source, see [installation](#installation)

[Back to top](#table-of-contents)


kafka_stub_status
--------------------
**syntax:** *kfk_stub_status on|off*

**default:** *kfk_stub_status off*

**context:** *server, location*

This directive is similar whih [stub_status](http://nginx.org/en/docs/http/ngx_http_stub_status_module.html#stub_status) of ngx_http_stub_status_module, it will print kafka status statistics data. note that this directive is valid when we Enable NGX_KFK_STATUS option in compile source, see [installation](#installation)

[Back to top](#table-of-contents)


kafka_read_body
--------------------
**syntax:** *kafka_read_body on|off*

**default:** *kafka_read_body off*

**context:** *http, server, location*

Set to on, it we force read request body, when [kafka](#kafka) directive set message body to request body, you should enable it

[Back to top](#table-of-contents)


kafka
--------------------
**syntax:** *kafka value*

**default:** *-*

**context:** *location*

Enable kafka handler and set message body, for example, *kafka $request_body*, when [kafka_read_body](#kafka_rad_body) enabled, it will read request body and send to broker

[Back to top](#table-of-contents)

API for ngx_lua
===============

kfk.log 
-------
**syntax:** kfk.log(topic, key, ...)

**context:** *init_by_lua*, init_worker_by_lua*, set_by_lua*, rewrite_by_lua*, access_by_lua*, content_by_lua*, header_filter_by_lua*, body_filter_by_lua*, log_by_lua*, ngx.timer.**


the topic parameter must be a string and length must be greater than 0.

the key parameter can be nil or string, key control which partition a message will be assign. when it is nil, a random partition will be assgin. the partition algorithm is: *crc32(key)%n*, where n is topic's total partition.

message body parameter can be string, number, boolean, nil, or userdata. Lua nil arguments result in literal "nil" string.
Lua booleans result in literal "true" or "false".
And the userdata will yield the "null" string output.

[Back to top](#table-of-contents)


Embedded Variables
==================

The ngx_http_kafka_module module supports the following embedded variables

$pending_msg
-----------

unsend messages

$succ_msg
---------

success send messages


$fail_msg
---------

fail send messages


$wait_buf
---------

wait buffers count, wait buffer means messages have sent to socket buffer but not receive response


$out_buf
--------

out buffers count, out buffer means messages are into partition buf not yet send to socket buffer


$free_buf
---------

free buffers count, idle buffers


$free_chain
-----------

free chains count, idle chain link


Installation
============

This module is not distributed with the Nginx source, configure with --add-module=module_path

ngx_http_kafka_module module support the following option:

* NGX_KFK_STATUS this option Enable/Disable kafka status statistics
* NGX_KFK_LUA this option Enable/Disable kafka ngx_lua api
* NGX_KFK_DEBUG this option Enable/Disable debug log

you can config it by edit the config file

[Back to top](#table-of-contents)


TODO
====

next step, i will code for support following directives:

* [kfk.message.timeout](#kfk.message.timeout)
* [kfk.compression.codec](#kfk.compression.codec)
* [kfk.compression.type](#kfk.compression.type)
* [kfk.compressed.topics](#kfk.compressed.topics)
* [kfk.message.send.max.retries](#kfk.message.send.max.retries)
* [kfk.retries](#kfk.retries)
* [kfk.retry.backoff](#kfk.retry.backoff)
* [kfk.block.on.buffer.full](#kfk.block.on.buffer.full)
* [kfk.backpath](#kfk.backpath)
* [kfk.send.buffer.bytes](#kfk.send.buffer.bytes)


[Back to top](#table-of-contents)
