# mempress

In-memory objects compression.

## Purpose

The goal of this library is to *seamlessly* enable compressing objects in-memory, such 
that no code around them need be changed or made aware.<br>
The general approach is simple: trade memory for time / cpu-cycles.<br>
More specifically: a proxy wrapper is used to serialize an object, then lz4-compress 
the outcome. It goes without saying - access time is compromised, and the memory gain 
is solely dependent on the object data..<br>
Beyond the (rare?) cases where this approach might be beneficial (or even needed) this 
simple concept can be quite powerful, since in principal, the data could not only be 
serialized+compressed, but also sent to remote storage. The code written on-top could 
be interacting with vast amounts of objects, whose data isn't local at all, as if it 
was. This is currently out of scope, though, for this library, as it is.

## Usage

The very basic introduction:
```clojure
user=> (require '[mempress.core :as mempress])
nil
;given an object type
user=> (defrecord MyObject [data-field])
user.MyObject
;given a highly compress-able object instance
user=> (def my-obj (->MyObject (repeat 1000 "value")))
#'user/my-obj
;create a compacted version of my-obj
user=> (def compacted-obj (mempress/compact-now my-obj))
#'user/compacted-obj
;compacted-obj behaves exactly as my-obj does
;but takes up a fraction memory-wise
user=> (:compact-ratio (mempress/compaction-ratio compacted-obj))
0.012741546
```

Objects do not have to be records, this approach works for simple maps just the same.<br>

Non-mutating/read operations, well, they keep the object as is. Internally, though, such 
operations apply decompression/deserialization as/when needed. An internal limited LRU 
cache is used to make repeated operations more efficient.<br>

Mutating operations make a best effort to keep the portion of the object that was 
compressed as is, applying their effects ontop of it. Compression is not reapplied 
implicitly over and over again; but it may be reapplied explicitly by the user at 
any given moment.

It is also possible to fine-tune compaction via the compact-but-keep and compact-just 
alternatives. These variations on the basic compact-now api, allow for selective 
compression to be applied as needed per specific portions of the object.<br>

## License

Copyright © 2023 [@s-doti](https://github.com/s-doti)

This project is licensed under the terms of [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
