Skipper  [![Build Status](https://travis-ci.org/zk1931/skipper.svg?branch=master)](https://travis-ci.org/zk1931/skipper)
=======

A distributed utility library built on top of [Jzab](https://github.com/zk1931/jzab)

For now we support consistent distributed hash map(```SkipperHashMap```) and distributed queue(```SkipperQueue```).

Usage
-----

Starts the first server :

```
  Skipper sk = new Skipper("localhost:5000", "localhost:5000", "/localhost5000");
```

Starts the second server joins the first one :

```
  Skipper sk = new Skipper("localhost:5001", "localhost:5000", "/localhost5001");
```

Creates a SkipperHashMap "map1" and does a put and remove:

```
  SkipperHashMap<String, Integer> map = sk.getHashMap("map1", String.class, Integer.class);
  // Puts one key-value pair.
  map.put("key1", 1);
  // Removes it
  map.remove("key1");
```

Objects with the same name will have a consistent view of data.

