/**
 * Licensed to the zk1931 under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.zk1931.skipper;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test SkipperMap.
 */
public class SkipperMapTest extends TestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(SkipperMapTest.class);

  File getDir(String server) {
    return new File(getDirectory(), server);
  }

  @Test
  public void testCluster() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creats two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1));
    Skipper sk2 = new Skipper(server2, server1, getDir(server2));

    // Gets one SkipperMap from each context with the same name.
    SkipperMap<String, String> map1 =
      sk1.getMap("m1", String.class, String.class);
    SkipperMap<String, String> map2 =
      sk2.getMap("m1", String.class, String.class);

    // A blocking put on map 1.
    map1.put("key1", "value1").get();
    String v = map1.get("key1");
    // Make sure the we have a consistent "get" after "put".
    Assert.assertEquals("value1", v);

    // A blocking put on map 2.
    map2.put("key2", "value2").get();
    v = map2.get("key2");
    // Make sure the we have a consistent "get" after "put".
    Assert.assertEquals("value2", v);

    // Make sure both m1 and m2 have same 2 key-value pairs.
    Assert.assertEquals(2, map1.size());
    Assert.assertEquals(2, map2.size());
    sk1.shutdown();
    sk2.shutdown();
  }

  @Test
  public void testPut() throws Exception {
    String server1 = getUniqueHostPort();
    Skipper sk1 = new Skipper(server1, server1, getDir(server1));
    SkipperMap<String, String> map1 =
      sk1.getMap("m1", String.class, String.class);
    map1.put("key1", "value1");
    SkipperFuture ft = map1.put("key1", "value2");
    // Gets the return value of second put.
    String prevValue = (String)ft.get();
    // It shoule be the value of first put.
    Assert.assertEquals("value1", prevValue);
    sk1.shutdown();
  }

  @Test
  public void testRemove() throws Exception {
    String server1 = getUniqueHostPort();
    Skipper sk1 = new Skipper(server1, server1, getDir(server1));
    SkipperMap<String, String> map1 =
      sk1.getMap("m1", String.class, String.class);
    map1.put("key1", "value1");
    SkipperFuture ft = map1.remove("key1");
    // Gets the return value of remove.
    String prevValue = (String)ft.get();
    // It shoule be the value of first put.
    Assert.assertEquals("value1", prevValue);
    // After remove, the size should be 0.
    Assert.assertEquals(0, map1.size());
    sk1.shutdown();
  }

  @Test(expected=SkipperException.WrongTypeException.class)
  public void testWrongType() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creats two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1));
    Skipper sk2 = new Skipper(server2, server1, getDir(server2));

    // Gets one SkipperMap from each context with the same name.
    SkipperMap<String, String> map1 =
      sk1.getMap("m1", String.class, String.class);

    // map2 creates the SkipperMap with same name as map1 but with different
    // types, we should raise an exception.
    SkipperMap<String, Integer> map2 =
      sk2.getMap("m1", String.class, Integer.class);

    sk1.shutdown();
    sk2.shutdown();
  }
}
