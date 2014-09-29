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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test SkipperHashMap.
 */
public class SkipperHashMapTest extends TestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(SkipperHashMapTest.class);

  File getDir(String server) {
    return new File(getDirectory(), server);
  }

  @Test
  public void testCluster() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creats two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    Skipper sk2 = new Skipper(server2, server1, getDir(server2), null);

    // Gets one SkipperHashMap from each context with the same name.
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);
    SkipperHashMap<String, String> map2 =
      sk2.getHashMap("m1", String.class, String.class);

    // A blocking put on map 1.
    map1.put("key1", "value1");
    String v = map1.get("key1");
    // Make sure the we have a consistent "get" after "putAsync".
    Assert.assertEquals("value1", v);

    // A blocking put on map 2.
    map2.put("key2", "value2");
    v = map2.get("key2");
    // Make sure the we have a consistent "get" after "putAsync".
    Assert.assertEquals("value2", v);

    // Puts same key on map1 again just to get synchronized.
    map1.put("key1", "value1");

    // Make sure both m1 and m2 have same 2 key-value pairs.
    Assert.assertEquals(2, map1.size());
    Assert.assertEquals(2, map2.size());
    sk1.shutdown();
    sk2.shutdown();
  }

  @Test
  public void testPut() throws Exception {
    String server1 = getUniqueHostPort();
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);
    map1.putAsync("key1", "value1");
    String prevValue = map1.put("key1", "value2");
    // It shoule be the value of first put.
    Assert.assertEquals("value1", prevValue);
    sk1.shutdown();
  }

  @Test
  public void testRemove() throws Exception {
    String server1 = getUniqueHostPort();
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);
    map1.putAsync("key1", "value1");
    String prevValue = map1.remove("key1");
    // It shoule be the value of first put.
    Assert.assertEquals("value1", prevValue);
    // After removeAsync, the size should be 0.
    Assert.assertEquals(0, map1.size());
    sk1.shutdown();
  }

  @Test(expected=SkipperException.WrongTypeException.class)
  public void testWrongType() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creats two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    Skipper sk2 = new Skipper(server2, server1, getDir(server2), null);

    // Gets one SkipperHashMap from each context with the same name.
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);

    // map2 creates the SkipperHashMap with same name as map1 but with different
    // types, we should raise an exception.
    SkipperHashMap<String, Integer> map2 =
      sk2.getHashMap("m1", String.class, Integer.class);

    sk1.shutdown();
    sk2.shutdown();
  }

  @Test
  public void testClear() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creats two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    Skipper sk2 = new Skipper(server2, server1, getDir(server2), null);

    // Gets one SkipperHashMap from each context with the same name.
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);
    SkipperHashMap<String, String> map2 =
      sk2.getHashMap("m1", String.class, String.class);

    // A blocking putAsync on map 1.
    map1.put("key1", "value1");
    String v = map1.get("key1");
    // Make sure the we have a consistent "get" after "putAsync".
    Assert.assertEquals("value1", v);

    // A blocking putAsync on map 2.
    map2.put("key2", "value2");
    v = map2.get("key2");
    // Make sure the we have a consistent "get" after "putAsync".
    Assert.assertEquals("value2", v);

    // Make sure both m1 and m2 have same 2 key-value pairs.
    Assert.assertEquals(2, map1.size());
    Assert.assertEquals(2, map2.size());

    map1.clear();
    // After clearAsync, the size should become 0.
    Assert.assertEquals(0, map1.size());

    sk1.shutdown();
    sk2.shutdown();
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creats two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    Skipper sk2 = new Skipper(server2, server1, getDir(server2), null);

    // Gets one SkipperHashMap from each context with the same name.
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);
    SkipperHashMap<String, String> map2 =
      sk2.getHashMap("m1", String.class, String.class);

    // A blocking putIfAbsent on map 1.
    String v = map1.putIfAbsent("key1", "value1");
    // Since it's the first putAsync, putAsyncIfAbset should return null.
    Assert.assertEquals(null, v);

    // A putIfAbsent put on map 2 with same key as first putAsync.
    v = (String)map2.putIfAbsent("key1", "value2");
    // Gets the return value of puIfAbsent, it should be the value of first
    // put.
    Assert.assertEquals("value1", v);

    // Make sure second putIfAbsent has no effects on SkipperHashMap.
    Assert.assertEquals("value1", map2.get("key1"));

    // Make sure both m1 and m2 have same 1 key-value pairs.
    Assert.assertEquals(1, map1.size());
    Assert.assertEquals(1, map2.size());

    sk1.shutdown();
    sk2.shutdown();
  }

  @Test
  public void testPutAll() throws Exception {
    String server1 = getUniqueHostPort();
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);
    Map<String, String> hm = new HashMap<String, String>();
    hm.put("key1", "value1");
    hm.put("key2", "value2");
    hm.put("key3", "value3");
    map1.putAll(hm);
    // Make sure the size of map is 3.
    Assert.assertEquals(3, map1.size());
    sk1.shutdown();
  }

  @Test
  public void testRemoveIf() throws Exception {
    String server1 = getUniqueHostPort();
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);
    map1.put("key1", "value1");
    // Removes key1 if its value is "value2".
    map1.remove("key1", "value2");
    // Since value of key1 is value1, it shouldn't be removed.
    Assert.assertEquals("value1", map1.get("key1"));
    // Removes key1 if its value is "value1".
    map1.remove("key1", "value1");
    // Now it's get removed.
    Assert.assertTrue(map1.isEmpty());
    sk1.shutdown();
  }

  @Test
  public void testReplace() throws Exception {
    String server1 = getUniqueHostPort();
    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    SkipperHashMap<String, String> map1 =
      sk1.getHashMap("m1", String.class, String.class);
    // Replace key-value pair key1 with value1.
    map1.replace("key1", "value1");
    // Since there's no key1, so the replace should have no effect on
    // SkipperHashMap.
    Assert.assertTrue(map1.isEmpty());

    map1.put("key1", "value1");
    map1.replace("key1", "value2");
    // Since key1 is there, then the replacement should work.
    Assert.assertEquals("value2", map1.get("key1"));

    // Replace the value of key1 to value3 if its value is value1.
    map1.replace("key1", "value1", "value3");
    // Shouldn't be replaced.
    Assert.assertEquals("value2", map1.get("key1"));
    // Replace the value of key1 to value3 if its value is value2.
    map1.replace("key1", "value2", "value3");
    // Should be replaced.
    Assert.assertEquals("value3", map1.get("key1"));
    sk1.shutdown();
  }

  @Test
  public void testStateChangeCallback() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();
    String server3 = getUniqueHostPort();

    class TestChangeCallback implements StateChangeCallback {
      volatile String leader;
      volatile Set<String> activeFollowers;
      volatile Set<String> clusterMembers;

      @Override
      public void leading(Set<String> actives,
                          Set<String> members) {
        this.activeFollowers = actives;
        this.clusterMembers = members;
      }

      @Override
      public void following(String ld, Set<String> members) {
        this.leader = ld;
        this.clusterMembers = members;
      }
    }

    TestChangeCallback testCallback = new TestChangeCallback();

    Skipper sk1 = new Skipper(server1, server1, getDir(server1), null);
    Skipper sk2 = new Skipper(server2, server1, getDir(server2), null);
    Skipper sk3 = new Skipper(server3, server1, getDir(server3), testCallback);

    Assert.assertTrue(server1.equals(testCallback.leader) ||
                      server2.equals(testCallback.leader));
    Assert.assertEquals(3, testCallback.clusterMembers.size());

    sk1.shutdown();
    sk2.shutdown();
    sk3.shutdown();
  }
}
