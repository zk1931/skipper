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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SkipperMap, a distributed map implementation. It has the compatible interface
 * with java Map and ConcurrentMap. You can just use it as java ConcurrentMap.
 * It also has some asynchronous versions for some of the operations.
 */
public class SkipperMap<K extends Serializable, V extends Serializable>
  extends SkipperModule implements ConcurrentMap<K, V> {

  private final ConcurrentMap<K, V> map;

  private final String name;

  /**
   * The type of the key.
   */
  final Class<K> keyType;

  /**
   * The type of the value.
   */
  final Class<V> valueType;

  private static final Logger LOG =
      LoggerFactory.getLogger(SkipperMap.class);

  SkipperMap(String name, CommandPool pool, Class<K> kt, Class<V> vt,
             ConcurrentMap<K, V> map) {
    super(pool);
    this.name = name;
    this.keyType = kt;
    this.valueType = vt;
    this.map = map;
  }

  /**
   * Puts a key-value pair to SkipperMap asynchronously.
   *
   * @param key the key.
   * @param value the value.
   * @return a SkipperFuture object.
   */
  public SkipperFuture putAsync(K key, V value) {
    Command put = new PutCommand<K, V>(this.name, key, value);
    return commandsPool.enqueueCommand(put);
  }

  /**
   * Puts a key-value pair to SkipperMap synchronously.
   *
   * @param key the key.
   * @param value the value.
   * @return previous key.
   */
  @Override
  public V put(K key, V value) {
    try {
      return (V)putAsync(key, value).get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Removes a key-value pair from SkipperMap asynchronously.
   *
   * @param key the key.
   * @param value the value.
   * @return a SkipperFuture object.
   */
  public SkipperFuture removeAsync(Object key) {
    Command remove = new RemoveCommand<K>(this.name, (K)key);
    return commandsPool.enqueueCommand(remove);
  }

  /**
   * Removes a key-value pair from SkipperMap synchronously.
   *
   * @param key the key.
   * @param value the value.
   * @return value of the key.
   */
  @Override
  public V remove(Object key) {
    try {
      return (V)removeAsync(key).get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Gets the value of given key.
   *
   * @param key the key.
   * @return the value of this key, or null if the key is not in SkipperMap.
   */
  @Override
  public V get(Object key) {
    return this.map.get(key);
  }

  /**
   * Gets the size of the SkipperMap.
   *
   * @return the size of the SkipperMap.
   */
  @Override
  public int size() {
    return this.map.size();
  }

  /**
   * Returns true if the map contains no key-value mappings.
   *
   * @return true if this map contains no key-value mappings.
   */
  @Override
  public boolean isEmpty() {
    return this.map.isEmpty();
  }

  /**
   * Removes all of the mappings from this map asynchronously.
   *
   * @return a SkipperFuture object.
   */
  public SkipperFuture clearAsync() {
    ClearCommand clear = new ClearCommand(this.name);
    return commandsPool.enqueueCommand(clear);
  }

  /**
   * Removes all of the mappings from this map synchronously.
   */
  @Override
  public void clear() {
    try {
      clearAsync().get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Returns true if this map contains a mapping for the specified key.
   *
   * @return true if this map contains a mapping for the specified key.
   */
  @Override
  public boolean containsKey(Object key) {
    return this.map.containsKey(key);
  }

  /**
   * Returns true if this map maps one or more keys to the specified value.
   *
   * @return true if this map maps one or more keys to the specified value.
   */
  @Override
  public boolean containsValue(Object value) {
    return this.map.containsValue(value);
  }

  /**
   * If the specified key is not already associated with a value, associate it
   * with the given value.
   *
   * @param key the key.
   * @param value the value.
   * @return a SkipperFuture object.
   */
  public SkipperFuture putIfAbsentAsync(K key, V value) {
    Command putIfAbsent = new PutIfAbsentCommand<K, V>(this.name, key, value);
    return commandsPool.enqueueCommand(putIfAbsent);
  }

  /**
   * If the specified key is not already associated with a value, associate it
   * with the given value.
   *
   * @param key the key.
   * @param value the value.
   * @return the previous value associated with the specified key, or null if
   * there was no mapping for the key.
   */
  @Override
  public V putIfAbsent(K key, V value) {
    try {
      return (V)putIfAbsentAsync(key, value).get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Copies all of the mappings from the specified map to this map
   * asynchronously.
   *
   * @param m mappings to be stored in this map.
   * @return a SkipperFuture object.
   */
  public SkipperFuture putAllAsync(Map<? extends K, ? extends V> m) {
    PutAllCommand putAll = new PutAllCommand<K, V>(this.name, m);
    return commandsPool.enqueueCommand(putAll);
  }

  /**
   * Copies all of the mappings from the specified map to this map
   * synchronously.
   *
   * @param m mappings to be stored in this map.
   */
  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    try {
      putAllAsync(m).get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * The Map.entrySet method returns a collection-view of the map, whose
   * elements are of this class.
   *
   * @return a set view of the mappings contained in this map.
   */
  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return this.map.entrySet();
  }

  /**
   * Returns a Set view of the keys contained in this map.
   *
   * @return a set view of the keys contained in this map.
   */
  @Override
  public Set<K> keySet() {
    return this.map.keySet();
  }

  /**
   * Returns a Collection view of the values contained in this map.
   *
   * @return a Collection view of the values contained in this map.
   */
  @Override
  public Collection<V> values() {
    return this.map.values();
  }

  /**
   * Removes the entry for a key only if currently mapped to a given value
   * asynchronously.
   *
   * @param key the key.
   * @param value the value.
   * @return the SkipperFuture object.
   */
  public SkipperFuture removeAsync(Object key, Object value) {
    RemoveIfCommand remove = new RemoveIfCommand(this.name, key, value);
    return this.commandsPool.enqueueCommand(remove);
  }

  /**
   * Removes the entry for a key only if currently mapped to a given value
   * synchronously.
   *
   * @param key the key.
   * @param value the value.
   * @return true if the value was removed.
   */
  @Override
  public boolean remove(Object key, Object value) {
    try {
      return (boolean)removeAsync(key, value).get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Replaces the entry for a key only if currently mapped to some value.
   *
   * @param key the key.
   * @param value the value.
   * @return the SkipperFuture object.
   */
  public SkipperFuture replaceAsync(K key, V value) {
    ReplaceCommand<K, V> replace =
      new ReplaceCommand<K, V>(this.name, key, value);
    return this.commandsPool.enqueueCommand(replace);
  }

  /**
   * Replaces the entry for a key only if currently mapped to a given value.
   *
   * @param key the key.
   * @param oldValue value expected to be associated with the specified key.
   * @param newValue value to be associated with the specified key.
   * @return the SkipperFuture object.
   */
  public SkipperFuture replaceAsync(K key, V oldValue, V newValue) {
    ReplaceIfCommand<K, V> replace =
      new ReplaceIfCommand<K, V>(this.name, key, oldValue, newValue);
    return this.commandsPool.enqueueCommand(replace);
  }

  /**
   * Replaces the entry for a key only if currently mapped to some value.
   *
   * @param key the key.
   * @param value the value.
   * @return true if the value was replaced.
   */
  @Override
  public V replace(K key, V value) {
    try {
      return (V)replaceAsync(key, value).get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Replaces the entry for a key only if currently mapped to a given value.
   *
   * @param key the key.
   * @param oldValue value expected to be associated with the specified key.
   * @param newValue value to be associated with the specified key.
   * @return true if the value was replaced.
   */
  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    try {
      return (boolean)replaceAsync(key, oldValue, newValue).get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  ByteBuffer preprocess(ByteBuffer request) {
    // Since the operations of Map are idempotent, just return it directly.
    return request;
  }

  /**
   * The base class for the commands of SkipperMap.
   */
  abstract static class MapCommand extends Command {

    private static final long serialVersionUID = 0L;

    MapCommand(String source) { super(source); }
  }

  /**
   * Command for put.
   */
  static class PutCommand<K, V> extends MapCommand {
    private static final long serialVersionUID = 0L;
    private final K key;
    private final V value;

    PutCommand(String name, K key, V value) {
      super(name);
      this.key = key;
      this.value = value;
    }

    @Override
    Object execute(SkipperModule module) {
      return ((SkipperMap)module).map.put(this.key, this.value);
    }
  }

  /**
   * Command for remove.
   */
  static class RemoveCommand<K> extends MapCommand {
    private static final long serialVersionUID = 0L;
    private final K key;

    RemoveCommand(String name, K key) {
      super(name);
      this.key = key;
    }

    @Override
    Object execute(SkipperModule module) {
      return ((SkipperMap)module).map.remove(this.key);
    }
  }

  /**
   * Command for clear.
   */
  static class ClearCommand extends MapCommand {
    private static final long serialVersionUID = 0L;

    ClearCommand(String source) {
      super(source);
    }

    @Override
    Object execute(SkipperModule module) {
      ((SkipperMap)module).map.clear();
      return null;
    }
  }

  /**
   * Command for putIfAbsent.
   */
  static class PutIfAbsentCommand<K, V> extends MapCommand {
    private static final long serialVersionUID = 0L;
    private final K key;
    private final V value;

    PutIfAbsentCommand(String name, K key, V value) {
      super(name);
      this.key = key;
      this.value = value;
    }

    @Override
    Object execute(SkipperModule module) {
      return ((SkipperMap)module).map.putIfAbsent(this.key, this.value);
    }
  }

  /**
   * Command for putAll.
   */
  static class PutAllCommand<K, V> extends MapCommand {
    private static final long serialVersionUID = 0L;
    private final Map<? extends K, ? extends V> map;

    PutAllCommand(String name, Map<? extends K, ? extends V> map) {
      super(name);
      this.map = map;
    }

    @Override
    Object execute(SkipperModule module) {
      ((SkipperMap)module).map.putAll(this.map);
      return null;
    }
  }

  /**
   * Command for remove a key-value pair with a given key and value.
   */
  static class RemoveIfCommand<K, V> extends MapCommand {
    private static final long serialVersionUID = 0L;
    private final K key;
    private final V value;

    RemoveIfCommand(String source, K key, V value) {
      super(source);
      this.key = key;
      this.value = value;
    }

    @Override
    Object execute(SkipperModule module) {
      return ((SkipperMap)module).map.remove(this.key, this.value);
    }
  }

  static class ReplaceCommand<K, V> extends MapCommand {
    private static final long serialVersionUID = 0L;
    private final K key;
    private final V value;

    ReplaceCommand(String source, K key, V value) {
      super(source);
      this.key = key;
      this.value = value;
    }

    @Override
    Object execute(SkipperModule module) {
      return ((SkipperMap)module).map.replace(this.key, this.value);
    }
  }

  /**
   * Command for replace with given key and value.
   */
  static class ReplaceIfCommand<K, V> extends MapCommand {
    private static final long serialVersionUID = 0L;
    private final K key;
    private final V oldValue;
    private final V newValue;

    ReplaceIfCommand(String source, K key, V oldValue, V newValue) {
      super(source);
      this.key = key;
      this.oldValue = oldValue;
      this.newValue = newValue;
    }

    @Override
    Object execute(SkipperModule module) {
      return ((SkipperMap)module).map.replace(this.key,
                                              this.oldValue,
                                              this.newValue);
    }
  }
}
