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
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SkipperMap, a distributed hash map implementation.
 */
public class SkipperMap<K extends Serializable, V extends Serializable>
  extends SkipperModule {

  private final ConcurrentHashMap<K, V> map =
    new ConcurrentHashMap<K, V>();

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

  SkipperMap(String name, CommandPool pool, Class<K> kt, Class<V> vt) {
    super(pool);
    this.name = name;
    this.keyType = kt;
    this.valueType = vt;
  }

  /**
   * Puts a key-value pair to SkipperMap asynchronously.
   *
   * @param key the key.
   * @param value the value.
   * @return a Skipper object.
   */
  public SkipperFuture put(K key, V value) {
    Command put = new PutCommand<K, V>(this.name, key, value);
    return commandsPool.enqueueCommand(put);
  }

  /**
   * Removes a key-value pair from SkipperMap asynchronously.
   *
   * @param key the key.
   * @param value the value.
   * @return a SkipperFuture object.
   */
  public SkipperFuture remove(K key) {
    Command remove = new RemoveCommand<K>(this.name, key);
    return commandsPool.enqueueCommand(remove);
  }

  /**
   * Gets the value of given key.
   *
   * @param key the key.
   * @return the value of this key, or null if the key is not in SkipperMap.
   */
  public V get(K key) {
    return this.map.get(key);
  }

  /**
   * Gets the size of the SkipperMap.
   *
   * @return the size of the SkipperMap.
   */
  public int size() {
    return this.map.size();
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
   * PutCommand.
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
   * RemoveCommand.
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
}
