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
 * SkipperContext.
 */
class SkipperContext extends SkipperModule {
  /**
   * Stores all the SkipperMaps.
   */
  final ConcurrentHashMap<String, SkipperMap> maps =
    new ConcurrentHashMap<String, SkipperMap>();

  /**
   * Stores all the SkipperQueues.
   */
  final ConcurrentHashMap<String, SkipperQueue> queues =
    new ConcurrentHashMap<String, SkipperQueue>();

  private String serverId;

  private static final Logger LOG =
      LoggerFactory.getLogger(SkipperContext.class);

  SkipperContext(CommandPool pool, String serverId) {
    super(pool);
    this.serverId = serverId;
  }

  @Override
  ByteBuffer preprocess(ByteBuffer message) {
    return message;
  }

  public <K extends Serializable, V extends Serializable> SkipperMap<K, V>
  getMap(String name, Class<K> kt, Class<V> vt)
      throws InterruptedException, SkipperException {
    SkipperMap<K, V> map = maps.get(name);
    if (map == null) {
      CreateMapCommand<K, V> cmd =
        new CreateMapCommand<>(this.serverId, name, kt, vt);
      SkipperFuture ft = this.commandsPool.enqueueCommand(cmd);
      return (SkipperMap<K, V>)ft.get();
    } else {
      if (map.keyType != kt || map.valueType != vt) {
        LOG.error("The newly created SkipperMap has the wrong type with the"
            + " existing one.");
        throw new SkipperException.WrongTypeException();
      }
      return map;
    }
  }

  /**
   * The base class of all the commands of SkipperContext.
   */
  abstract static class KeeperCommand extends Command {

    private static final long serialVersionUID = 0L;

    KeeperCommand(String source) { super(source); }
  }

  /**
   * The command which creates SkipperMap.
   */
  static class CreateMapCommand<K, V> extends KeeperCommand {
    private static final long serialVersionUID = 0L;
    private final Class<K> kt;
    private final Class<V> vt;
    private String name;

    CreateMapCommand(String source, String name, Class<K> kt, Class<V> vt) {
      super(source);
      this.name = name;
      this.kt = kt;
      this.vt = vt;
    }

    @Override
    Object execute(SkipperModule module) throws SkipperException {
      LOG.debug("Create map with key type : {}, value type :  {}", kt, vt);
      SkipperContext ctx = (SkipperContext)module;
      SkipperMap map = ctx.maps.get(name);
      if (map == null) {
        // There's no SkipperMap for this name, creating it.
        map = new SkipperMap(name, module.commandsPool, kt, vt);
        // Actuall the execute is called in single thread and this is the only
        // place we do update, so we don't need to call putIfAbsent. Here we
        // call it just to bypass findbug plugin warnings.
        ctx.maps.putIfAbsent(name, map);
        return ctx.maps.get(name);
      } else {
        // There's already an existing one, if the key-type and value-type
        // don't match the existing one, raise an exception.
        if (map.keyType != kt || map.valueType != vt) {
          LOG.error("The newly created SkipperMap has the wrong type with the"
              + " existing one.");
          throw new SkipperException.WrongTypeException();
        }
      }
      return map;
    }
  }
}
