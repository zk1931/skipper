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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.Properties;
import java.util.Set;
import com.github.zk1931.jzab.QuorumZab;
import com.github.zk1931.jzab.StateMachine;
import com.github.zk1931.jzab.Zxid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skipper.
 */
public class Skipper {

  private final QuorumZab zab;

  private final SkipperStateMachine stateMachine;

  private final String serverId;

  private static final Logger LOG = LoggerFactory.getLogger(Skipper.class);

  private final CommandPool commandsPool;

  private final CountDownLatch broadcasting = new CountDownLatch(1);

  private final SkipperContext skipperCtx;

  /**
   * Constructs Skipper object by joining a Skipper server.
   *
   * @param serverId the server address of this server.
   * @param joinPeer the server address you want to join in.
   * @param logDir the log directory for Skipper.
   * @throws InterruptedException in case of interruption.
   */
  public Skipper(String serverId, String joinPeer, File logdir)
      throws InterruptedException {
    Properties prop = new Properties();
    prop.setProperty("logdir", logdir.getPath());
    prop.setProperty("serverId", serverId);
    this.stateMachine = new SkipperStateMachine();
    this.zab = new QuorumZab(this.stateMachine, prop, joinPeer);
    this.commandsPool= new CommandPool(this.zab);
    this.serverId = this.zab.getServerId();
    this.skipperCtx = new SkipperContext(this.commandsPool, serverId);
    // Waits Jzab enters broadcasting phase.
    this.broadcasting.await();
  }

  /**
   * Constructs Skipper object by recovering from a log directory.
   *
   * @param logDir the log directory for Skipper.
   * @throws InterruptedException in case of interruption.
   */
  public Skipper(File logdir) throws InterruptedException {
    Properties prop = new Properties();
    prop.setProperty("logdir", logdir.getPath());
    this.stateMachine = new SkipperStateMachine();
    this.zab = new QuorumZab(this.stateMachine, prop);
    this.commandsPool= new CommandPool(this.zab);
    this.serverId = this.zab.getServerId();
    this.skipperCtx = new SkipperContext(this.commandsPool, serverId);
    // Waits Jzab enters broadcasting phase.
    this.broadcasting.await();
  }

  /**
   * Shutdown the Skipper.
   *
   * @throws InterruptedException in case of interruption.
   */
  public void shutdown() throws InterruptedException {
    this.commandsPool.shutdown();
    this.zab.shutdown();
  }

  /**
   * Gets a SkipperHashMap object for a given name. It will create a new
   * SkipperHashMap object if it doesn't exist, otherwise return the existing
   * one.
   *
   * @param name the name of the SkipperMap object.
   * @return the SkipperHashMap object.
   * @throws InterruptedException in case of interruption.
   * @throws SkipperException exception from Skipper.
   */
  public <K extends Serializable, V extends Serializable> SkipperHashMap<K, V>
  getHashMap(String name, Class<K> kt, Class<V> vt)
      throws InterruptedException, SkipperException {
    return skipperCtx.getHashMap(name, kt, vt);
  }

  /**
   * Gets a SkipperQueue object for a given name. It will create a new
   * SkipperQueue object if it doesn't exist, otherwise return the existing one.
   *
   * @param name the name of the SkipperQueue object.
   * @return the SkipperQueue object.
   */
  public SkipperQueue getQueue(String name) { return null; }

  /**
   * StateMachine of Skipper.
   */
  class SkipperStateMachine implements StateMachine {

    String setToString(Set<String> set) {
      StringBuilder sb = new StringBuilder();
      String sep = " ";
      for (String server : set) {
        sb.append(sep).append(server);
      }
      return sb.toString();
    }

    // Finds out the correct module object according to the command.
    SkipperModule getModule(Command cmd) {
      if (cmd instanceof SkipperMap.MapCommand) {
        return skipperCtx.maps.get(cmd.getSource());
      } else if (cmd instanceof SkipperContext.KeeperCommand) {
        return skipperCtx;
      }
      return null;
    }

    @Override
    public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
      Command cmd = Serializer.deserialize(message);
      // Gets the right module for this command.
      SkipperModule md = getModule(cmd);
      message.rewind();
      // The message should be preprocessed by its module.
      return md.preprocess(message);
    }

    @Override
    public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId) {
      LOG.debug("Delivered txn : {}", zxid);
      Command cmd = Serializer.deserialize(stateUpdate);
      SkipperModule md = getModule(cmd);
      Object result = null;
      SkipperException exception = null;
      try {
        result = cmd.execute(md);
      } catch (SkipperException ex) {
        LOG.warn("Caught exception while executing command!");
        exception = ex;
      }
      if (clientId != null && clientId.equals(serverId)) {
        // If the command was issued by this server, we need to signal and
        // remove the pending futures.
        commandsPool.commandExecuted(cmd.getId(), result, exception);
      }
    }

    @Override
    public void flushed(ByteBuffer flushReq) {
      LOG.debug("Flushed request");
    }

    @Override
    public void save(OutputStream os) {
      LOG.info("Snapshot save is called!");
    }

    @Override
    public void restore(InputStream is) {
      LOG.info("Snapshot restore is called!");
    }

    @Override
    public void recovering() {
      LOG.info("Recovering!");
    }

    @Override
    public void leading(Set<String> activeFollowers,
                        Set<String> clusterMembers) {
      LOG.info("Leading : ");
      LOG.info("- Active followers : {}", setToString(activeFollowers));
      LOG.info("- Cluster members: {}", setToString(clusterMembers));
      broadcasting.countDown();
    }

    @Override
    public void following(String leader, Set<String> clusterMembers) {
      LOG.info("Following {} : ", leader);
      LOG.info("- Cluster members: {}", setToString(clusterMembers));
      broadcasting.countDown();
    }
  }
}

