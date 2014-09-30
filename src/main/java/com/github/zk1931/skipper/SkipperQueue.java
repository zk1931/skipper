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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SkipperQueue.
 */
public class SkipperQueue<E extends Serializable> extends SkipperModule {

  final ConcurrentSkipListMap<Long, E> map = new ConcurrentSkipListMap<>();

  // On the leader side, we pre-apply every transaction to this map to generate
  // idempotent transactions.
  // TODO : Everytime after Jzab comes back from recovery, we should reset this
  // map.
  private final TreeMap<Long, E> preAppliedMap = new TreeMap<>();

  private final Queue<TakeCommand> pendingTakes = new LinkedList<TakeCommand>();

  private static final Logger LOG =
      LoggerFactory.getLogger(SkipperQueue.class);

  final Class<E> elemType;

  SkipperQueue(CommandPool pool, String name, String serverId, Class<E> et) {
    super(pool, name, serverId);
    this.elemType = et;
  }

  /**
   * Appends an item to the end of SkipperQueue asynchronously.
   *
   * @param e the item.
   * @return a SkipperFuture object.
   */
  public SkipperFuture addAsync(E e) {
    AddCommand add = new AddCommand(this.name, e);
    return this.commandsPool.enqueueCommand(add);
  }

  /**
   * Appends an item to the end of SkipperQueue synchronously.
   *
   * @param e the item.
   */
  public void add(E e) {
    try {
      addAsync(e).get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Gets the first element of the queue without removing it.
   *
   * @return the first element of the queue.
   * @throws NoSuchElementException if there's no elements in the queue.
   */
  public E element() throws NoSuchElementException {
    Iterator<Long> iter = this.map.keySet().iterator();
    if (!iter.hasNext()) {
      throw new NoSuchElementException();
    }
    return this.map.get(iter.next());
  }

  /**
   * Gets the first element of the queue without removing it.
   *
   * @return the first element of the queue or null if there's none.
   */
  public E peek() {
    try {
      return element();
    } catch (NoSuchElementException ex) {
      return null;
    }
  }

  /**
   * Removes and gets the first element of SkipperQueue asynchronously.
   *
   * @return a SkipperFuture object.
   */
  public SkipperFuture removeAsync() {
    RemoveCommand remove = new RemoveCommand(this.name);
    return this.commandsPool.enqueueCommand(remove);
  }

  /**
   * Removes and gets the first element of SkipperQueue synchronously.
   *
   * @return the first element or a RuntimeException if there's no elements in
   * queue.
   */
  public E remove() {
    try {
      return (E)removeAsync().get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Clears the entire SkipperQueue asynchronously.
   *
   * @return a SkipperFuture object.
   */
  public SkipperFuture clearAsync() {
    ClearCommand clear = new ClearCommand(this.name);
    return this.commandsPool.enqueueCommand(clear);
  }

  /**
   * Clears the entire SkipperQueue synchronously.
   */
  public void clear() {
    try {
      clearAsync().get();
    } catch (InterruptedException | SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Gets the size of the queue.
   *
   * @return the size of the queue.
   */
  public int size() {
    return this.map.size();
  }

  /**
   * Returns true if the SkipperQueue is empty.
   *
   * @return true if the SkipperQueue is empty.
   */
  public boolean isEmpty() {
    return this.map.isEmpty();
  }

  /**
   * Retrieves and removes the head of this queue, waiting if necessary until
   * an element becomes available.
   *
   * @return the first element of the queue.
   */
  public E take() throws InterruptedException {
    try {
      TakeCommand take = new TakeCommand(this.name, this.serverId);
      return (E)commandsPool.enqueueCommand(take).get();
    } catch (SkipperException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  ByteBuffer preprocess(ByteBuffer request) {
    try {
      Command cmd = Serializer.deserialize(request);
      if (cmd instanceof AddCommand) {
        AddCommand add = (AddCommand)cmd;
        if (!this.pendingTakes.isEmpty()) {
          // If there're someone who are blocking on take, converts this command
          // to TakeWithCommand and send out.
          cmd = new TakeWithCommand(add.getSource(), pendingTakes.remove(),
                                    add.getItem(), add.getId());
        } else {
          long id = 0;
          // Finds out the largest id in current pre-applied map.
          for (long i : preAppliedMap.keySet()) {
            id = i;
          }
          id += 1;
          add.setKey(id);
          // Pre-applies this command.
          preAppliedMap.put(id, (E)add.getItem());
        }
      } else if (cmd instanceof RemoveCommand) {
        Iterator<Long> iter = preAppliedMap.keySet().iterator();
        RemoveCommand remove = (RemoveCommand)cmd;
        if (!iter.hasNext()) {
          remove.setKey((long)-1);
        } else {
          Long id = iter.next();
          remove.setKey(id);
          // Pre-applies this command.
          preAppliedMap.remove(id);
        }
      } else if (cmd instanceof ClearCommand) {
        // Pre-applies this command.
        preAppliedMap.clear();
      } else if (cmd instanceof TakeCommand) {
        TakeCommand take = (TakeCommand)cmd;
        Iterator<Long> iter = preAppliedMap.keySet().iterator();
        if (!iter.hasNext()) {
          // If there's nothing in the SkipperQueue, turn this into a
          // NullCommand. Also adds the command to pendingTakes queue.
          this.pendingTakes.add(take);
          cmd = new NullCommand(this.name);
        } else {
          //  If there're something in SkipperQueue, then just take it.
          Long id = iter.next();
          take.setKey(id);
          // Pre-applied this command.
          preAppliedMap.remove(id);
        }
      }
      return Serializer.serialize(cmd);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * The base class for the commands of SkipperQueue.
   */
  abstract static class QueueCommand extends Command {

    private static final long serialVersionUID = 0L;

    QueueCommand(String source) { super(source); }
  }

  /**
   * Command for add.
   */
  static class AddCommand<E> extends QueueCommand {
    private static final long serialVersionUID = 0L;
    private final E item;
    private Long key;

    AddCommand(String source, E item) {
      super(source);
      this.item = item;
    }

    void setKey(Long id) {
      this.key = id;
    }

    E getItem() {
      return this.item;
    }

    @Override
    Object execute(SkipperModule module, String clientId) {
      ((SkipperQueue)module).map.put(key, item);
      return null;
    }
  }

  /**
   * Command for remove.
   */
  static class RemoveCommand extends QueueCommand {
    private static final long serialVersionUID = 0L;
    private Long key = (long)-1;

    RemoveCommand(String source) {
      super(source);
    }

    void setKey(Long id) {
      this.key = id;
    }

    @Override
    Object execute(SkipperModule module, String clientId) {
      SkipperQueue queue = (SkipperQueue)module;
      if (key == -1) {
        return new SkipperException.NoSuchElementException();
      }
      return queue.map.remove(this.key);
    }
  }

  /**
   * Command for clear.
   */
  static class ClearCommand extends QueueCommand {
    private static final long serialVersionUID = 0L;

    ClearCommand(String source) {
      super(source);
    }

    @Override
    Object execute(SkipperModule module, String clientId) {
      SkipperQueue queue = (SkipperQueue)module;
      queue.map.clear();
      return null;
    }
  }

  static class TakeCommand extends QueueCommand {
    private static final long serialVersionUID = 0L;
    private Long key = (long)-1;
    private String serverId;

    TakeCommand(String source, String serverId) {
      super(source);
      this.serverId = serverId;
    }

    void setKey(Long id) {
      this.key = id;
    }

    String getServerId() {
      return this.serverId;
    }

    @Override
    Object execute(SkipperModule module, String clientId) {
      SkipperQueue queue = (SkipperQueue)module;
      if (queue.isEmpty()) {
        throw new RuntimeException("The queue is empty for take command, bug?");
      }
      return queue.map.remove(this.key);
    }
  }

  static class NullCommand extends QueueCommand {
    private static final long serialVersionUID = 0L;

    NullCommand(String source) {
      super(source);
      setId(-1);
    }

    @Override
    Object execute(SkipperModule module, String clientId) {
      // Does nothing.
      return null;
    }
  }

  /**
   * The command which unblocks the take call.
   */
  static class TakeWithCommand extends QueueCommand {
    private static final long serialVersionUID = 0L;
    private final TakeCommand take;
    private Object item;

    TakeWithCommand(String source, TakeCommand take, Object item, long id) {
      super(source);
      this.take = take;
      this.item = item;
      setId(id);
    }

    // For TakeWithCommand, the clientId will be the id of who issued
    // the Add command instead of the TakeCommand. So we should record the
    // id of who issued the take command in TakeCommand to figure out whether
    // we should wakeup the future.
    @Override
    Object execute(SkipperModule module, String clientId) {
      if (clientId == null) {
        // Ignores the command which is executed in synchronizing phase.
        return null;
      }
      if (module.serverId.equals(take.getServerId())) {
        // Wakes up the future object.
        module.commandsPool.commandExecuted(take.getId(),
                                            this.item, null);
      }
      return null;
    }
  }
}
