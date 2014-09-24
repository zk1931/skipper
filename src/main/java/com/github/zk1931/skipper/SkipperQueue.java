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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
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
  final TreeMap<Long, E> preAppliedMap = new TreeMap<>();

  private final String name;

  private static final Logger LOG =
      LoggerFactory.getLogger(SkipperQueue.class);

  final Class<E> elemType;

  SkipperQueue(String name, CommandPool pool, Class<E> et) {
    super(pool);
    this.name = name;
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

  @Override
  ByteBuffer preprocess(ByteBuffer request) {
    try {
      Command cmd = Serializer.deserialize(request);
      if (cmd instanceof AddCommand) {
        long id = 0;
        // Finds out the largest id in current pre-applied map.
        for (long i : preAppliedMap.keySet()) {
          id = i;
        }
        id += 1;
        ((AddCommand)cmd).setKey(id);
        // Pre-applies this command.
        preAppliedMap.put(id, (E)((AddCommand)cmd).getItem());
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
      }
      return Serializer.serialize(cmd);
    } catch (Exception ex) {
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
    Object execute(SkipperModule module) {
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
    Object execute(SkipperModule module) {
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
    Object execute(SkipperModule module) {
      SkipperQueue queue = (SkipperQueue)module;
      queue.map.clear();
      return null;
    }
  }
}
