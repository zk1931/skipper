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

import com.github.zk1931.jzab.QuorumZab;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CommandPool.
 */
public class CommandPool {

  private final QuorumZab zab;

  private final Map<Long, SkipperFuture> pendingFutures =
    new ConcurrentHashMap<>();

  private final AtomicLong cidGenerator = new AtomicLong(0);

  private final BlockingQueue<Command> commandsQueue =
    new LinkedBlockingQueue<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(CommandPool.class);

  // Future for SendCommandTask.
  private final Future<Void> future;

  public CommandPool(QuorumZab zab) {
    this.zab = zab;
    ExecutorService es = Executors.newSingleThreadExecutor();
    this.future = es.submit(new SendCommandTask());
    es.shutdown();
  }

  SkipperFuture enqueueCommand(Command cmd) {
    try {
      // Generates future for this command.
      long cid = cidGenerator.incrementAndGet();
      cmd.setId(cid);
      this.commandsQueue.put(cmd);
      SkipperFuture ft = new SkipperFuture();
      // Add it to pending futures.
      pendingFutures.put(cid, ft);
      return ft;
    } catch (InterruptedException e) {
      throw new RuntimeException("InterruptedException");
    }
  }

  void commandExecuted(long cid, Object result, SkipperException ex) {
    // Remove the corresponding future of this command.
    SkipperFuture ft = pendingFutures.remove(cid);
    if (ft == null) {
      throw new RuntimeException("No pending future for command, bug?");
    }
    // Set the exception.
    ft.setException(ex);
    // Set the result of this command.
    ft.setResult(result);
    // Sinal the future.
    ft.wakeup();
  }

  void shutdown() throws InterruptedException {
    this.future.cancel(true);
  }

  /**
   * The task which keeps sending commands to Zab from the pending commands
   * queue.
   */
  class SendCommandTask implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      while (true) {
        Command cmd = commandsQueue.take();
        ByteBuffer buf = Serializer.serialize(cmd);
        zab.send(buf);
      }
    }
  }
}
