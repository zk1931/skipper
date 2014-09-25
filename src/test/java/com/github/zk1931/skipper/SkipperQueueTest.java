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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test SkipperQueue.
 */
public class SkipperQueueTest extends TestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(SkipperQueue.class);

  File getDir(String server) {
    return new File(getDirectory(), server);
  }

  @Test
  public void testGeneral() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creats two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1));
    Skipper sk2 = new Skipper(server2, server1, getDir(server2));

    // Gets one SkipperMap from each context with the same name.
    SkipperQueue<String> queue1 =
      sk1.getQueue("m1", String.class);

    SkipperQueue<String> queue2 =
      sk2.getQueue("m1", String.class);

    queue1.add("test1");
    queue2.add("test2");
    queue2.add("test3");
    queue2.add("test4");

    SkipperFuture f1 = queue1.removeAsync();
    SkipperFuture f2 = queue1.removeAsync();
    SkipperFuture f3 = queue1.removeAsync();
    SkipperFuture f4 = queue1.removeAsync();

    Assert.assertEquals("test1", f1.get());
    Assert.assertEquals("test2", f2.get());
    Assert.assertEquals("test3", f3.get());
    Assert.assertEquals("test4", f4.get());
    // Now all the elements should have been removed.
    Assert.assertTrue(queue1.isEmpty());

    queue1.add("test1");
    queue1.add("test2");

    // Both queue1 and queue2 should see "test1" as their first element.
    Assert.assertEquals("test1", queue1.peek());
    Assert.assertEquals("test1", queue2.peek());

    queue1.clear();
    // Now all the elements should have been cleared.
    Assert.assertTrue(queue1.isEmpty());

    sk1.shutdown();
    sk2.shutdown();
  }

  @Test
  public void testSync() throws Exception {
    // We do a bunch of operations on queue1 and then create the queue2,
    // queue2 will have seen the same state as queue1.
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creates the first Skipper.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1));

    // Creates queue1.
    SkipperQueue<String> queue1 =
      sk1.getQueue("m1", String.class);

    // A bunch of adds.
    queue1.add("test1");
    queue1.add("test2");
    queue1.add("test3");
    queue1.add("test4");

    // The second Skipper joins the first one.
    Skipper sk2 = new Skipper(server2, server1, getDir(server2));
    // Creates queue2 with the same name.
    SkipperQueue<String> queue2 =
      sk2.getQueue("m1", String.class);
    // Verify if it gets synchronized.
    Assert.assertEquals(4, queue2.size());
    Assert.assertEquals("test1", queue2.remove());
    Assert.assertEquals("test2", queue2.remove());
    Assert.assertEquals("test3", queue2.remove());
    Assert.assertEquals("test4", queue2.remove());

    sk1.shutdown();
    sk2.shutdown();
  }

  @Test
  public void testTake() throws Exception {
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creates the two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1));
    Skipper sk2 = new Skipper(server2, server1, getDir(server2));

    // Creates queue1.
    final SkipperQueue<String> queue1 =
      sk1.getQueue("m1", String.class);

    // Creates queue2.
    final SkipperQueue<String> queue2 =
      sk2.getQueue("m1", String.class);

    queue1.add("test1");
    // Make sure we can get it if there're something in queue.
    Assert.assertEquals("test1", queue1.take());

    ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();

    // Append "test2" one second later in another thread.
    es.schedule(new Runnable() { public void run() { queue1.add("test2"); }},
                (long)1, TimeUnit.SECONDS);
    // Make sure we can get it.
    Assert.assertEquals("test2", queue1.take());

    // Append "test3" one second later in another thread.
    es.schedule(new Runnable() { public void run() { queue1.add("test3"); }},
                (long)1, TimeUnit.SECONDS);
    // Make sure we can get it from queue2.
    Assert.assertEquals("test3", queue2.take());

    sk1.shutdown();
    sk2.shutdown();
  }

  @Test(timeout=5000)
  public void testConcurrentTake() throws Exception {
    // Two consumers consume the queue concurrently, one producer produces.
    String server1 = getUniqueHostPort();
    String server2 = getUniqueHostPort();

    // Creates the two Skippers.
    Skipper sk1 = new Skipper(server1, server1, getDir(server1));
    Skipper sk2 = new Skipper(server2, server1, getDir(server2));

    // Creates queue1.
    final SkipperQueue<String> queue1 =
      sk1.getQueue("m1", String.class);

    // Creates queue2.
    final SkipperQueue<String> queue2 =
      sk2.getQueue("m1", String.class);

    final CountDownLatch count = new CountDownLatch(10);

    ExecutorService es1 = Executors.newSingleThreadExecutor();
    ExecutorService es2 = Executors.newSingleThreadExecutor();

    class Consumer implements Runnable {
      final SkipperQueue<String> queue;

      Consumer(SkipperQueue<String> queue) {
        this.queue = queue;
      }

      @Override
      public void run() {
        while (true) {
          try {
            String item = queue.take();
            LOG.info("GOT {} from {}", item, queue);
            count.countDown();
          } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
    }

    es1.submit(new Consumer(queue1));
    es2.submit(new Consumer(queue2));

    for (int i = 0; i < 10; ++i) {
      queue1.add("item" + i);
    }

    count.await();
    sk1.shutdown();
    sk2.shutdown();
  }
}
