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

import java.util.concurrent.CountDownLatch;

/**
 * A Future represents the result of an asynchronous operation.
 */
public class SkipperFuture {
  // TODO : Add listeners.
  private volatile Object result = null;
  private volatile SkipperException exception = null;
  private final CountDownLatch cond = new CountDownLatch(1);

  /**
   * Waits if necessary for the computation to complete, and then retrieves
   * its result.
   */
  public Object get() throws InterruptedException, SkipperException {
    this.cond.await();
    if (this.exception != null) {
      // If there's an exception, throw it.
      throw this.exception;
    }
    return this.result;
  }

  void setResult(Object res) {
    this.result = res;
  }

  void setException(SkipperException ex) {
    this.exception = ex;
  }

  void wakeup() {
    this.cond.countDown();
  }
}
