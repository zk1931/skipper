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

import java.nio.ByteBuffer;

/**
 * SkipperModule.
 */
public abstract class SkipperModule {

  protected final CommandPool commandsPool;

  /**
   * The name of the module.
   */
  protected final String name;

  /**
   * The server id of the process.
   */
  protected final String serverId;

  SkipperModule(CommandPool pool, String name, String serverId) {
    this.commandsPool = pool;
    this.name = name;
    this.serverId = serverId;
  }

  abstract ByteBuffer preprocess(ByteBuffer request);
}
