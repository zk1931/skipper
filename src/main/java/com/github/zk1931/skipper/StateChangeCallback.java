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

import java.util.Set;

/**
 * The callback interface for cluster state changes.
 */
public interface StateChangeCallback {
  /**
   * Upcall to notify the application who is running on the leader role of
   * Skipper instance the membership changes of Zab cluster. The membership
   * changes include the detection of recovered members or disconnected members
   * in current configuration or new configuration after some one joined or be
   * removed from current configuration.
   *
   * @param activeFollowers current alive followers.
   * @param clusterMembers the members of new configuration.
   */
  void leading(Set<String> activeFollowers, Set<String> clusterMembeers);

  /**
   * Upcall to notify the application who is running on the follower role of
   * Skipper instance the membership changes of Zab cluster. The membership
   * changes include the detection of the leader or the new cluster
   * configuration after some servers are joined or removed.
   *
   * @param leader current leader.
   * @param clusterMembers the members of new configuration.
   */
  void following(String leader, Set<String> clusterMembers);
}
