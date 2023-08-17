/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.federation.globalqueues;

import java.util.Comparator;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.GlobalFederationTempQueuePerPartition;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * Implementors of this interface provides a heuristic way to sort
 * {@code GlobalFederationTempQueuePerPartition} to implement global queues.
 */
public abstract class GlobalQueuesComparator
    implements Comparator<GlobalFederationTempQueuePerPartition> {

  protected ResourceCalculator resourceCalculator;
  protected Resource totResource;

  public void init(ResourceCalculator rc, Resource totResource) {
    this.resourceCalculator = rc;
    this.totResource = totResource;
  }

}
