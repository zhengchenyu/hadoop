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

import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.FederationGlobalResourceCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.GlobalFederationTempQueuePerPartition;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * This class provides methods to calculate global allocation within a federated
 * cluster.
 */
public class GlobalAllocationCalculator {

  private ResourceCalculator rc;

  public GlobalAllocationCalculator(ResourceCalculator rc) {
    this.rc = rc;
  }

  /**
   * This method leverages functionalities from the
   * {@code PreemptableResourceCalculator} to compute the ideal allocation for a
   * federated cluster, and updates the {@code FederationGlobalView} state
   * accordingly.
   * 
   * @param global the input federated cluster state.
   */
  public void computeOverallAllocation(FederationGlobalView global) {

    FederationQueue root = global.getGlobal();        // 这里为全局的queue, 即merge的一起的queue
    GlobalFederationTempQueuePerPartition rootAsTQ = GlobalQueuesConversionUtils
        .getAsMergedTempQueuePerPartition(rc, global);      // 对所有subcluster进行merge得到的queue

    MinimalCSPreemptionContext context =
        new MinimalCSPreemptionContext(rootAsTQ, 0.0, 0.0, rc);

    FederationGlobalResourceCalculator prc =
        new FederationGlobalResourceCalculator(context, true);

    prc.computeIdealAllocation(root.getTotCap(), root.getTotCap());   // 第二个参数设置有误，应该为允许抢占的资源

    GlobalQueuesConversionUtils.updateIdealAlloc(rootAsTQ, global);

  }

}
