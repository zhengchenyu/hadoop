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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * This class provides extensions of the {@code PreemptableResourceCalculator}
 * needed to support global calculations for federation sub-cluster management.
 */
public class FederationGlobalResourceCalculator
    extends PreemptableResourceCalculator {

  public FederationGlobalResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector) {
    super(preemptionContext, isReservedPreemptionCandidatesSelector);
  }

  /**
   * For the computation of federation, we need the initial assignment to be
   * book-keeped by the queue as well.
   */
  @Override
  protected void initIdealAssignment(Resource totGuarant,
      TempQueuePerPartition q, Resource initIdealAssigned) {
    q.offer(initIdealAssigned, rc, totGuarant, false);
  }

  @Override
  protected void recursivelyComputeIdealAssignment(TempQueuePerPartition root,
                                                 Resource totalPreemptionAllowed) {
    // clone the root idealAssigned in each SC, to prime the amounts
    // each children can allocate in a subcluster
    if (root instanceof GlobalFederationTempQueuePerPartition) {

      Map<SubClusterId, Resource> unassignedToInherit = new HashMap<>();
      for (Map.Entry<SubClusterId, GlobalFederationTempQueuePerPartition> e
              : ((GlobalFederationTempQueuePerPartition) root).localSelf
              .entrySet()) {
        unassignedToInherit.put(e.getKey(),
                Resources.clone(e.getValue().getIdealAssigned()));
      }

      // FIRST propagate down the how much each children can use in a
      // sub-cluster
      if (root.getChildren() != null && root.getChildren().size() > 0) {
        for (TempQueuePerPartition t : root.getChildren()) {
          for (Map.Entry<SubClusterId, GlobalFederationTempQueuePerPartition> localT
                  : ((GlobalFederationTempQueuePerPartition) t).localSelf
                  .entrySet()) {
            localT.getValue().setTotalPartitionUnassigned(
                    unassignedToInherit.get(localT.getKey()));
          }
        }
      }
    }

    // compute ideal distribution at this level
    computeIdealResourceDistribution(rc, root.getChildren(),
            totalPreemptionAllowed, root.idealAssigned);

    if (root.getChildren() != null && root.getChildren().size() > 0) {
      // compute recursively for lower levels and build list of leafs
      for (TempQueuePerPartition t : root.getChildren()) {
        recursivelyComputeIdealAssignment(t, totalPreemptionAllowed);
      }
    }
  }

}
