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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.CapacitySchedulerPreemptionContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.TempQueuePerPartition;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * This class implements a minimalistic
 * {@code CapacitySchedulerPreemptionContext}, used to simply store
 * TempQueuePerPartition we obtain from files or network, to be used with the
 * {@code PreemptableResourceCalculator}.
 */
public class MinimalCSPreemptionContext
    implements CapacitySchedulerPreemptionContext {

  private final TempQueuePerPartition root;
  private final double naturalTerminationFactor;
  private final double maxIgnoredOverCapacity;
  private final ResourceCalculator resourceCalculator;
  private Map<String, String> underServedQueues;
  private Map<String, TempQueuePerPartition> queuesByName;

  public MinimalCSPreemptionContext(TempQueuePerPartition root,
      double naturalTerminationFactor, double maxIgnoredOverCapacity,
      ResourceCalculator rc) {
    this.root = root;
    this.naturalTerminationFactor = naturalTerminationFactor;
    this.maxIgnoredOverCapacity = maxIgnoredOverCapacity;
    this.resourceCalculator = rc;
    underServedQueues = new HashMap<>();
    queuesByName = new HashMap<>();
    populateQueueNamesMap(root);
  }

  @Override
  public TempQueuePerPartition getQueueByPartition(String queueName,
      String partition) {
    if (partition.equals(RMNodeLabelsManager.NO_LABEL)) {
      return queuesByName.get(queueName);
    } else {
      throw new NotImplementedException(
          "Support for labels not implemented yet.");
    }
  }

  private void populateQueueNamesMap(TempQueuePerPartition curRoot) {
    queuesByName.put(curRoot.getQueueName(), curRoot);
    for (TempQueuePerPartition c : curRoot.getChildren()) {
      populateQueueNamesMap(c);
    }
  }

  @Override
  public Collection<TempQueuePerPartition> getQueuePartitions(
      String queueName) {
    return Collections.singletonList(queuesByName.get(queueName));
  }

  @Override
  public double getMaxIgnoreOverCapacity() {
    return maxIgnoredOverCapacity;
  }

  @Override
  public double getNaturalTerminationFactor() {
    return naturalTerminationFactor;
  }

  @Override
  public CapacityScheduler getScheduler() {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  @Override
  public RMContext getRMContext() {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public boolean isObserveOnly() {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public Set<ContainerId> getKillableContainers() {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public Set<String> getLeafQueueNames() {
    return getLeafQueueNames(root);
  }

  private Set<String> getLeafQueueNames(TempQueuePerPartition localRoot) {
    Set<String> leafQueues = new HashSet<>();
    for (TempQueuePerPartition c : localRoot.getChildren()) {
      if (c.getChildren() == null || c.getChildren().size() == 0) {
        leafQueues.add(c.getQueueName());
      } else {
        leafQueues.addAll(getLeafQueueNames(c));
      }
    }
    return leafQueues;
  }

  @Override
  public Set<String> getAllPartitions() {
    return Collections.singleton(RMNodeLabelsManager.NO_LABEL);
  }

  @Override
  public int getClusterMaxApplicationPriority() {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public Resource getPartitionResource(String partition) {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public LinkedHashSet<String> getUnderServedQueuesPerPartition(
      String partition) {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public void addPartitionToUnderServedQueues(String queueName,
      String partition) {
    underServedQueues.put(queueName, partition);
  }

  @Override
  public float getMinimumThresholdForIntraQueuePreemption() {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public float getMaxAllowableLimitForIntraQueuePreemption() {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public long getDefaultMaximumKillWaitTimeout() {
    return 0;
  }

  @Override
  public ProportionalCapacityPreemptionPolicy.IntraQueuePreemptionOrderPolicy getIntraQueuePreemptionOrderPolicy() {
    throw new NotImplementedException(
        "Method not implemented by " + this.getClass().getCanonicalName());
  }

  @Override
  public boolean getCrossQueuePreemptionConservativeDRF() {
    return false;
  }

  @Override
  public boolean getInQueuePreemptionConservativeDRF() {
    return false;
  }
}
