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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DEFAULT_RESOURCE_CALCULATOR_CLASS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.federation.globalqueues.GlobalQueuesComparator;
import org.apache.hadoop.yarn.server.resourcemanager.federation.globalqueues.LoadBalancingGlobalQueueComparator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Temporary data-structure tracking resource availability, pending resource
 * need, current utilization. This is per-queue-per-partition data structure
 * inheriting from {@code TempQueuePerPartition} and adding functionalities
 * needed for federation global queue management.
 */
public class GlobalFederationTempQueuePerPartition
    extends TempQueuePerPartition {

  private Configuration conf;
  private SubClusterId scope;
  private Resource zero = Resources.createResource(0, 0);
  private GlobalQueuesComparator comparator;
  private ResourceCalculator resourceCalculator;

  Resource totalPartitionUnassigned; // this is passed by reference to track
  // globally

  // this represents the allocation for this queue in each sub-cluster
  final Map<SubClusterId, GlobalFederationTempQueuePerPartition> localSelf;

  public GlobalFederationTempQueuePerPartition(String queueName,
      Resource current, boolean preemptionDisabled, String partition,
      Resource killable, float absCapacity, float absMaxCapacity,
      Resource totalPartitionResource, Resource reserved, CSQueue queue,
      Resource effMinRes, Resource effMaxRes, SubClusterId scope,
      Configuration config) {
    super(queueName, current, preemptionDisabled, partition, killable,
        absCapacity, absMaxCapacity, totalPartitionResource, reserved, queue,
        effMinRes, effMaxRes);

    this.conf = config;

    resourceCalculator =
        ReflectionUtils.newInstance(
            conf.getClass(RESOURCE_CALCULATOR_CLASS,
                DEFAULT_RESOURCE_CALCULATOR_CLASS, ResourceCalculator.class),
            conf);

    comparator = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.FEDERATION_GLOBAL_QUEUES_HEURISTIC,
            LoadBalancingGlobalQueueComparator.class,
            GlobalQueuesComparator.class),
        conf);

    comparator.init(resourceCalculator, totalPartitionResource);

    this.scope = scope;
    this.conf = config;
    this.localSelf = new TreeMap<>();
  }

  public SubClusterId getScope() {
    return scope;
  }

  public GlobalFederationTempQueuePerPartition getLocalSelf(SubClusterId id) {
    return localSelf.get(id);
  }

  public void addLocalSelf(SubClusterId id,
      GlobalFederationTempQueuePerPartition q) {
    assert leafQueue == null;
    localSelf.put(id, q);
  }

  public void addAllLocalSelf(
      Map<SubClusterId, GlobalFederationTempQueuePerPartition> map) {
    assert leafQueue == null;
    localSelf.putAll(map);
  }

  // for global allocation purposes we don't want this, overriding with
  // passthrough
  @Override
  protected Resource filterByMaxDeductAssigned(ResourceCalculator rc,
      Resource clusterResource, Resource accepted) {
    return accepted;
  }

  @Override
  protected void initializeRootIdealWithGuarangeed() {
    super.initializeRootIdealWithGuarangeed();
    for (TempQueuePerPartition l : localSelf.values()) {
      l.initializeRootIdealWithGuarangeed();
    }
  }

  @Override
  protected Resource acceptedByLocality(ResourceCalculator rc,
      Resource globalOffer) {
    StringBuilder sb = new StringBuilder();

    sb.append(getQueueName());
    sb.append(" globalOffer: ");
    sb.append(globalOffer.getMemorySize());

    if (localSelf == null || localSelf.size() == 0) {
      return globalOffer;
    }

    Resource accepted = Resources.createResource(0L, 0);
    // first divide among "used"
    acceptByUsed(rc, globalOffer, sb, accepted);

    // if there is more to assign, spread based on pending
    if (Resources.greaterThan(rc, totalPartitionResource, globalOffer,
        accepted)) {

      acceptedByPending(rc, globalOffer, sb, accepted);

    }

    sb.append(" total accepted: ");
    sb.append(accepted.getMemorySize());
    System.out.println(sb.toString());

    return accepted;

  }

  /**
   * This heuristics accepts resources based on pending (and available)
   * resources in each sub-cluster. It favors sub-clusters where
   * pending/available is highest (i.e., where this queue has most demands
   * w.r.t. to what is needed.
   *
   * FIXME we might want to change this to a heuristics that favors picking where
   * the sum of demands is lowest (i.e., where my picking containers is least
   * likely to affect others).
   *
   * @param rc the ResourceCalculator
   * @param globalOffer the offer
   * @param sb
   * @param accepted
   * @return
   */
  private void acceptedByPending(ResourceCalculator rc, Resource globalOffer,
      StringBuilder sb, Resource accepted) {
    Resource globalOfferLeft = Resources.subtract(globalOffer, accepted);

    // SORT localSelf based on pending/totalPartitionUnassigned ratio (largest
    // first)
    List<GlobalFederationTempQueuePerPartition> sortedGQ =
        new ArrayList(localSelf.values());

    sortedGQ.sort(comparator);

    sb.append(" accepted locally: [");
    for (GlobalFederationTempQueuePerPartition l : sortedGQ) {

      // find portion of globalOffer available in this cluster
      Resource locallyFree = Resources.clone(l.totalPartitionUnassigned);

      Resource consumable = Resources.min(rc, this.totalPartitionResource,
          locallyFree, l.getPending());
      Resource locallyAcceptableByPending = Resources.clone(Resources.min(rc,
          this.totalPartitionResource, consumable, globalOfferLeft));

      // book-keep at global/local level about what we accepted
      if (Resources.greaterThan(rc, totalPartitionResource,
          locallyAcceptableByPending, zero)) {

        // update global book-keeping
        Resources.subtractFrom(globalOfferLeft, locallyAcceptableByPending);
        Resources.addTo(accepted, locallyAcceptableByPending);

        // update local book-keeping
        Resources.subtractFrom(l.totalPartitionUnassigned,
            locallyAcceptableByPending);
        Resources.addTo(l.idealAssigned, locallyAcceptableByPending);
        sb.append(
            l.getScope() + ": " + locallyAcceptableByPending.getMemorySize());
        sb.append(", ");
      }
    }
    sb.append("] ");
  }

  /**
   * This is a heuristic that accepts capacity based on currently used
   * resources, the globalOffer is proportionally spread among sub-cluster based
   * on where consumption is currently happening.
   *
   * @param rc the ResourceCalculator
   * @param globalOffer the current round of offering to this queue
   * @param sb StringBuilder used for logging.
   * @return the amount of accepted resources based on "used" resources.
   */
  private Resource acceptByUsed(ResourceCalculator rc, Resource globalOffer,
      StringBuilder sb, Resource accepted) {
    sb.append(" acceptedByUse: [");
    for (Map.Entry<SubClusterId, GlobalFederationTempQueuePerPartition> e : localSelf
        .entrySet()) {

      SubClusterId subCluster = e.getKey();
      GlobalFederationTempQueuePerPartition l = e.getValue();

      double locallyUsedRatio =
          Resources.ratio(rc, l.getUsed(), this.getUsed());
      Resource locallyAcceptableByUse =
          Resources.min(rc, this.totalPartitionResource,
              Resources.subtract(l.getUsed(), l.idealAssigned),
              Resources.multiply(globalOffer, locallyUsedRatio));

      if (Resources.greaterThan(rc, totalPartitionResource,
          locallyAcceptableByUse, zero)) {
        // update total to return
        Resources.addTo(accepted, locallyAcceptableByUse);
        // update local book-keeping
        Resources.addTo(l.idealAssigned, locallyAcceptableByUse);
        Resources.subtractFrom(l.totalPartitionUnassigned,
            locallyAcceptableByUse);

        sb.append(subCluster + ": " + locallyAcceptableByUse.getMemorySize());
        sb.append(", ");
      }
    }
    sb.append("] ");

    return accepted;
  }

  public String toGlobalString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n").append(toString());
    for (TempQueuePerPartition c : children) {
      sb.append(c.toGlobalString());
    }
    return sb.toString();
  }

  public Map<SubClusterId, GlobalFederationTempQueuePerPartition> getLocalSelves() {
    return localSelf;
  }

  /**
   * Return the leaves of a hierarchy in a Map <name, object>.
   *
   * @return the leaves of the hierarchy in a sorted Map.
   */
  public Map<String, GlobalFederationTempQueuePerPartition> getLeaves() {
    Map<String, GlobalFederationTempQueuePerPartition> ret = new TreeMap<>();
    if (getChildren() == null || getChildren().size() == 0) {
      ret.put(this.getQueueName(), this);
    } else {
      for (TempQueuePerPartition c : getChildren()) {
        ret.putAll(((GlobalFederationTempQueuePerPartition) c).getLeaves());
      }
    }
    return ret;
  }

  public Resource getTotalPartitionUnassigned() {
    return totalPartitionUnassigned;
  }

  public void setTotalPartitionUnassigned(Resource totalPartitionUnassigned) {
    this.totalPartitionUnassigned = totalPartitionUnassigned;
  }

  /**
   * Search a children by name.
   *
   * @param queuename the input name of the queue we are looking for.
   * @return the queue if one is found, or null otherwise.
   */
  public GlobalFederationTempQueuePerPartition getChildrenByName(
      String queuename) {
    for (TempQueuePerPartition c : children) {
      if (c.getQueueName().equals(queuename)) {
        return (GlobalFederationTempQueuePerPartition) c;
      } else {
        ((GlobalFederationTempQueuePerPartition) c)
            .getChildrenByName(queuename);
      }
    }
    return null;
  }
}
