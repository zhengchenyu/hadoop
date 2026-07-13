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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.*;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFairOrderingPolicy {

  final static int GB = 1024;

  @Test
  public void testSimpleComparison() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
      new FairOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    assertEquals(0, policy.getComparator().compare(r1, r2), "Comparator Output");

    //consumption
    r1.setUsed(Resources.createResource(1, 0));
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r1.getSchedulingResourceUsage());
    assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testSizeBasedWeight() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
      new FairOrderingPolicy<MockSchedulableEntity>();
    policy.setSizeBasedWeight(true);
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    //No changes, equal
    assertEquals(0, policy.getComparator().compare(r1, r2), "Comparator Output");

    r1.setUsed(Resources.createResource(4 * GB));
    r2.setUsed(Resources.createResource(4 * GB));

    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(4 * GB));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());

    //Same, equal
    assertEquals(0, policy.getComparator().compare(r1, r2), "Comparator Output");

    r2.setUsed(Resources.createResource(5 * GB));
    r2.setPending(Resources.createResource(5 * GB));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());

    //More demand and consumption, but not enough more demand to overcome
    //additional consumption
    assertTrue(policy.getComparator().compare(r1, r2) < 0);

    //High demand, enough to reverse sbw
    r2.setPending(Resources.createResource(100 * GB));
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      r2.getSchedulingResourceUsage());
    assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testPendingResourceFirstDisabledByDefault() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    MockSchedulableEntity withMoreUsedAndPending =
        new MockSchedulableEntity();
    MockSchedulableEntity withoutPending = new MockSchedulableEntity();

    withMoreUsedAndPending.setUsed(Resources.createResource(4 * GB));
    withMoreUsedAndPending.setPending(Resources.createResource(1 * GB));
    withoutPending.setUsed(Resources.createResource(1 * GB));
    withoutPending.setPending(Resources.none());

    updateSchedulingResourceUsage(withMoreUsedAndPending);
    updateSchedulingResourceUsage(withoutPending);

    assertTrue(policy.getComparator()
        .compare(withMoreUsedAndPending, withoutPending) > 0);
  }

  @Test
  public void testPendingResourceFirstOrdersPendingAppsFirst() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    Map<String, String> conf = new HashMap<>();
    conf.put(FairOrderingPolicy.ENABLE_PENDING_RESOURCE_FIRST, "true");
    policy.configure(conf);

    MockSchedulableEntity withMoreUsedAndPending =
        new MockSchedulableEntity();
    MockSchedulableEntity withoutPending = new MockSchedulableEntity();

    withMoreUsedAndPending.setUsed(Resources.createResource(4 * GB));
    withMoreUsedAndPending.setPending(Resources.createResource(1 * GB));
    withoutPending.setUsed(Resources.createResource(1 * GB));
    withoutPending.setPending(Resources.none());

    updateSchedulingResourceUsage(withMoreUsedAndPending);
    updateSchedulingResourceUsage(withoutPending);

    assertTrue(policy.isPendingResourceFirst());
    assertTrue(policy.hasPendingResource(withMoreUsedAndPending));
    assertTrue(policy.getComparator()
        .compare(withMoreUsedAndPending, withoutPending) < 0);
    assertTrue(policy.getComparator()
        .compare(withoutPending, withMoreUsedAndPending) > 0);
  }

  @Test
  public void testPendingResourceFirstKeepsFairOrderWithinSamePendingGroup() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    Map<String, String> conf = new HashMap<>();
    conf.put(FairOrderingPolicy.ENABLE_PENDING_RESOURCE_FIRST, "true");
    policy.configure(conf);

    MockSchedulableEntity lessUsed = new MockSchedulableEntity();
    MockSchedulableEntity moreUsed = new MockSchedulableEntity();

    lessUsed.setUsed(Resources.createResource(1 * GB));
    lessUsed.setPending(Resources.createResource(1 * GB));
    moreUsed.setUsed(Resources.createResource(4 * GB));
    moreUsed.setPending(Resources.createResource(1 * GB));

    updateSchedulingResourceUsage(lessUsed);
    updateSchedulingResourceUsage(moreUsed);

    assertTrue(policy.getComparator().compare(lessUsed, moreUsed) < 0);
  }

  @Test
  public void testPendingResourceFirstUsesAllResourceTypes() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    Map<String, String> conf = new HashMap<>();
    conf.put(FairOrderingPolicy.ENABLE_PENDING_RESOURCE_FIRST, "true");
    policy.configure(conf);

    MockSchedulableEntity withOnlyVcorePending = new MockSchedulableEntity();
    MockSchedulableEntity withoutPending = new MockSchedulableEntity();

    withOnlyVcorePending.setPending(Resources.createResource(0, 1));
    withoutPending.setPending(Resources.none());

    updateSchedulingResourceUsage(withOnlyVcorePending);
    updateSchedulingResourceUsage(withoutPending);

    assertTrue(policy.hasPendingResource(withOnlyVcorePending));
    assertTrue(policy.getComparator()
        .compare(withOnlyVcorePending, withoutPending) < 0);
  }

  @Test
  public void testDemandUpdatedReordersWhenPendingResourceFirstEnabled() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    Map<String, String> conf = new HashMap<>();
    conf.put(FairOrderingPolicy.ENABLE_PENDING_RESOURCE_FIRST, "true");
    policy.configure(conf);

    MockSchedulableEntity first = new MockSchedulableEntity();
    first.setId("first");
    first.setUsed(Resources.createResource(1 * GB));
    first.setPending(Resources.none());

    MockSchedulableEntity second = new MockSchedulableEntity();
    second.setId("second");
    second.setUsed(Resources.createResource(4 * GB));
    second.setPending(Resources.none());

    updateSchedulingResourceUsage(first);
    updateSchedulingResourceUsage(second);
    policy.addSchedulableEntity(first);
    policy.addSchedulableEntity(second);

    checkIds(policy.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR), new String[]{"first",
            "second"});

    second.setPending(Resources.createResource(1 * GB));
    checkIds(policy.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR), new String[]{"first",
            "second"});

    policy.demandUpdated(second);
    checkIds(policy.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR), new String[]{"second",
            "first"});
  }

  @Test
  public void testIterators() {
    OrderingPolicy<MockSchedulableEntity> schedOrder =
     new FairOrderingPolicy<MockSchedulableEntity>();

    MockSchedulableEntity msp1 = new MockSchedulableEntity();
    MockSchedulableEntity msp2 = new MockSchedulableEntity();
    MockSchedulableEntity msp3 = new MockSchedulableEntity();

    msp1.setId("1");
    msp2.setId("2");
    msp3.setId("3");

    msp1.setUsed(Resources.createResource(3));
    msp2.setUsed(Resources.createResource(2));
    msp3.setUsed(Resources.createResource(1));

    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp2.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
      msp2.getSchedulingResourceUsage());

    schedOrder.addSchedulableEntity(msp1);
    schedOrder.addSchedulableEntity(msp2);
    schedOrder.addSchedulableEntity(msp3);


    //Assignment, least to greatest consumption
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"3", "2", "1"});

    //Preemption, greatest to least
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"1", "2", "3"});

    //Change value without inform, should see no change
    msp2.setUsed(Resources.createResource(6));
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"3", "2", "1"});
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"1", "2", "3"});

    //Do inform, will reorder
    schedOrder.containerAllocated(msp2, null);
    checkIds(schedOrder.getAssignmentIterator(
        IteratorSelector.EMPTY_ITERATOR_SELECTOR),
        new String[]{"3", "1", "2"});
    checkIds(schedOrder.getPreemptionIterator(), new String[]{"2", "1", "3"});
  }

  @Test
  public void testSizeBasedWeightNotAffectAppActivation() throws Exception {
    assertFairOrderingPolicyDoesNotAffectAppActivation(
        FairOrderingPolicy.ENABLE_SIZE_BASED_WEIGHT);
  }

  @Test
  public void testPendingResourceFirstNotAffectAppActivation()
      throws Exception {
    assertFairOrderingPolicyDoesNotAffectAppActivation(
        FairOrderingPolicy.ENABLE_PENDING_RESOURCE_FIRST);
  }

  private void assertFairOrderingPolicyDoesNotAffectAppActivation(
      String policyParameter) throws Exception {
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();

    // Define top-level queues
    String defaultPath = CapacitySchedulerConfiguration.ROOT + ".default";
    QueuePath queuePath = new QueuePath(defaultPath);
    csConf.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getCanonicalName());
    csConf.setOrderingPolicy(queuePath,
        CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY);
    csConf.setOrderingPolicyParameter(queuePath,
        policyParameter, "true");
    csConf.setMaximumApplicationMasterResourcePerQueuePercent(queuePath, 0.1f);

    // inject node label manager
    MockRM rm = new MockRM(csConf);
    try {
      rm.start();

      CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

      // Get LeafQueue
      LeafQueue lq = (LeafQueue) cs.getQueue("default");
      OrderingPolicy<FiCaSchedulerApp> policy = lq.getOrderingPolicy();
      assertTrue(policy instanceof FairOrderingPolicy);
      FairOrderingPolicy<FiCaSchedulerApp> fairPolicy =
          (FairOrderingPolicy<FiCaSchedulerApp>) policy;
      if (FairOrderingPolicy.ENABLE_SIZE_BASED_WEIGHT.equals(
          policyParameter)) {
        assertTrue(fairPolicy.getSizeBasedWeight());
      } else {
        assertTrue(fairPolicy.isPendingResourceFirst());
      }

      rm.registerNode("h1:1234", 10 * GB);

      // Submit 4 apps
      for (int i = 0; i < 4; i++) {
        MockRMAppSubmissionData data =
            MockRMAppSubmissionData.Builder.createWithMemory(1 * GB, rm)
                .withAppName("app")
                .withUser("user")
                .withAcls(null)
                .withQueue("default")
                .withUnmanagedAM(false)
                .build();
        MockRMAppSubmitter.submit(rm, data);
      }

      assertEquals(1, lq.getNumActiveApplications());
      assertEquals(3, lq.getNumPendingApplications());

      // Try allocate once, #active-apps and #pending-apps should be still
      // correct.
      cs.handle(new NodeUpdateSchedulerEvent(
          rm.getRMContext().getRMNodes().get(NodeId.newInstance("h1", 1234))));
      assertEquals(1, lq.getNumActiveApplications());
      assertEquals(3, lq.getNumPendingApplications());
    } finally {
      rm.close();
    }
  }

  public void checkIds(Iterator<MockSchedulableEntity> si,
      String[] ids) {
    for (int i = 0;i < ids.length;i++) {
      assertEquals(si.next().getId(), ids[i]);
    }
  }

  private void updateSchedulingResourceUsage(MockSchedulableEntity entity) {
    AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
        entity.getSchedulingResourceUsage());
  }

  @Test
  public void testOrderingUsingUsedAndPendingResources() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    policy.setSizeBasedWeight(true);
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    r1.setUsed(Resources.createResource(4 * GB));
    r2.setUsed(Resources.createResource(4 * GB));

    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(4 * GB));

    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    // Same, equal
    assertEquals(0, policy.getComparator().compare(r1, r2),
        "Comparator Output");

    r1.setUsed(Resources.createResource(4 * GB));
    r2.setUsed(Resources.createResource(8 * GB));

    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(8 * GB));

    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    assertTrue(policy.getComparator().compare(r1, r2) < 0);
  }

  @Test
  public void testOrderingUsingAppSubmitTime() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<>();
    policy.setSizeBasedWeight(true);
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    // R1, R2 has been started at same time
    assertEquals(r1.getStartTime(), r2.getStartTime());

    // No changes, equal
    assertEquals(0, policy.getComparator().compare(r1, r2), "Comparator Output");

    // R2 has been started after R1
    r1.setStartTime(5);
    r2.setStartTime(10);

    assertTrue(policy.getComparator().compare(r1, r2) < 0);

    // R1 has been started after R2
    r1.setStartTime(10);
    r2.setStartTime(5);

    assertTrue(policy.getComparator().compare(r1, r2) > 0);
  }

  @Test
  public void testOrderingUsingAppDemand() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<MockSchedulableEntity>();
    MockSchedulableEntity r1 = new MockSchedulableEntity();
    MockSchedulableEntity r2 = new MockSchedulableEntity();

    r1.setUsed(Resources.createResource(0));
    r2.setUsed(Resources.createResource(0));

    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    // Same, equal
    assertEquals(0, policy.getComparator().compare(r1, r2), "Comparator Output");

    // Compare demands ensures entity without resource demands gets lower
    // priority
    r1.setPending(Resources.createResource(0));
    r2.setPending(Resources.createResource(8 * GB));
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    assertTrue(policy.getComparator().compare(r1, r2) > 0);

    // When both entity has certain demands, then there is no actual comparison
    r1.setPending(Resources.createResource(4 * GB));
    r2.setPending(Resources.createResource(12 * GB));
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r1.getSchedulingResourceUsage());
    AbstractComparatorOrderingPolicy
        .updateSchedulingResourceUsage(r2.getSchedulingResourceUsage());

    assertEquals(0, policy.getComparator().compare(r1, r2),
        "Comparator Output");
  }

  @Test
  public void testRemoveEntitiesWithSizeBasedWeightAsCompletedJobs() {
    FairOrderingPolicy<MockSchedulableEntity> policy =
        new FairOrderingPolicy<MockSchedulableEntity>();
    policy.setSizeBasedWeight(true);

    // Add 10 different schedulable entities
    List<MockSchedulableEntity> entities = new ArrayList<>(10);
    for (int i = 1; i <= 10; i++) {
      MockSchedulableEntity r = new MockSchedulableEntity();
      r.setApplicationPriority(Priority.newInstance(i));
      r.setUsed(Resources.createResource(4 * i));
      r.setPending(Resources.createResource(4 * i));
      AbstractComparatorOrderingPolicy.updateSchedulingResourceUsage(
          r.getSchedulingResourceUsage());
      policy.addSchedulableEntity(r);
      entities.add(r);
    }

    // Mark the first 5 entities as completed by setting
    // the resources to 0
    for (int i = 0; i < 5; i++) {
      MockSchedulableEntity r = entities.get(i);
      r.getSchedulingResourceUsage().setCachedUsed(
          CommonNodeLabelsManager.ANY, Resources.createResource(0));
      r.getSchedulingResourceUsage().setCachedPending(
          CommonNodeLabelsManager.ANY, Resources.createResource(0));
      policy.entityRequiresReordering(r);
    }

    policy.reorderScheduleEntities();

    // Remove the first 5 elements
    for (int i = 0; i < 5; i++) {
      policy.removeSchedulableEntity(entities.get(i));
    }

    assertEquals(5, policy.getNumSchedulableEntities());
  }
}
