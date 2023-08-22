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

import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.ojalgo.optimisation.Optimisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the functionality of {@code LPQueueRebalancer}, by
 * generating random workloads of different size. It can be used for timing
 * analysis for growing size of input problem.
 */
@RunWith(value = Parameterized.class)
public class TestLPQueueRebalancerRandomize {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestLPQueueRebalancerRandomize.class);

  protected ResourceCalculator rc;
  protected Resource minAlloc;

  // adding on top of base class
  @Parameterized.Parameter(value = 0)
  public int queues;

  @Parameterized.Parameter(value = 1)
  public int subclusters;

  @Parameterized.Parameters(name = "Testing with: {0} queues, {1} subclusters")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] { { 10, 5 }, { 50, 20 }, { 100, 20 } });
  }

  @Before
  public void setup() {
    rc = new DominantResourceCalculator();
    minAlloc = Resource.newInstance(1, 1);
  }

  @Test
  public void testLargeRandomRebalance() throws Exception {
    runExperiment(queues, subclusters);
  }

  private void runExperiment(int numQueues, int numSubclusters)
      throws FederationGlobalQueueValidationException,
      FederationPolicyException {
    Map<String, Resource> queues = new TreeMap<>();

    Random rand = new Random();

    Resource totQueueSize = Resource.newInstance(0, 0);
    for (int i = 0; i < numQueues; i++) {
      double r = rand.nextDouble();
      Resource queueSize =
          Resources.multiply(Resource.newInstance(100, 100), r);
      queues.put("a" + i, queueSize);
      Resources.addTo(totQueueSize, queueSize);
    }

    Map<SubClusterId, Resource> subCluster = new TreeMap<>();
    for (int i = 0; i < numSubclusters; i++) {
      Resource subClusterSize = Resources.divideAndCeil(
          new DominantResourceCalculator(), totQueueSize, numSubclusters);
      SubClusterId sc = SubClusterId.newInstance("sc" + i);
      subCluster.put(sc, subClusterSize);
    }

    float[][] affinity = new float[numQueues][numSubclusters];
    for (int i = 0; i < numQueues; i++) {
      for (int j = 0; j < numSubclusters; j++) {
        affinity[i][j] = rand.nextFloat() * 2;
      }
    }

    LPQueueRebalancer qr =
        new LPQueueRebalancer(new Configuration(), rc, minAlloc);

    long tstart = System.currentTimeMillis();
    qr.initProblem(Resource.newInstance(100, 100),
        new DominantResourceCalculator(), queues, subCluster, affinity);

    Optimisation.Result result = qr.solve();
    long tend = System.currentTimeMillis();
    LOGGER.info("Runtime for " + numQueues + " queues " + numSubclusters
        + " sub-clusters is: " + (tend - tstart) + "ms");
    int i = 0;
    Iterator<BigDecimal> iterator = result.iterator();
    while (iterator.hasNext()) {
      System.out.println(String.format("value[%s] = %s", i, iterator.next()));
    }

    // check result for optimality/feasibility
    Optimisation.State state = result.getState();
    assertTrue(state.isFeasible());
    assertTrue(state.isSuccess());
    assertTrue(state.isOptimal());

  }

}
