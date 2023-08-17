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

import static org.apache.hadoop.yarn.server.resourcemanager.federation.globalqueues.GlobalQueueTestUtil.parseCluster;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This class tests the functionalities of {@code GlobalAllocationCalculator}.
 */
@RunWith(value = Parameterized.class)
public class TestGlobalCalculatorAllocationGlobalQueueAlgo
    extends GlobalQueueAlgoBaseTest {

  private GlobalAllocationCalculator gpc;
  private ResourceCalculator rc;

  @Parameterized.Parameters(name = "Testing with: {0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { "globalqueues/global-allocation/fed-allocation1.json" },
        { "globalqueues/global-allocation/fed-allocation2.json" },
        { "globalqueues/global-allocation/fed-allocation3.json" },
        { "globalqueues/global-allocation/fed-allocation4.json" } });
  }

  @Before
  public void setup() {
    this.minAlloc = Resource.newInstance(100, 1);
    rc = new DominantResourceCalculator();
  }

  @Test
  public void testGlobalAllocation() throws Exception {

    FederationGlobalView federationGlobalView = parseCluster(inputFile);

    gpc = new GlobalAllocationCalculator(rc);

    // compute global allocation
    gpc.computeOverallAllocation(federationGlobalView);

    System.out.println(federationGlobalView);

    checkInvariants(federationGlobalView, true);

  }

}
