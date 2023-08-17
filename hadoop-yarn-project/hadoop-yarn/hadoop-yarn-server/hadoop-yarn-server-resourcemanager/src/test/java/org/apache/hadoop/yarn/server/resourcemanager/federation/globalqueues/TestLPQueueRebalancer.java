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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.ojalgo.optimisation.Optimisation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the functionality of {@code LPQueueRebalancer}. It has two
 * inner class, one using parametrized execution and one running basic
 * misconfiguration tests (once).
 */

@RunWith(Enclosed.class)
public class TestLPQueueRebalancer {

  @RunWith(value = Parameterized.class)
  public static class TestLPQueueRebalancerParam
      extends GlobalQueueAlgoBaseTest {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(TestLPQueueRebalancer.class);

    @Parameterized.Parameters(name = "Testing with: {0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {
          { "globalqueues/global-allocation/fed-allocation1.json" },
          { "globalqueues/global-allocation/fed-allocation2.json" },
          { "globalqueues/global-allocation/fed-allocation3.json" },
          { "globalqueues/global-allocation/fed-allocation4.json" },
          { "globalqueues/global-allocation/fed-loadbalance1.json" } });
    }

    @Test
    public void testFromFile() throws Exception {
      FederationGlobalView federationGlobalView = parseCluster(inputFile);

      // init and run LP
      LPQueueRebalancer qr =
          new LPQueueRebalancer(new Configuration(), rc, minAlloc);
      qr.init(federationGlobalView);
      Optimisation.Result result = qr.solve();

      // check result for optimality/feasibility
      Optimisation.State state = result.getState();
      assertTrue(state.isFeasible());
      assertTrue(state.isSuccess());
      assertTrue(state.isOptimal());

      // export back in FederationGlobalView format and validate
      FederationGlobalView rebalanced = qr.exportResultToFederationGlobalView();
      rebalanced.validate();

    }
  }

  /**
   * Non parametrized run of mis-uses of the {@code LPQueueRebalancer}.
   */
  public static class TestLPQueueRebalancerMisuses
      extends GlobalQueueAlgoBaseTest {

    @Test(expected = FederationPolicyException.class)
    public void testNoInitOnSolve() throws Exception {
      LPQueueRebalancer qr =
          new LPQueueRebalancer(new Configuration(), rc, minAlloc);
      Optimisation.Result result = qr.solve();
    }

    @Test(expected = FederationPolicyException.class)
    public void testNoInitOnExport() throws Exception {
      LPQueueRebalancer qr =
          new LPQueueRebalancer(new Configuration(), rc, minAlloc);
      qr.exportResultToFederationGlobalView();
    }

    @Test(expected = FederationPolicyException.class)
    public void testNoSolveOnExport() throws Exception {
      FederationGlobalView federationGlobalView =
          parseCluster("globalqueues/global-allocation/fed-allocation1.json");

      // init and run LP
      LPQueueRebalancer qr =
          new LPQueueRebalancer(new Configuration(), rc, minAlloc);
      qr.init(federationGlobalView);

      qr.exportResultToFederationGlobalView();
    }

  }
}
