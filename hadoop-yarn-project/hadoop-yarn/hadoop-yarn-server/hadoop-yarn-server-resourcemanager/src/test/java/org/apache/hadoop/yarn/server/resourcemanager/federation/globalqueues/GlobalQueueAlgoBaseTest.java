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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.runners.Parameterized;

/**
 * Base class to write GlobalQueue algo tests.
 */
public class GlobalQueueAlgoBaseTest {

  protected ResourceCalculator rc;
  protected Resource minAlloc;

  @Before
  public void setup() {
    rc = new DominantResourceCalculator();
    minAlloc = Resource.newInstance(1, 1);
  }

  @Parameterized.Parameter(value = 0)
  public String inputFile;

  private void recursiveCheckInvariants(FederationQueue root, boolean global)
      throws FederationGlobalQueueValidationException {

    if (root.getExpectedIdealAlloc() != null) {
      assertEquals(
          "IdealAllocation for queue " + root.getQueueName() + " in subcluster "
              + root.getSubClusterId() + " is off from expected ",
          root.getExpectedIdealAlloc(),
          root.getIdealAlloc());
    }

    // If we are not a leaf, propagate down and check child-self coherence
    if (root.getChildren() != null && root.getChildren().size() > 0) {
      Resource childrenIdealAlloc = Resources.createResource(0, 0);
      Resource childrenPreemption = Resources.createResource(0, 0);

      // if we have idealAlloc set check consistency
      if (root.getIdealAlloc() != null) {
        for (FederationQueue c : root.getChildren().values()) {
          if (c.getIdealAlloc() != null) {
            Resources.addTo(childrenIdealAlloc,
                c.getIdealAlloc());
          }
        }
        assertEquals(
            "IdealAlloc for " + root.getQueueName() + "@" + root.getSubClusterId()
                + " is: " + root.getIdealAlloc()
                + " but sum of children is " + childrenIdealAlloc,
            root.getIdealAlloc(), childrenIdealAlloc);
      }

      // recurse down
      for (FederationQueue c : root.getChildren().values()) {
        recursiveCheckInvariants(c, global);
      }
    }
  }

  protected void checkInvariants(FederationGlobalView fgv, boolean global)
      throws FederationGlobalQueueValidationException {
    // check invariants on global (recursive)
    if (fgv.getGlobal() != null) {
      fgv.getGlobal().validate();
    }

    for (FederationQueue lr : fgv.getSubClusters()) {
      // validate already recursively checks the tree
      lr.validate();
      recursiveCheckInvariants(lr, global);
    }
  }

  protected FederationQueue getQueue(List<FederationQueue> local, String sc,
      String queueName) {
    for (FederationQueue l : local) {
      if (l.getSubClusterId().equals(sc)) {
        return l.getChildByName(queueName);
      }
    }
    return null;
  }

}
