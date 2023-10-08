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

package org.apache.hadoop.yarn.server.federation.resolver;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestScriptBasedSubClusterResolverImpl {

  @Test
  public void testNullConfig() throws Throwable {
    ScriptBasedSubClusterResolverImpl resolver = new ScriptBasedSubClusterResolverImpl();
    resolver.setConf(new Configuration());
    resolver.load();
    LambdaTestUtils.intercept(YarnException.class, "Script file name should not be null!",
        () -> {
          resolver.getSubClusterForNode("NodeX");
        });
  }

  @Test
  public void testResolveSubCluster() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.FEDERATION_CLUSTER_RESOLVER_SCRIPT_FILE_NAME, "subcluster.sh");
    ScriptBasedSubClusterResolverImplForTest resolver =
        new ScriptBasedSubClusterResolverImplForTest();
    resolver.setConf(conf);
    resolver.load();
    // Case 1: resolve unknown node and rack
    LambdaTestUtils.intercept(YarnException.class, "Cannot find subClusterId for node",
        () -> {
          resolver.getSubClusterForNode("NodeX");
        });
    LambdaTestUtils.intercept(YarnException.class, "Cannot resolve rack",
        () -> {
          resolver.getSubClustersForRack("RackX");
        });
    // Case 2: resolve known node and rack
    SubClusterId clusterId1 = SubClusterId.newInstance("SC-1");
    SubClusterId clusterId2 = SubClusterId.newInstance("SC-2");
    Assert.assertEquals(clusterId1, resolver.getSubClusterForNode("Node1"));
    Assert.assertEquals(clusterId2, resolver.getSubClusterForNode("Node2"));
    Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newSet(clusterId1, clusterId2),
        resolver.getSubClustersForRack("Rack1")));
    Assert.assertTrue(CollectionUtils.isEqualCollection(Sets.newSet(clusterId2),
        resolver.getSubClustersForRack("Rack2")));
  }

  class ScriptBasedSubClusterResolverImplForTest extends ScriptBasedSubClusterResolverImpl {

    ScriptBasedSubClusterResolverImplForTest() {
    }

    protected String runResolveCommand(String arg, String commandScriptName, String mode) {
      if (mode == ScriptBasedSubClusterResolverImpl.MODE_NODE) {
        switch (arg) {
        case "Node1":
          return "SC-1";
        case "Node2":
          return "SC-2";
        default:
          return null;
        }
      } else if (mode == ScriptBasedSubClusterResolverImpl.MODE_RACK) {
        switch (arg) {
        case "Rack1":
          return "SC-1 SC-2";
        case "Rack2":
          return "SC-2";
        default:
          return null;
        }
      }
      return null;
    }
  }
}
