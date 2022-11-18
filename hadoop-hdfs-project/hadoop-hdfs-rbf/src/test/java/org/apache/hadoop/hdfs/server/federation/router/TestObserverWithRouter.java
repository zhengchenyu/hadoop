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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;


public class TestObserverWithRouter {
  private static final String SKIP_BEFORE_EACH_CLUSTER_STARTUP = "SkipBeforeEachClusterStartup";
  public static final String FEDERATION_NS = "ns-fed";
  private MiniRouterDFSCluster cluster;
  private RouterContext routerContext;
  private FileSystem fileSystem;

  @BeforeEach
  void init(TestInfo info) throws Exception {
    if (info.getTags().contains(SKIP_BEFORE_EACH_CLUSTER_STARTUP)) {
      return;
    }
    startUpCluster(2, null);
  }

  @AfterEach
  public void teardown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }

    routerContext = null;

    if (fileSystem != null) {
      fileSystem.close();
      fileSystem = null;
    }
  }

  public void startUpCluster(int numberOfObserver, Configuration confOverrides) throws Exception {
    int numberOfNamenode = 2 + numberOfObserver;
    Configuration conf = new Configuration(false);
    conf.setBoolean(DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");
    if (confOverrides != null) {
      conf.addResource(confOverrides);
    }
    cluster = new MiniRouterDFSCluster(true, 2, numberOfNamenode);
    cluster.addNamenodeOverrides(conf);
    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Making one Namenode active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
        for (int i = 2; i < numberOfNamenode; i++) {
          cluster.switchToObserver(ns, NAMENODES[i]);
        }
      }
    }

    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();

    cluster.addRouterOverrides(conf);
    cluster.addRouterOverrides(routerConf);

    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    // Setup the mount table
    cluster.installMockLocations();

    cluster.waitActiveNamespaces();
    routerContext  = cluster.getRandomRouter();
    fileSystem = routerContext.getFileSystemWithObserverReadsEnabled();
  }

  @Test
  public void testObserverRead() throws Exception {
    internalTestObserverRead();
  }

  /**
   * Tests that without adding config to use ObserverProxyProvider, the client shouldn't
   * have reads served by Observers.
   * Fixes regression in HDFS-13522.
   */
  @Test
  public void testReadWithoutObserverClientConfigurations() throws Exception {
    fileSystem.close();
    fileSystem = routerContext.getFileSystem();
    assertThrows(AssertionError.class, this::internalTestObserverRead);
  }

  public void internalTestObserverRead()
      throws Exception {
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/testFile");
    // Send create call
    fileSystem.create(path).close();

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create and complete calls should be sent to active
    assertEquals("Two calls should be sent to active", 2, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    // getBlockLocations should be sent to observer
    assertEquals("One call should be sent to observer", 1, rpcCountForObserver);
  }

  @Test
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testObserverReadMultiNameservice() throws Exception {
    Configuration confOverrides = new Configuration(false);
    confOverrides.setInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_FEDERATED_STATE_PROPAGATION_MAXSIZE, 1);
    startUpCluster(1, confOverrides);
    RouterContext routerContext = cluster.getRandomRouter();
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);

    // 1 Create or open file in ns0
    Configuration clientConf = new Configuration();
    clientConf.setBoolean("fs.hdfs.impl.disable.cache", true);
    clientConf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + FEDERATION_NS,
        ObserverReadProxyProvider.class.getCanonicalName());
    FileSystem fs0 = cluster.getRBFFileSystem(FEDERATION_NS, clientConf);
    Path path0 = new Path("/ns0/testFile");
    // Send msync, should ignore it.
    fs0.msync();

    // Send Create call to active
    fs0.create(path0).close();

    // Send read request to observer
    fs0.open(path0).close();

    // Send msync to active, only connect ns0 active
    fs0.msync();

    // 2 Create or open file in ns1
    FileSystem fs1 = cluster.getRBFFileSystem(FEDERATION_NS, clientConf);
    // Send msync, router should ignore it.
    fs1.msync();

    Path path1 = new Path("/ns1/testFile");
    // Send msync, should ignore it.
    fs1.msync();

    // Send Create call to active
    fs1.create(path1).close();

    // Send read request to observer
    fs1.open(path1).close();

    // Send msync to active, only connect ns1 active
    fs1.msync();

    // Create and complete and second msync calls should be sent to active
    long rpcCountForActive = cluster.getRouters().stream()
        .map(s -> s.getRouter().getRpcServer().getRPCMetrics().getActiveProxyOps())
        .reduce(0L, Long::sum);
    assertEquals("Six calls should be sent to active", 6, rpcCountForActive);

    // getBlockLocations should be sent to observer
    long rpcCountForObserver = cluster.getRouters().stream()
        .map(s -> s.getRouter().getRpcServer().getRPCMetrics().getObserverProxyOps())
        .reduce(0L, Long::sum);
    assertEquals("Two call should be sent to observer", 2, rpcCountForObserver);

    fs0.close();
    fs1.close();
  }

  @Test
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testObserverReadWithoutFederatedStatePropagation() throws Exception {
    Configuration confOverrides = new Configuration(false);
    confOverrides.setInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_FEDERATED_STATE_PROPAGATION_MAXSIZE, 0);
    startUpCluster(2, confOverrides);
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer", namenodes.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    Path path = new Path("/testFile");
    // Send Create call to active
    fileSystem.create(path).close();

    // Send read request to observer. The router will msync to the active namenode.
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create, complete and getBlockLocations calls should be sent to active
    assertEquals("Three calls should be sent to active", 3, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should be sent to observer", 0, rpcCountForObserver);
  }

  @Test
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testDisablingObserverReadUsingNameserviceOverride() throws Exception {
    // Disable observer reads using per-nameservice override
    Configuration confOverrides = new Configuration(false);
    confOverrides.set(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_OVERRIDES, "ns0");
    startUpCluster(2, confOverrides);

    Path path = new Path("/testFile");
    fileSystem.create(path).close();
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create, complete and read calls should be sent to active
    assertEquals("Three calls should be sent to active", 3, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    assertEquals("Zero calls should be sent to observer", 0, rpcCountForObserver);
  }

  @Test
  public void testReadWhenObserverIsDown() throws Exception {
    Path path = new Path("/testFile1");
    // Send Create call to active
    fileSystem.create(path).close();

    // Stop observer NN
    int nnIndex = stopObserver(1);
    assertNotEquals("No observer found", 3, nnIndex);
    nnIndex = stopObserver(1);
    assertNotEquals("No observer found", 4, nnIndex);

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create, complete and getBlockLocation calls should be sent to active
    assertEquals("Three calls should be sent to active", 3,
        rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should send to observer", 0,
        rpcCountForObserver);
  }

  @Test
  public void testMultipleObserver() throws Exception {
    Path path = new Path("/testFile1");
    // Send Create call to active
    fileSystem.create(path).close();

    // Stop one observer NN
    stopObserver(1);

    // Send read request
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    long expectedActiveRpc = 2;
    long expectedObserverRpc = 1;

    // Create and complete calls should be sent to active
    assertEquals("Two calls should be sent to active",
        expectedActiveRpc, rpcCountForActive);

    long rpcCountForObserver = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getObserverProxyOps();
    // getBlockLocation call should send to observer
    assertEquals("Read should be success with another observer",
        expectedObserverRpc, rpcCountForObserver);

    // Stop one observer NN
    stopObserver(1);

    // Send read request
    fileSystem.open(path).close();

    rpcCountForActive = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getActiveProxyOps();

    // getBlockLocation call should be sent to active
    expectedActiveRpc += 1;
    assertEquals("One call should be sent to active", expectedActiveRpc,
        rpcCountForActive);
    expectedObserverRpc += 0;
    rpcCountForObserver = routerContext.getRouter()
        .getRpcServer().getRPCMetrics().getObserverProxyOps();
    assertEquals("No call should send to observer",
        expectedObserverRpc, rpcCountForObserver);
  }

  private int stopObserver(int num) {
    int nnIndex;
    for (nnIndex = 0; nnIndex < cluster.getNamenodes().size(); nnIndex++) {
      NameNode nameNode = cluster.getCluster().getNameNode(nnIndex);
      if (nameNode != null && nameNode.isObserverState()) {
        cluster.getCluster().shutdownNameNode(nnIndex);
        num--;
        if (num == 0) {
          break;
        }
      }
    }
    return nnIndex;
  }

  // test router observer with multiple to know which observer NN received
  // requests
  @Test
  @Tag(SKIP_BEFORE_EACH_CLUSTER_STARTUP)
  public void testMultipleObserverRouter() throws Exception {
    StateStoreDFSCluster innerCluster;
    MembershipNamenodeResolver resolver;

    String ns0;
    String ns1;
    //create 4NN, One Active One Standby and Two Observers
    innerCluster = new StateStoreDFSCluster(true, 4, 4, TimeUnit.SECONDS.toMillis(5),
        TimeUnit.SECONDS.toMillis(5));
    Configuration routerConf =
        new RouterConfigBuilder().stateStore().admin().rpc()
            .enableLocalHeartbeat(true).heartbeat().build();

    StringBuilder sb = new StringBuilder();
    ns0 = innerCluster.getNameservices().get(0);
    MiniRouterDFSCluster.NamenodeContext context =
        innerCluster.getNamenodes(ns0).get(1);
    routerConf.set(DFS_NAMESERVICE_ID, ns0);
    routerConf.set(DFS_HA_NAMENODE_ID_KEY, context.getNamenodeId());

    // Specify namenodes (ns1.nn0,ns1.nn1) to monitor
    ns1 = innerCluster.getNameservices().get(1);
    for (MiniRouterDFSCluster.NamenodeContext ctx : innerCluster.getNamenodes(ns1)) {
      String suffix = ctx.getConfSuffix();
      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(suffix);
    }
    routerConf.set(DFS_ROUTER_MONITOR_NAMENODE, sb.toString());
    routerConf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_DEFAULT_KEY, true);
    routerConf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    routerConf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "0ms");

    innerCluster.addNamenodeOverrides(routerConf);
    innerCluster.addRouterOverrides(routerConf);
    innerCluster.startCluster();

    if (innerCluster.isHighAvailability()) {
      for (String ns : innerCluster.getNameservices()) {
        innerCluster.switchToActive(ns, NAMENODES[0]);
        innerCluster.switchToStandby(ns, NAMENODES[1]);
        for (int i = 2; i < 4; i++) {
          innerCluster.switchToObserver(ns, NAMENODES[i]);
        }
      }
    }
    innerCluster.startRouters();
    innerCluster.waitClusterUp();

    routerContext = innerCluster.getRandomRouter();
    resolver = (MembershipNamenodeResolver) routerContext.getRouter()
        .getNamenodeResolver();

    resolver.loadCache(true);
    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId(ns0, true);
    List<? extends FederationNamenodeContext> namespaceInfo1 =
        resolver.getNamenodesForNameserviceId(ns1, true);
    assertEquals(namespaceInfo0.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);
    assertEquals(namespaceInfo0.get(1).getState(),
        FederationNamenodeServiceState.OBSERVER);
    assertNotEquals(namespaceInfo0.get(0).getNamenodeId(),
        namespaceInfo0.get(1).getNamenodeId());
    assertEquals(namespaceInfo1.get(0).getState(),
        FederationNamenodeServiceState.OBSERVER);

    innerCluster.shutdown();
  }

  @Test
  public void testUnavailableObserverNN() throws Exception {
    stopObserver(2);

    Path path = new Path("/testFile");
    // Send Create call to active
    fileSystem.create(path).close();

    // Send read request.
    fileSystem.open(path).close();

    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();

    // Create, complete and getBlockLocations
    // calls should be sent to active.
    assertEquals("Three calls should be send to active",
        3, rpcCountForActive);


    boolean hasUnavailable = false;
    for(String ns : cluster.getNameservices()) {
      List<? extends FederationNamenodeContext> nns = routerContext.getRouter()
          .getNamenodeResolver().getNamenodesForNameserviceId(ns, false);
      for(FederationNamenodeContext nn : nns) {
        if(FederationNamenodeServiceState.UNAVAILABLE == nn.getState()) {
          hasUnavailable = true;
        }
      }
    }
    // After attempting to communicate with unavailable observer namenode,
    // its state is updated to unavailable.
    assertTrue("There must be unavailable namenodes", hasUnavailable);
  }



  @Test
  public void testRouterMsync() throws Exception {
    Path path = new Path("/testFile");

    // Send Create call to active
    fileSystem.create(path).close();
    long rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // Create and complete calls should be sent to active
    assertEquals("Two calls should be sent to active", 2,
        rpcCountForActive);

    // Send msync
    fileSystem.msync();
    rpcCountForActive = routerContext.getRouter().getRpcServer()
        .getRPCMetrics().getActiveProxyOps();
    // 1 msync calls should be sent.
    assertEquals("Four calls should be sent to active", 3, rpcCountForActive);
  }
}