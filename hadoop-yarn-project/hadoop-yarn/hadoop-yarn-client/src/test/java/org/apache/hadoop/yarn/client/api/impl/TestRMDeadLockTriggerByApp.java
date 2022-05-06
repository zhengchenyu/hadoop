package org.apache.hadoop.yarn.client.api.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRMDeadLockTriggerByApp {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestRMDeadLockTriggerByApp.class);

  int interval = 1;
  int loop = 5000;
  float checkDeadLockRatio = 2.0f;
  boolean deadLock = false;
  String errString = null;

  Configuration conf = null;
  MiniYARNCluster yarnCluster = null;

  List<NodeReport> nodeReports = null;
  ApplicationId appId = null;
  ApplicationAttemptId attemptId = null;

  YarnClient yarnClient = null;
  AMRMClient<ContainerRequest> amClient = null;

  ResourceManager rm;
  NodeManager nm;
  int nodeCount = 1;

  @Before
  public void setup() throws Exception {
    conf = new YarnConfiguration();
    createClusterAndStartApplication(conf);
  }

  void createClusterAndStartApplication(Configuration conf)
      throws Exception {
    // start minicluster
    this.conf = conf;
    conf.set(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class.getName());
    conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    conf.setInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH, 10);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.setInt(YarnConfiguration.NM_VCORES, loop);
    conf.setInt(YarnConfiguration.YARN_MINICLUSTER_NM_PMEM_MB, 512 * loop);
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, this.interval);

    yarnCluster = new MiniYARNCluster(
        TestAMRMClient.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    // start rm client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // get node info
    assertTrue("All node managers did not connect to the RM within the "
            + "allotted 5-second timeout",
        yarnCluster.waitForNodeManagersToConnect(5000L));
    nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    assertEquals("Not all node managers were reported running",
        nodeCount, nodeReports.size());

    // get rm and nm info
    rm = yarnCluster.getResourceManager(0);
    nm = yarnCluster.getNodeManager(0);

    // submit new app
    ApplicationSubmissionContext appContext =
        yarnClient.createApplication().getApplicationSubmissionContext();
    appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
        BuilderUtils.newContainerLaunchContext(
            Collections.<String, LocalResource>emptyMap(),
            new HashMap<String, String>(), Arrays.asList("sleep", "100"),
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(Resource.newInstance(1024, 1));
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);

    // wait for app to start
    RMAppAttempt appAttempt = null;
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() ==
          YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        appAttempt =
            yarnCluster.getResourceManager().getRMContext().getRMApps()
                .get(attemptId.getApplicationId()).getCurrentAppAttempt();
        while (true) {
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            break;
          }
        }
        break;
      }
    }
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));

    // emulate RM setup of AMRM token in credentials by adding the token
    // *before* setting the token service
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());
    appAttempt.getAMRMToken().setService(
        ClientRMProxy.getAMRMTokenService(conf));

    // create AMRMClient
    amClient = AMRMClient.createAMRMClient();
    amClient.init(conf);
    amClient.start();
    amClient.registerApplicationMaster("Host", 10000, "");
  }

  @After
  public void teardown() throws YarnException, IOException {
    if (yarnClient != null && yarnClient.getServiceState() == STATE.STARTED) {
      yarnClient.stop();
    }

    if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
      amClient.stop();
    }

    // Avoid the EventHandlingThread struck forever
    if (deadLock) {
      LOG.info("Found dead lock, stop EventHandlingThread manually!");
      ((AsyncDispatcher) rm.getRMContext().getDispatcher())
          .forceEventHandlingThreadStop();
    }
    if (yarnCluster != null && yarnCluster.getServiceState() == STATE.STARTED) {
      yarnCluster.stop();
    }
  }

  @Test(timeout = 60000)
  public void TestRMDeadLockTriggerByApp() throws InterruptedException {

    // thread for allocate container
    Thread allocateThread = new AllocateTread();

    // thread for add log aggregation report
    Thread addLogAggReportThread = new AddLogAggregationReportThread();

    // thread for get application report
    Thread getAppReportThread = new GetApplicationReportThread();

    // start all thread
    allocateThread.start();
    addLogAggReportThread.start();
    getAppReportThread.start();

    this.deadLock = checkAsyncDispatcherDeadLock();
    Assert.assertFalse("There is dead lock!", deadLock);

    // join all thread
    allocateThread.join();
    addLogAggReportThread.join();
    getAppReportThread.join();

    Assert.assertNull(errString);
  }

  private boolean checkAsyncDispatcherDeadLock() throws InterruptedException {
    Event lastEvent = null;
    Event currentEvent = null;
    int counter = 0;
    for (int i = 0; i < loop * checkDeadLockRatio; i++) {
      currentEvent = ((AsyncDispatcher) rm.getRmDispatcher()).getHeadEvent();
      if (currentEvent != null && (currentEvent == lastEvent)) {
        if (counter++ > loop * checkDeadLockRatio / 2) {
          return true;
        }
      } else {
        counter = 0;
        lastEvent = currentEvent;
      }
      Thread.sleep(interval);
    }
    return false;
  }

  class AllocateTread extends Thread {

    @Override
    public void run() {
      ContainerId amContainerId = rm.getRMContext().getRMApps().get(appId)
          .getAppAttempts().get(attemptId).getMasterContainer().getId();
      ContainerRequest request = setupContainerAskForRM();
      try {
        for (int i = 0; i < loop; i++) {
          amClient.addContainerRequest(request);
          for (ContainerId containerId : nm.getNMContext().getContainers()
              .keySet()) {
            // release all container except am container
            if (!amContainerId.equals(containerId)) {
              amClient.releaseAssignedContainer(containerId);
            }
          }
          amClient.allocate(0.1f);
          Thread.sleep(interval);
        }
      } catch (Throwable t) {
        errString = t.getMessage();
        return;
      }
    }
  }

  class AddLogAggregationReportThread extends Thread {

    @Override
    public void run() {
      LogAggregationReport report = LogAggregationReport
          .newInstance(appId, LogAggregationStatus.RUNNING, "");
      try {
        for (int i = 0; i < loop; i++) {
          if (nm.getNMContext().getLogAggregationStatusForApps().size() == 0) {
            nm.getNMContext().getLogAggregationStatusForApps().add(report);
          }
          Thread.sleep(interval);
        }
      } catch (Throwable t) {
        errString = t.getMessage();
        return;
      }
    }
  }

  class GetApplicationReportThread extends Thread {

    @Override
    public void run() {
      try {
        for (int i = 0; i < loop; i++) {
          yarnClient.getApplicationReport(appId);
          Thread.sleep(interval);
        }
      } catch (Throwable t) {
        errString = t.getMessage();
        return;
      }
    }
  }

  private ContainerRequest setupContainerAskForRM() {
    Priority pri = Priority.newInstance(1);
    ContainerRequest request = new ContainerRequest(
        Resource.newInstance(512, 1), null, null, pri, 0, true, null,
        ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED, true), "");
    return request;
  }
}
