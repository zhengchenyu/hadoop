/*
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
package org.apache.hadoop.yarn.server.router;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * This class is for maintaining the various Router Federation Interceptor
 * activity statistics and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about = "Metrics for Router Federation Interceptor", context = "fedr")
public final class RouterMetrics {

  private static final MetricsInfo RECORD_INFO =
      info("RouterMetrics", "Router Federation Interceptor");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  // Metrics for operation failed
  @Metric("# of applications failed to be submitted")
  private MutableGaugeInt numAppsFailedSubmitted;
  @Metric("# of applications failed to be created")
  private MutableGaugeInt numAppsFailedCreated;
  @Metric("# of applications failed to be killed")
  private MutableGaugeInt numAppsFailedKilled;
  @Metric("# of application reports failed to be retrieved")
  private MutableGaugeInt numAppsFailedRetrieved;
  @Metric("# of multiple applications reports failed to be retrieved")
  private MutableGaugeInt numMultipleAppsFailedRetrieved;
  @Metric("# of getApplicationAttempts failed to be retrieved")
  private MutableGaugeInt numAppAttemptsFailedRetrieved;
  @Metric("# of getClusterMetrics failed to be retrieved")
  private MutableGaugeInt numGetClusterMetricsFailedRetrieved;
  @Metric("# of getClusterNodes failed to be retrieved")
  private MutableGaugeInt numGetClusterNodesFailedRetrieved;
  @Metric("# of getNodeToLabels failed to be retrieved")
  private MutableGaugeInt numGetNodeToLabelsFailedRetrieved;
  @Metric("# of getNodeToLabels failed to be retrieved")
  private MutableGaugeInt numGetLabelsToNodesFailedRetrieved;
  @Metric("# of getClusterNodeLabels failed to be retrieved")
  private MutableGaugeInt numGetClusterNodeLabelsFailedRetrieved;
  @Metric("# of getApplicationAttemptReports failed to be retrieved")
  private MutableGaugeInt numAppAttemptReportFailedRetrieved;
  @Metric("# of getQueueUserAcls failed to be retrieved")
  private MutableGaugeInt numGetQueueUserAclsFailedRetrieved;
  @Metric("# of getContainerReport failed to be retrieved")
  private MutableGaugeInt numGetContainerReportFailedRetrieved;
  @Metric("# of getContainers failed to be retrieved")
  private MutableGaugeInt numGetContainersFailedRetrieved;
  @Metric("# of getContainers failed to be retrieved")
  private MutableGaugeInt numListReservationsFailedRetrieved;
  @Metric("# of getResourceTypeInfo failed to be retrieved")
  private MutableGaugeInt numGetResourceTypeInfo;
  @Metric("# of failApplicationAttempt failed to be retrieved")
  private MutableGaugeInt numFailAppAttemptFailedRetrieved;
  @Metric("# of updateApplicationPriority failed to be retrieved")
  private MutableGaugeInt numUpdateAppPriorityFailedRetrieved;
  @Metric("# of updateApplicationPriority failed to be retrieved")
  private MutableGaugeInt numUpdateAppTimeoutsFailedRetrieved;
  @Metric("# of signalToContainer failed to be retrieved")
  private MutableGaugeInt numSignalToContainerFailedRetrieved;

  // Aggregate metrics are shared, and don't have to be looked up per call
  @Metric("Total number of successful Submitted apps and latency(ms)")
  private MutableRate totalSucceededAppsSubmitted;
  @Metric("Total number of successful Killed apps and latency(ms)")
  private MutableRate totalSucceededAppsKilled;
  @Metric("Total number of successful Created apps and latency(ms)")
  private MutableRate totalSucceededAppsCreated;
  @Metric("Total number of successful Retrieved app reports and latency(ms)")
  private MutableRate totalSucceededAppsRetrieved;
  @Metric("Total number of successful Retrieved multiple apps reports and "
      + "latency(ms)")
  private MutableRate totalSucceededMultipleAppsRetrieved;
  @Metric("Total number of successful Retrieved " +
          "appAttempt reports and latency(ms)")
  private MutableRate totalSucceededAppAttemptsRetrieved;
  @Metric("Total number of successful Retrieved getClusterMetrics and "
      + "latency(ms)")
  private MutableRate totalSucceededGetClusterMetricsRetrieved;
  @Metric("Total number of successful Retrieved getClusterNodes and latency(ms)")
  private MutableRate totalSucceededGetClusterNodesRetrieved;
  @Metric("Total number of successful Retrieved getNodeToLabels and latency(ms)")
  private MutableRate totalSucceededGetNodeToLabelsRetrieved;
  @Metric("Total number of successful Retrieved getNodeToLabels and latency(ms)")
  private MutableRate totalSucceededGetLabelsToNodesRetrieved;
  @Metric("Total number of successful Retrieved getClusterNodeLabels and latency(ms)")
  private MutableRate totalSucceededGetClusterNodeLabelsRetrieved;
  @Metric("Total number of successful Retrieved getApplicationAttemptReport and latency(ms)")
  private MutableRate totalSucceededAppAttemptReportRetrieved;
  @Metric("Total number of successful Retrieved getQueueUserAcls and latency(ms)")
  private MutableRate totalSucceededGetQueueUserAclsRetrieved;
  @Metric("Total number of successful Retrieved getContainerReport and latency(ms)")
  private MutableRate totalSucceededGetContainerReportRetrieved;
  @Metric("Total number of successful Retrieved getContainers and latency(ms)")
  private MutableRate totalSucceededGetContainersRetrieved;
  @Metric("Total number of successful Retrieved listReservations and latency(ms)")
  private MutableRate totalSucceededListReservationsRetrieved;
  @Metric("Total number of successful Retrieved getResourceTypeInfo and latency(ms)")
  private MutableRate totalSucceededGetResourceTypeInfoRetrieved;
  @Metric("Total number of successful Retrieved failApplicationAttempt and latency(ms)")
  private MutableRate totalSucceededFailAppAttemptRetrieved;
  @Metric("Total number of successful Retrieved updateApplicationPriority and latency(ms)")
  private MutableRate totalSucceededUpdateAppPriorityRetrieved;
  @Metric("Total number of successful Retrieved updateApplicationTimeouts and latency(ms)")
  private MutableRate totalSucceededUpdateAppTimeoutsRetrieved;
  @Metric("Total number of successful Retrieved signalToContainer and latency(ms)")
  private MutableRate totalSucceededSignalToContainerRetrieved;

  /**
   * Provide quantile counters for all latencies.
   */
  private MutableQuantiles submitApplicationLatency;
  private MutableQuantiles getNewApplicationLatency;
  private MutableQuantiles killApplicationLatency;
  private MutableQuantiles getApplicationReportLatency;
  private MutableQuantiles getApplicationsReportLatency;
  private MutableQuantiles getApplicationAttemptReportLatency;
  private MutableQuantiles getClusterMetricsLatency;
  private MutableQuantiles getClusterNodesLatency;
  private MutableQuantiles getNodeToLabelsLatency;
  private MutableQuantiles getLabelToNodesLatency;
  private MutableQuantiles getClusterNodeLabelsLatency;
  private MutableQuantiles getApplicationAttemptsLatency;
  private MutableQuantiles getQueueUserAclsLatency;
  private MutableQuantiles getContainerReportLatency;
  private MutableQuantiles getContainerLatency;
  private MutableQuantiles listReservationsLatency;
  private MutableQuantiles listResourceTypeInfoLatency;
  private MutableQuantiles failAppAttemptLatency;
  private MutableQuantiles updateAppPriorityLatency;
  private MutableQuantiles updateAppTimeoutsLatency;
  private MutableQuantiles signalToContainerLatency;

  private static volatile RouterMetrics instance = null;
  private static MetricsRegistry registry;

  private RouterMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "Router");
    getNewApplicationLatency = registry.newQuantiles("getNewApplicationLatency",
        "latency of get new application", "ops", "latency", 10);
    submitApplicationLatency = registry.newQuantiles("submitApplicationLatency",
        "latency of submit application", "ops", "latency", 10);
    killApplicationLatency = registry.newQuantiles("killApplicationLatency",
        "latency of kill application", "ops", "latency", 10);
    getApplicationReportLatency =
        registry.newQuantiles("getApplicationReportLatency",
            "latency of get application report", "ops", "latency", 10);
    getApplicationsReportLatency =
        registry.newQuantiles("getApplicationsReportLatency",
            "latency of get applications report", "ops", "latency", 10);
    getApplicationAttemptReportLatency =
        registry.newQuantiles("getApplicationAttemptReportLatency",
                    "latency of get applicationattempt " +
                            "report", "ops", "latency", 10);
    getClusterMetricsLatency =
        registry.newQuantiles("getClusterMetricsLatency",
            "latency of get cluster metrics", "ops", "latency", 10);

    getClusterNodesLatency =
        registry.newQuantiles("getClusterNodesLatency",
            "latency of get cluster nodes", "ops", "latency", 10);

    getNodeToLabelsLatency =
        registry.newQuantiles("getNodeToLabelsLatency",
            "latency of get node labels", "ops", "latency", 10);

    getLabelToNodesLatency =
        registry.newQuantiles("getLabelToNodesLatency",
            "latency of get label nodes", "ops", "latency", 10);

    getClusterNodeLabelsLatency =
        registry.newQuantiles("getClusterNodeLabelsLatency",
            "latency of get cluster node labels", "ops", "latency", 10);

    getApplicationAttemptsLatency =
        registry.newQuantiles("getApplicationAttemptsLatency",
            "latency of get application attempts", "ops", "latency", 10);

    getQueueUserAclsLatency =
        registry.newQuantiles("getQueueUserAclsLatency",
            "latency of get queue user acls", "ops", "latency", 10);

    getContainerReportLatency =
        registry.newQuantiles("getContainerReportLatency",
            "latency of get container report", "ops", "latency", 10);

    getContainerLatency =
        registry.newQuantiles("getContainerLatency",
            "latency of get container", "ops", "latency", 10);

    listReservationsLatency =
        registry.newQuantiles("listReservationsLatency",
            "latency of list reservations", "ops", "latency", 10);

    listResourceTypeInfoLatency =
        registry.newQuantiles("getResourceTypeInfoLatency",
            "latency of get resource type info", "ops", "latency", 10);

    failAppAttemptLatency =
        registry.newQuantiles("failApplicationAttemptLatency",
            "latency of fail application attempt", "ops", "latency", 10);

    updateAppPriorityLatency =
        registry.newQuantiles("updateApplicationPriorityLatency",
            "latency of update application priority", "ops", "latency", 10);

    updateAppTimeoutsLatency =
        registry.newQuantiles("updateApplicationTimeoutsLatency",
            "latency of update application timeouts", "ops", "latency", 10);

    signalToContainerLatency =
        registry.newQuantiles("signalToContainerLatency",
            "latency of signal to container timeouts", "ops", "latency", 10);
  }

  public static RouterMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (RouterMetrics.class) {
        if (instance == null) {
          instance = DefaultMetricsSystem.instance().register("RouterMetrics",
              "Metrics for the Yarn Router", new RouterMetrics());
          isInitialized.set(true);
        }
      }
    }
    return instance;
  }

  @VisibleForTesting
  synchronized static void destroy() {
    isInitialized.set(false);
    instance = null;
  }

  @VisibleForTesting
  public long getNumSucceededAppsCreated() {
    return totalSucceededAppsCreated.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsSubmitted() {
    return totalSucceededAppsSubmitted.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsKilled() {
    return totalSucceededAppsKilled.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsRetrieved() {
    return totalSucceededAppsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppAttemptsRetrieved() {
    return totalSucceededAppAttemptsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededMultipleAppsRetrieved() {
    return totalSucceededMultipleAppsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterMetricsRetrieved(){
    return totalSucceededGetClusterMetricsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterNodesRetrieved(){
    return totalSucceededGetClusterNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetNodeToLabelsRetrieved(){
    return totalSucceededGetNodeToLabelsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetLabelsToNodesRetrieved(){
    return totalSucceededGetLabelsToNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterNodeLabelsRetrieved(){
    return totalSucceededGetClusterNodeLabelsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppAttemptReportRetrieved(){
    return totalSucceededAppAttemptReportRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetQueueUserAclsRetrieved(){
    return totalSucceededGetQueueUserAclsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetContainerReportRetrieved() {
    return totalSucceededGetContainerReportRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetContainersRetrieved() {
    return totalSucceededGetContainersRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededListReservationsRetrieved() {
    return totalSucceededListReservationsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetResourceTypeInfoRetrieved() {
    return totalSucceededGetResourceTypeInfoRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededFailAppAttemptRetrieved() {
    return totalSucceededFailAppAttemptRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededUpdateAppPriorityRetrieved() {
    return totalSucceededUpdateAppPriorityRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededUpdateAppTimeoutsRetrieved() {
    return totalSucceededUpdateAppTimeoutsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededSignalToContainerRetrieved() {
    return totalSucceededSignalToContainerRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsCreated() {
    return totalSucceededAppsCreated.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsSubmitted() {
    return totalSucceededAppsSubmitted.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsKilled() {
    return totalSucceededAppsKilled.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppAttemptReport() {
    return totalSucceededAppAttemptReportRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppReport() {
    return totalSucceededAppsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededMultipleGetAppReport() {
    return totalSucceededMultipleAppsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterMetricsRetrieved() {
    return totalSucceededGetClusterMetricsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterNodesRetrieved() {
    return totalSucceededGetClusterNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetNodeToLabelsRetrieved() {
    return totalSucceededGetNodeToLabelsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetLabelsToNodesRetrieved() {
    return totalSucceededGetLabelsToNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterNodeLabelsRetrieved() {
    return totalSucceededGetClusterNodeLabelsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAppAttemptRetrieved() {
    return totalSucceededAppAttemptsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetQueueUserAclsRetrieved() {
    return totalSucceededGetQueueUserAclsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetContainerReportRetrieved() {
    return totalSucceededGetContainerReportRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetContainersRetrieved() {
    return totalSucceededGetContainersRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededListReservationsRetrieved() {
    return totalSucceededListReservationsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetResourceTypeInfoRetrieved() {
    return totalSucceededGetResourceTypeInfoRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededFailAppAttemptRetrieved() {
    return totalSucceededFailAppAttemptRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededUpdateAppPriorityRetrieved() {
    return totalSucceededUpdateAppPriorityRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededUpdateAppTimeoutsRetrieved() {
    return totalSucceededUpdateAppTimeoutsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededSignalToContainerRetrieved() {
    return totalSucceededSignalToContainerRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public int getAppsFailedCreated() {
    return numAppsFailedCreated.value();
  }

  @VisibleForTesting
  public int getAppsFailedSubmitted() {
    return numAppsFailedSubmitted.value();
  }

  @VisibleForTesting
  public int getAppsFailedKilled() {
    return numAppsFailedKilled.value();
  }

  @VisibleForTesting
  public int getAppsFailedRetrieved() {
    return numAppsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getAppAttemptsFailedRetrieved() {
    return numAppAttemptsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getMultipleAppsFailedRetrieved() {
    return numMultipleAppsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getClusterMetricsFailedRetrieved() {
    return numGetClusterMetricsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getClusterNodesFailedRetrieved() {
    return numGetClusterNodesFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getNodeToLabelsFailedRetrieved() {
    return numGetNodeToLabelsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getLabelsToNodesFailedRetrieved() {
    return numGetLabelsToNodesFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getGetClusterNodeLabelsFailedRetrieved() {
    return numGetClusterNodeLabelsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getAppAttemptReportFailedRetrieved() {
    return numAppAttemptReportFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getQueueUserAclsFailedRetrieved() {
    return numGetQueueUserAclsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getContainerReportFailedRetrieved() {
    return numGetContainerReportFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getContainersFailedRetrieved() {
    return numGetContainersFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getListReservationsFailedRetrieved() {
    return numListReservationsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getGetResourceTypeInfoRetrieved() {
    return numGetResourceTypeInfo.value();
  }

  @VisibleForTesting
  public int getFailApplicationAttemptFailedRetrieved() {
    return numFailAppAttemptFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getUpdateApplicationPriorityFailedRetrieved() {
    return numUpdateAppPriorityFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getUpdateApplicationTimeoutsFailedRetrieved() {
    return numUpdateAppTimeoutsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getSignalToContainerFailedRetrieved() {
    return numSignalToContainerFailedRetrieved.value();
  }

  public void succeededAppsCreated(long duration) {
    totalSucceededAppsCreated.add(duration);
    getNewApplicationLatency.add(duration);
  }

  public void succeededAppsSubmitted(long duration) {
    totalSucceededAppsSubmitted.add(duration);
    submitApplicationLatency.add(duration);
  }

  public void succeededAppsKilled(long duration) {
    totalSucceededAppsKilled.add(duration);
    killApplicationLatency.add(duration);
  }

  public void succeededAppsRetrieved(long duration) {
    totalSucceededAppsRetrieved.add(duration);
    getApplicationReportLatency.add(duration);
  }

  public void succeededMultipleAppsRetrieved(long duration) {
    totalSucceededMultipleAppsRetrieved.add(duration);
    getApplicationsReportLatency.add(duration);
  }

  public void succeededAppAttemptsRetrieved(long duration) {
    totalSucceededAppAttemptsRetrieved.add(duration);
    getApplicationAttemptsLatency.add(duration);
  }

  public void succeededGetClusterMetricsRetrieved(long duration) {
    totalSucceededGetClusterMetricsRetrieved.add(duration);
    getClusterMetricsLatency.add(duration);
  }

  public void succeededGetClusterNodesRetrieved(long duration) {
    totalSucceededGetClusterNodesRetrieved.add(duration);
    getClusterNodesLatency.add(duration);
  }

  public void succeededGetNodeToLabelsRetrieved(long duration) {
    totalSucceededGetNodeToLabelsRetrieved.add(duration);
    getNodeToLabelsLatency.add(duration);
  }

  public void succeededGetLabelsToNodesRetrieved(long duration) {
    totalSucceededGetLabelsToNodesRetrieved.add(duration);
    getLabelToNodesLatency.add(duration);
  }

  public void succeededGetClusterNodeLabelsRetrieved(long duration) {
    totalSucceededGetClusterNodeLabelsRetrieved.add(duration);
    getClusterNodeLabelsLatency.add(duration);
  }

  public void succeededAppAttemptReportRetrieved(long duration) {
    totalSucceededAppAttemptReportRetrieved.add(duration);
    getApplicationAttemptReportLatency.add(duration);
  }

  public void succeededGetQueueUserAclsRetrieved(long duration) {
    totalSucceededGetQueueUserAclsRetrieved.add(duration);
    getQueueUserAclsLatency.add(duration);
  }

  public void succeededGetContainerReportRetrieved(long duration) {
    totalSucceededGetContainerReportRetrieved.add(duration);
    getContainerReportLatency.add(duration);
  }

  public void succeededGetContainersRetrieved(long duration) {
    totalSucceededGetContainersRetrieved.add(duration);
    getContainerLatency.add(duration);
  }

  public void succeededListReservationsRetrieved(long duration) {
    totalSucceededListReservationsRetrieved.add(duration);
    listReservationsLatency.add(duration);
  }

  public void succeededGetResourceTypeInfoRetrieved(long duration) {
    totalSucceededGetResourceTypeInfoRetrieved.add(duration);
    listResourceTypeInfoLatency.add(duration);
  }

  public void succeededFailAppAttemptRetrieved(long duration) {
    totalSucceededFailAppAttemptRetrieved.add(duration);
    failAppAttemptLatency.add(duration);
  }

  public void succeededUpdateAppPriorityRetrieved(long duration) {
    totalSucceededUpdateAppPriorityRetrieved.add(duration);
    updateAppPriorityLatency.add(duration);
  }

  public void succeededUpdateAppTimeoutsRetrieved(long duration) {
    totalSucceededUpdateAppTimeoutsRetrieved.add(duration);
    updateAppTimeoutsLatency.add(duration);
  }

  public void succeededSignalToContainerRetrieved(long duration) {
    totalSucceededSignalToContainerRetrieved.add(duration);
    signalToContainerLatency.add(duration);
  }

  public void incrAppsFailedCreated() {
    numAppsFailedCreated.incr();
  }

  public void incrAppsFailedSubmitted() {
    numAppsFailedSubmitted.incr();
  }

  public void incrAppsFailedKilled() {
    numAppsFailedKilled.incr();
  }

  public void incrAppsFailedRetrieved() {
    numAppsFailedRetrieved.incr();
  }

  public void incrMultipleAppsFailedRetrieved() {
    numMultipleAppsFailedRetrieved.incr();
  }

  public void incrAppAttemptsFailedRetrieved() {
    numAppAttemptsFailedRetrieved.incr();
  }

  public void incrGetClusterMetricsFailedRetrieved() {
    numGetClusterMetricsFailedRetrieved.incr();
  }

  public void incrClusterNodesFailedRetrieved() {
    numGetClusterNodesFailedRetrieved.incr();
  }

  public void incrNodeToLabelsFailedRetrieved() {
    numGetNodeToLabelsFailedRetrieved.incr();
  }

  public void incrLabelsToNodesFailedRetrieved() {
    numGetLabelsToNodesFailedRetrieved.incr();
  }

  public void incrClusterNodeLabelsFailedRetrieved() {
    numGetClusterNodeLabelsFailedRetrieved.incr();
  }

  public void incrAppAttemptReportFailedRetrieved() {
    numAppAttemptReportFailedRetrieved.incr();
  }

  public void incrQueueUserAclsFailedRetrieved() {
    numGetQueueUserAclsFailedRetrieved.incr();
  }

  public void incrContainerReportFailedRetrieved() {
    numGetContainerReportFailedRetrieved.incr();
  }

  public void incrContainerFailedRetrieved() {
    numGetContainersFailedRetrieved.incr();
  }

  public void incrListReservationsFailedRetrieved() {
    numListReservationsFailedRetrieved.incr();
  }

  public void incrResourceTypeInfoFailedRetrieved() {
    numGetResourceTypeInfo.incr();
  }

  public void incrFailAppAttemptFailedRetrieved() {
    numFailAppAttemptFailedRetrieved.incr();
  }

  public void incrUpdateAppPriorityFailedRetrieved() {
    numUpdateAppPriorityFailedRetrieved.incr();
  }

  public void incrUpdateApplicationTimeoutsRetrieved() {
    numUpdateAppTimeoutsFailedRetrieved.incr();
  }

  public void incrSignalToContainerFailedRetrieved() {
    numSignalToContainerFailedRetrieved.incr();
  }
}
