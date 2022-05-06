package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;

public class RMAppLogAggregationStatusEvent extends RMAppEvent {

  private final NodeId node;
  private final LogAggregationReport report;

  public RMAppLogAggregationStatusEvent(ApplicationId appId, NodeId node,
      LogAggregationReport report) {
    super(appId, RMAppEventType.APP_LOG_AGG_STATUS_UPDATE);
    this.node = node;
    this.report = report;
  }

  public NodeId getNodeId() {
    return node;
  }

  public LogAggregationReport getReport() {
    return report;
  }
}
