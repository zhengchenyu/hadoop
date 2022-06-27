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
package org.apache.hadoop.yarn.server.router.clientrm;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Util class for Router Yarn client API calls.
 */
public final class RouterYarnClientUtils {

  private final static String PARTIAL_REPORT = "Partial Report ";

  private RouterYarnClientUtils() {

  }

  public static GetClusterMetricsResponse merge(
      Collection<GetClusterMetricsResponse> responses) {
    YarnClusterMetrics tmp = YarnClusterMetrics.newInstance(0);
    for (GetClusterMetricsResponse response : responses) {
      YarnClusterMetrics metrics = response.getClusterMetrics();
      tmp.setNumNodeManagers(
          tmp.getNumNodeManagers() + metrics.getNumNodeManagers());
      tmp.setNumActiveNodeManagers(
          tmp.getNumActiveNodeManagers() + metrics.getNumActiveNodeManagers());
      tmp.setNumDecommissionedNodeManagers(
          tmp.getNumDecommissionedNodeManagers() + metrics
              .getNumDecommissionedNodeManagers());
      tmp.setNumLostNodeManagers(
          tmp.getNumLostNodeManagers() + metrics.getNumLostNodeManagers());
      tmp.setNumRebootedNodeManagers(tmp.getNumRebootedNodeManagers() + metrics
          .getNumRebootedNodeManagers());
      tmp.setNumUnhealthyNodeManagers(
          tmp.getNumUnhealthyNodeManagers() + metrics
              .getNumUnhealthyNodeManagers());
    }
    return GetClusterMetricsResponse.newInstance(tmp);
  }

  /**
   * Merges a list of ApplicationReports grouping by ApplicationId.
   * Our current policy is to merge the application reports from the reachable
   * SubClusters.
   * @param responses a list of ApplicationResponse to merge
   * @param returnPartialResult if the merge ApplicationReports should contain
   * partial result or not
   * @return the merged ApplicationsResponse
   */
  public static GetApplicationsResponse mergeApplications(
      Collection<GetApplicationsResponse> responses,
      boolean returnPartialResult){
    Map<ApplicationId, ApplicationReport> federationAM = new HashMap<>();
    Map<ApplicationId, ApplicationReport> federationUAMSum = new HashMap<>();

    for (GetApplicationsResponse appResponse : responses){
      for (ApplicationReport appReport : appResponse.getApplicationList()){
        ApplicationId appId = appReport.getApplicationId();
        // Check if this ApplicationReport is an AM
        if (!appReport.isUnmanagedApp()) {
          // Insert in the list of AM
          federationAM.put(appId, appReport);
          // Check if there are any UAM found before
          if (federationUAMSum.containsKey(appId)) {
            // Merge the current AM with the found UAM
            mergeAMWithUAM(appReport, federationUAMSum.get(appId));
            // Remove the sum of the UAMs
            federationUAMSum.remove(appId);
          }
          // This ApplicationReport is an UAM
        } else if (federationAM.containsKey(appId)) {
          // Merge the current UAM with its own AM
          mergeAMWithUAM(federationAM.get(appId), appReport);
        } else if (federationUAMSum.containsKey(appId)) {
          // Merge the current UAM with its own UAM and update the list of UAM
          ApplicationReport mergedUAMReport =
              mergeUAMWithUAM(federationUAMSum.get(appId), appReport);
          federationUAMSum.put(appId, mergedUAMReport);
        } else {
          // Insert in the list of UAM
          federationUAMSum.put(appId, appReport);
        }
      }
    }
    // Check the remaining UAMs are depending or not from federation
    for (ApplicationReport appReport : federationUAMSum.values()) {
      if (mergeUamToReport(appReport.getName(), returnPartialResult)) {
        federationAM.put(appReport.getApplicationId(), appReport);
      }
    }

    return GetApplicationsResponse.newInstance(federationAM.values());
  }

  private static ApplicationReport mergeUAMWithUAM(ApplicationReport uam1,
      ApplicationReport uam2){
    uam1.setName(PARTIAL_REPORT + uam1.getApplicationId());
    mergeAMWithUAM(uam1, uam2);
    return uam1;
  }

  private static void mergeAMWithUAM(ApplicationReport am,
      ApplicationReport uam){
    ApplicationResourceUsageReport amResourceReport =
        am.getApplicationResourceUsageReport();

    ApplicationResourceUsageReport uamResourceReport =
        uam.getApplicationResourceUsageReport();

    if (amResourceReport == null) {
      am.setApplicationResourceUsageReport(uamResourceReport);
    } else if (uamResourceReport != null) {

      amResourceReport.setNumUsedContainers(
          amResourceReport.getNumUsedContainers() +
              uamResourceReport.getNumUsedContainers());

      amResourceReport.setNumReservedContainers(
          amResourceReport.getNumReservedContainers() +
              uamResourceReport.getNumReservedContainers());

      amResourceReport.setUsedResources(Resources.add(
          amResourceReport.getUsedResources(),
          uamResourceReport.getUsedResources()));

      amResourceReport.setReservedResources(Resources.add(
          amResourceReport.getReservedResources(),
          uamResourceReport.getReservedResources()));

      amResourceReport.setNeededResources(Resources.add(
          amResourceReport.getNeededResources(),
          uamResourceReport.getNeededResources()));

      amResourceReport.setMemorySeconds(
          amResourceReport.getMemorySeconds() +
              uamResourceReport.getMemorySeconds());

      amResourceReport.setVcoreSeconds(
          amResourceReport.getVcoreSeconds() +
              uamResourceReport.getVcoreSeconds());

      amResourceReport.setQueueUsagePercentage(
          amResourceReport.getQueueUsagePercentage() +
              uamResourceReport.getQueueUsagePercentage());

      amResourceReport.setClusterUsagePercentage(
          amResourceReport.getClusterUsagePercentage() +
              uamResourceReport.getClusterUsagePercentage());

      am.setApplicationResourceUsageReport(amResourceReport);
    }
  }

  /**
   * Returns whether or not to add an unmanaged application to the report.
   * @param appName Application Name
   * @param returnPartialResult if the merge ApplicationReports should contain
   * partial result or not
   */
  private static boolean mergeUamToReport(String appName,
      boolean returnPartialResult){
    if (returnPartialResult) {
      return true;
    }
    if (appName == null) {
      return false;
    }
    return !(appName.startsWith(UnmanagedApplicationManager.APP_NAME) ||
        appName.startsWith(PARTIAL_REPORT));
  }

  /**
   * Merges a list of GetClusterNodesResponse.
   *
   * @param responses a list of GetClusterNodesResponse to merge.
   * @return the merged GetClusterNodesResponse.
   */
  public static GetClusterNodesResponse mergeClusterNodesResponse(
      Collection<GetClusterNodesResponse> responses) {
    GetClusterNodesResponse clusterNodesResponse = Records.newRecord(GetClusterNodesResponse.class);
    List<NodeReport> nodeReports = new ArrayList<>();
    for (GetClusterNodesResponse response : responses) {
      if (response != null && response.getNodeReports() != null) {
        nodeReports.addAll(response.getNodeReports());
      }
    }
    clusterNodesResponse.setNodeReports(nodeReports);
    return clusterNodesResponse;
  }

  /**
   * Merges a list of GetNodesToLabelsResponse.
   *
   * @param responses a list of GetNodesToLabelsResponse to merge.
   * @return the merged GetNodesToLabelsResponse.
   */
  public static GetNodesToLabelsResponse mergeNodesToLabelsResponse(
      Collection<GetNodesToLabelsResponse> responses) {
    GetNodesToLabelsResponse nodesToLabelsResponse = Records.newRecord(
         GetNodesToLabelsResponse.class);
    Map<NodeId, Set<String>> nodesToLabelMap = new HashMap<>();
    for (GetNodesToLabelsResponse response : responses) {
      if (response != null && response.getNodeToLabels() != null) {
        nodesToLabelMap.putAll(response.getNodeToLabels());
      }
    }
    nodesToLabelsResponse.setNodeToLabels(nodesToLabelMap);
    return nodesToLabelsResponse;
  }

  /**
   * Merges a list of GetLabelsToNodesResponse.
   *
   * @param responses a list of GetLabelsToNodesResponse to merge.
   * @return the merged GetLabelsToNodesResponse.
   */
  public static GetLabelsToNodesResponse mergeLabelsToNodes(
      Collection<GetLabelsToNodesResponse> responses){
    GetLabelsToNodesResponse labelsToNodesResponse = Records.newRecord(
        GetLabelsToNodesResponse.class);
    Map<String, Set<NodeId>> labelsToNodesMap = new HashMap<>();
    for (GetLabelsToNodesResponse response : responses) {
      if (response != null && response.getLabelsToNodes() != null) {
        Map<String, Set<NodeId>> clusterLabelsToNodesMap = response.getLabelsToNodes();
        for (Map.Entry<String, Set<NodeId>> entry : clusterLabelsToNodesMap.entrySet()) {
          String label = entry.getKey();
          Set<NodeId> clusterNodes = entry.getValue();
          if (labelsToNodesMap.containsKey(label)) {
            Set<NodeId> allNodes = labelsToNodesMap.get(label);
            allNodes.addAll(clusterNodes);
          } else {
            labelsToNodesMap.put(label, clusterNodes);
          }
        }
      }
    }
    labelsToNodesResponse.setLabelsToNodes(labelsToNodesMap);
    return labelsToNodesResponse;
  }

  /**
   * Merges a list of GetClusterNodeLabelsResponse.
   *
   * @param responses a list of GetClusterNodeLabelsResponse to merge.
   * @return the merged GetClusterNodeLabelsResponse.
   */
  public static GetClusterNodeLabelsResponse mergeClusterNodeLabelsResponse(
      Collection<GetClusterNodeLabelsResponse> responses) {
    GetClusterNodeLabelsResponse nodeLabelsResponse = Records.newRecord(
        GetClusterNodeLabelsResponse.class);
    Set<NodeLabel> nodeLabelsList = new HashSet<>();
    for (GetClusterNodeLabelsResponse response : responses) {
      if (response != null && response.getNodeLabelList() != null) {
        nodeLabelsList.addAll(response.getNodeLabelList());
      }
    }
    nodeLabelsResponse.setNodeLabelList(new ArrayList<>(nodeLabelsList));
    return nodeLabelsResponse;
  }

  /**
   * Merges a list of GetQueueUserAclsInfoResponse.
   *
   * @param responses a list of GetQueueUserAclsInfoResponse to merge.
   * @return the merged GetQueueUserAclsInfoResponse.
   */
  public static GetQueueUserAclsInfoResponse mergeQueueUserAcls(
      Collection<GetQueueUserAclsInfoResponse> responses) {
    GetQueueUserAclsInfoResponse aclsInfoResponse = Records.newRecord(
        GetQueueUserAclsInfoResponse.class);
    Set<QueueUserACLInfo> queueUserACLInfos = new HashSet<>();
    for (GetQueueUserAclsInfoResponse response : responses) {
      if (response != null && response.getUserAclsInfoList() != null) {
        queueUserACLInfos.addAll(response.getUserAclsInfoList());
      }
    }
    aclsInfoResponse.setUserAclsInfoList(new ArrayList<>(queueUserACLInfos));
    return aclsInfoResponse;
  }

  /**
   * Merges a list of ReservationListResponse.
   *
   * @param responses a list of ReservationListResponse to merge.
   * @return the merged ReservationListResponse.
   */
  public static ReservationListResponse mergeReservationsList(
      Collection<ReservationListResponse> responses) {
    ReservationListResponse reservationListResponse =
        Records.newRecord(ReservationListResponse.class);
    List<ReservationAllocationState> reservationAllocationStates =
        new ArrayList<>();
    for (ReservationListResponse response : responses) {
      if (response != null && response.getReservationAllocationState() != null) {
        reservationAllocationStates.addAll(
            response.getReservationAllocationState());
      }
    }
    reservationListResponse.setReservationAllocationState(
        reservationAllocationStates);
    return reservationListResponse;
  }

  /**
   * Merges a list of GetAllResourceTypeInfoResponse.
   *
   * @param responses a list of GetAllResourceTypeInfoResponse to merge.
   * @return the merged GetAllResourceTypeInfoResponse.
   */
  public static GetAllResourceTypeInfoResponse mergeResourceTypes(
      Collection<GetAllResourceTypeInfoResponse> responses) {
    GetAllResourceTypeInfoResponse resourceTypeInfoResponse =
        Records.newRecord(GetAllResourceTypeInfoResponse.class);
    Set<ResourceTypeInfo> resourceTypeInfoSet = new HashSet<>();
    for (GetAllResourceTypeInfoResponse response : responses) {
      if (response != null && response.getResourceTypeInfo() != null) {
        resourceTypeInfoSet.addAll(response.getResourceTypeInfo());
      }
    }
    resourceTypeInfoResponse.setResourceTypeInfo(
        new ArrayList<>(resourceTypeInfoSet));
    return resourceTypeInfoResponse;
  }
}

