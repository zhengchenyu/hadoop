
/*
 *
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
 *
 */

package org.apache.hadoop.yarn.server.resourcemanager.federation.globalqueues;

import java.math.BigDecimal;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.GlobalFederationTempQueuePerPartition;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.ojalgo.optimisation.Expression;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation.Result;
import org.ojalgo.optimisation.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LP(Linear Programming) solution to map queues to sub-cluters. The current
 * formulation is a multi-objective one, that first ensure perfect load
 * balancing, and then optimize for locality affinitization (by allowing a
 * configurable amount of increased load imbalance, zero by default).
 */
public class LPQueueRebalancer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LPQueueRebalancer.class);

  private Map<String, Resource> queueSize;
  private Map<SubClusterId, Resource> resourceAtSubcluster;
  private float[][] affinityToSubcluster;
  private Resource totClusterCap;
  private ResourceCalculator rc;
  private Resource minAlloc;
  private FederationGlobalView inputGlobalView;
  private Result result = null;
  private boolean initialized = false;

  // This parameter allows for some slack when going from primary
  // to secondary objective
  private static final String QUEUE_REBALANCER_LOAD_BALANCE_EPSILON =
      YarnConfiguration.FEDERATION_PREFIX
          + "queue_rebalancer.loadbalance_epsilon";

  private final float primary_to_secondary_epsilon;

  private final Configuration conf;
  private Map<String, Variable> variablesByName;
  private ExpressionsBasedModel lpModel;

  public LPQueueRebalancer(Configuration config,
      ResourceCalculator resourceCalculator, Resource minAlloc) {
    this.conf = config;
    this.rc = resourceCalculator;
    this.minAlloc = minAlloc;
    this.primary_to_secondary_epsilon =
        config.getFloat(QUEUE_REBALANCER_LOAD_BALANCE_EPSILON, 0.0f);
  }

  /**
   * This method is invoked to setup a new problem.
   * 
   * @param federationGlobalView the input {@code FederationGlobalView} to
   *          rebalance.
   */
  public void init(FederationGlobalView federationGlobalView)
      throws FederationGlobalQueueValidationException {

    result = null;
    federationGlobalView.validate();

    this.inputGlobalView = federationGlobalView;
    GlobalFederationTempQueuePerPartition gRoot = GlobalQueuesConversionUtils
        .getAsMergedTempQueuePerPartition(rc, federationGlobalView);

    // EXTRACT SUBCLUSTER SIZES
    Map<SubClusterId, Resource> subClusterSizes = new TreeMap<>();
    for (FederationQueue fq : federationGlobalView.getSubClusters()) {
      subClusterSizes.put(fq.getSubClusterId(), fq.getTotCap());
    }

    // EXTRACT QUEUE SIZES AND AFFINITY TO SUBCLUSTERS
    Map<SubClusterId, GlobalFederationTempQueuePerPartition> localRoots =
        gRoot.getLocalSelves();
    Map<String, GlobalFederationTempQueuePerPartition> globalLeaves =
        gRoot.getLeaves();
    float[][] affinity = new float[globalLeaves.size()][localRoots.size()];

    int i = 0;
    Map<String, Resource> leavesSizes = new TreeMap<>();
    for (GlobalFederationTempQueuePerPartition lr : globalLeaves.values()) {
      int j = 0;
      leavesSizes.put(lr.getQueueName(), lr.getGuaranteed());
      for (GlobalFederationTempQueuePerPartition localLeaf : lr.getLocalSelves()
          .values()) {
        affinity[i][j] = Resources.ratio(rc,
            Resources.add(localLeaf.getPending(), localLeaf.getUsed()),
            federationGlobalView.getGlobal().getTotCap());
        j++;
      }
      i++;
    }

    // INIT PROBLEM
    initProblem(federationGlobalView.getGlobal().getTotCap(), rc,
        leavesSizes, subClusterSizes, affinity);

  }

  @VisibleForTesting
  protected void initProblem(Resource totClusterCap, ResourceCalculator rc,
      Map<String, Resource> queueSize,
      Map<SubClusterId, Resource> resourceAtSubcluster,
      float[][] affinityToSubcluster) {
    this.totClusterCap = totClusterCap;
    this.rc = rc;
    this.queueSize = queueSize;
    this.resourceAtSubcluster = resourceAtSubcluster;
    this.affinityToSubcluster = affinityToSubcluster;
    initialized = true;
  }

  /**
   * This methods must be invoked after initialization, and it constructs the
   * actual LP problem and invokes the solver.
   *
   * @return the result of the calculation.
   * @throws FederationPolicyException
   */
  public Result solve() throws FederationPolicyException {

    if (!initialized) {
      throw new FederationPolicyException(
          "The rebalancer was not initialized before this method was called");
    }

    // SETUP MODEL AND VARIABLES
    lpModel = new ExpressionsBasedModel();
    this.variablesByName = generateVariables();

    // SETUP GENERAL CONSTRAINTS
    generateQueueFullyAssignedConstraints();
    generateSubClusterNotOverallocatedConstraints();
    generateSupportConstraintsForLoadBalancing();

    // SETUP AND SOLVE FOR PRIMARY OBJECTIVE (LOAD BALANCE)
    generatePrimaryObjective();
    if (!lpModel.validate()) {
      throw new FederationPolicyException(
          "The primary LP formulaiton is not valid. " + lpModel);
    }
    final Result primaryResult = lpModel.maximise();
    print(primaryResult);

    // CLEANUP AND PREPARE FOR SECONDARY LP COMPUTATION
    lpModel.dispose();
    this.variablesByName = generateVariables();

    // RE-SETUP GENERAL CONSTRAINTS
    generateQueueFullyAssignedConstraints();
    generateSubClusterNotOverallocatedConstraints();
    generateSupportConstraintsForLoadBalancing();

    // SETUP AND SOLVE FOR SECONDARY OBJECTIVE (AFFINITY)
    generateSecondaryObjectiveFunction(primaryResult);
    if (!lpModel.validate()) {
      throw new FederationPolicyException(
          "The secondary LP formulaiton is not valid." + lpModel);
    }
    final Result secondaryResult = lpModel.maximise();
    print(primaryResult);

    this.result = secondaryResult;

    return secondaryResult;
  }

  /**
   * This methods constructs all the variables used in our problem.
   * 
   * @return a map of variable name to object.
   */
  private Map<String, Variable> generateVariables() {

    // create variables Xij that determines the allocation of
    // queue i in subcluster j.
    Map<String, Variable> variablesByName = new TreeMap<>();
    for (String queue : queueSize.keySet()) {
      for (SubClusterId subCluster : resourceAtSubcluster.keySet()) {
        String varName = queue + "_" + subCluster.getId();
        Variable x = new Variable(varName).lower(BigDecimal.valueOf(0));
        variablesByName.put(varName, x);
      }
    }

    // create bounding max/min and delta of load
    Variable scMaxLoad = new Variable("scMaxLoad").lower(BigDecimal.valueOf(0));
    variablesByName.put("scMaxLoad", scMaxLoad);
    Variable scMinLoad = new Variable("scMinLoad");
    variablesByName.put("scMinLoad", scMinLoad);
    Variable deltaLoad = new Variable("deltaLoad");
    variablesByName.put("deltaLoad", deltaLoad);

    lpModel.addVariables(variablesByName.values());

    return variablesByName;
  }

  /**
   * This method add constraints that ensure that each queue is fully assigned.
   */
  private void generateQueueFullyAssignedConstraints() {

    // each queue is fully assigned
    for (Map.Entry<String, Resource> queue : queueSize.entrySet()) {
      Expression queueFullAllocation =
          lpModel.addExpression(queue.getKey() + "_full_alloc");
      for (Map.Entry<SubClusterId, Resource> subCluster : resourceAtSubcluster
          .entrySet()) {
        double queueSizeAsRatioOfTotal =
            Resources.ratio(rc, queue.getValue(), totClusterCap);
        queueFullAllocation.set(
            variablesByName.get(queue.getKey() + "_" + subCluster.getKey()), 1);
        queueFullAllocation.lower(queueSizeAsRatioOfTotal);
        queueFullAllocation.upper(queueSizeAsRatioOfTotal);
      }
    }
  }

  /**
   * This methods adds constraints that ensures that no sub-cluster is
   * overallocated.
   */
  private void generateSubClusterNotOverallocatedConstraints() {
    for (Map.Entry<SubClusterId, Resource> subCluster : resourceAtSubcluster
        .entrySet()) {
      Expression subclusterNotOverallocated =
          lpModel.addExpression(subCluster.getKey() + "_not_overallocated");
      for (Map.Entry<String, Resource> queue : queueSize.entrySet()) {
        double subClusterLimit =
            Resources.ratio(rc, subCluster.getValue(), totClusterCap);
        subclusterNotOverallocated.set(
            variablesByName.get(queue.getKey() + "_" + subCluster.getKey()), 1);
        subclusterNotOverallocated.lower(BigDecimal.valueOf(0));
        subclusterNotOverallocated.upper(subClusterLimit);
      }
    }
  }

  /**
   * This methods generate constraints to bound the beahaviors of
   * support-variables scLoadMin <= load on any subcluster <= scLoadMax.
   */
  private void generateSupportConstraintsForLoadBalancing() {

    for (Map.Entry<SubClusterId, Resource> subCluster : resourceAtSubcluster
        .entrySet()) {

      // find the capacity of this subcluster relative to totCap of federation
      float subClusterTotalCap = Resources.divide(rc, totClusterCap,
          subCluster.getValue(), totClusterCap);

      Expression scMax =
          lpModel.addExpression("scMaxLoad_" + subCluster.getKey());
      Expression scMin =
          lpModel.addExpression("scMinLoad_" + subCluster.getKey());

      Variable max = variablesByName.get("scMaxLoad");
      Variable min = variablesByName.get("scMinLoad");

      // max and min are proportional to usedCap/subClusterTotalCap
      // moving the divisor
      scMax.set(max, -subClusterTotalCap);
      scMin.set(min, -subClusterTotalCap);

      for (Map.Entry<String, Resource> queue : queueSize.entrySet()) {
        scMax.set(
            variablesByName.get(queue.getKey() + "_" + subCluster.getKey()), 1);
        scMin.set(
            variablesByName.get(queue.getKey() + "_" + subCluster.getKey()), 1);
        scMax.upper(BigDecimal.valueOf(0));
        scMin.lower(BigDecimal.valueOf(0));
      }
    }

    Expression delta = lpModel.addExpression("deltaLoad_constraint");

    delta.set(variablesByName.get("scMaxLoad"), 1);
    delta.set(variablesByName.get("scMinLoad"), -1);
    delta.set(variablesByName.get("deltaLoad"), -1);
    delta.upper(BigDecimal.valueOf(0));

  }

  /**
   * Our primary objective is to load balance the cluster, i.e., to minimize the
   * gap between the two bounding variables scLoadMin and scLoadMax.
   */
  private void generatePrimaryObjective() {
    Expression objective = lpModel.addExpression("maximize load balance");
    // make it very bad for deltaLoad to be large
    objective.set(variablesByName.get("deltaLoad"), Float.valueOf(-1));
    objective.weight(BigDecimal.valueOf(1));
  }

  /**
   * Our secondary objective is to maximize affinity. We disable previous
   * objective, and enable this one, setting the result of the primary objective
   * as a hard constraint (i.e., ensuring we are not decreasing load balance by
   * more than a configurable primary_to_secondary_epsilon).
   */
  private void generateSecondaryObjectiveFunction(Result primaryResult) {

    // add primaryObjective as a constraint (with a bit of slack)
    Expression primaryObjective =
        lpModel.addExpression("maximize load balance");
    primaryObjective.set(variablesByName.get("deltaLoad"), Float.valueOf(-1));

    // NOTE: this is "addictive" we could also use "multiplicative" but doesn't
    // work well when the load-balance is near perfect (close to zero).
    primaryObjective
        .upper(primaryResult.getValue() + primary_to_secondary_epsilon);
    primaryObjective
        .lower(primaryResult.getValue() - primary_to_secondary_epsilon);

    int i = 0;
    Expression objective = lpModel.addExpression("maximize affinity");
    for (String queue : queueSize.keySet()) {
      int j = 0;
      for (SubClusterId subCluster : resourceAtSubcluster.keySet()) {
        String varName = queue + "_" + subCluster;
        objective.set(variablesByName.get(varName),
            Float.valueOf(affinityToSubcluster[i][j]));
        j++;
      }
      i++;
    }
    objective.weight(BigDecimal.valueOf(1));
  }

  /**
   * This methods export the result of running the LP as an updated
   * {@code FederationGlobalView} object.
   * 
   * @return a {@code FederationGlobalView} object whose local queue guarantees
   *         have been updated to match the result of the LP run.
   *
   * @throws FederationPolicyException if
   */
  public FederationGlobalView exportResultToFederationGlobalView()
      throws FederationPolicyException {

    if (inputGlobalView == null) {
      throw new FederationPolicyException(
          "The rebalancer was not initialized before export was called.");
    }

    if (result == null) {
      throw new FederationPolicyException(
          "The rebalancer was initialized but not run before export was called.");
    }

    if (!result.getState().isSuccess()) {
      throw new FederationPolicyException(
          "The rebalancer run was not successful (state: " + result.getState()
              + ")");
    }

    GlobalFederationTempQueuePerPartition gftq = GlobalQueuesConversionUtils
        .getAsMergedTempQueuePerPartition(rc, inputGlobalView);

    for (String queueName : gftq.getLeaves().keySet()) {
      for (FederationQueue root : inputGlobalView.getSubClusters()) {
        Variable v = variablesByName.get(queueName + "_" + root.getSubClusterId());
        double value = v.getValue().doubleValue();
        Resource queueSizeInSubcluster = Resources.multiplyAndNormalizeUp(rc,
            totClusterCap, value, minAlloc);

        root.getChildByName(queueName)
            .setGuarCap(queueSizeInSubcluster);
      }
    }

    // propagate up our guar settings to parents
    for (FederationQueue root : inputGlobalView.getSubClusters()) {
      root.propagateCapacities();
    }

    return inputGlobalView;
  }

  private void print(Result result) {

    LOGGER.info(lpModel.toString());
    LOGGER.info(result.toString());

    LOGGER.debug(lpModel.toString());
    for (Map.Entry<String, Variable> e : variablesByName.entrySet()) {
      LOGGER.debug(e.getKey() + ": " + e.getValue().getValue());
    }
    LOGGER.debug(result.toString());
  }
}
