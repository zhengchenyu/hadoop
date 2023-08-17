package org.apache.hadoop.yarn.server.resourcemanager.federation.globalqueues;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.GlobalFederationTempQueuePerPartition;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.TempQueuePerPartition;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceInfo;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.sun.tools.javac.util.Assert;

/**
 * Utils class to perform import/export operations among queue hierarchy
 * representation formats.
 */
public class GlobalQueuesConversionUtils {

  /**
   * This method exports this FedQueue (and its underlying hierarchy) as a
   * {@code GlobalFederationTempQueuePerPartition} object, to be used to compute
   * ideal allocation.
   *
   * @param rc the ResourceCalculator
   * @param fRoot the FedQueue to export as
   *          GlobalFederationTempQueuePerPartition
   *
   * @return a GlobalFederationTempQueuePerPartition representation of this
   *         queue hierarchy
   */
  public static GlobalFederationTempQueuePerPartition getAsTempQueuePerPartition(
      ResourceCalculator rc, FederationQueue fRoot) {

    Resource totCap = fRoot.getTotCap();
    Resource guarCap = fRoot.getGuarCap();
    Resource maxCap = fRoot.getMaxCap();
    Resource demandCap = fRoot.getDemandCap();
    Resource usedCap = fRoot.getUsedCap();

    SubClusterId scope = fRoot.getSubClusterId();

    float absCapacity = Resources.divide(rc, totCap, guarCap, totCap);
    float absmaxCapacity = Resources.divide(rc, totCap, maxCap, totCap);

    Assert.check(absCapacity >= 0 && absCapacity <= 1.0,
        "Queue " + fRoot.getQueueName() + " at " + fRoot.getSubClusterId()
            + " has absCapacity:" + absCapacity + " (with guarCap: " + maxCap
            + " and totCap: " + totCap + ")");
    Assert.check(absmaxCapacity >= 0 && absmaxCapacity <= 1.0,
        "Queue " + fRoot.getQueueName() + " at " + fRoot.getUsedCap()
            + " has absmaxCapacity:" + absmaxCapacity + " (with maxCap: "
            + maxCap + " and totCap: " + totCap + ")");

    GlobalFederationTempQueuePerPartition root =
        new GlobalFederationTempQueuePerPartition(fRoot.getQueueName(), usedCap,
            false, RMNodeLabelsManager.NO_LABEL, Resources.none(), absCapacity,
            absmaxCapacity, totCap, Resources.none(), null, Resources.none(),
            Resources.none(), scope, fRoot.getConf());

    for (FederationQueue c : fRoot.getChildren().values()) {
      root.addChild(getAsTempQueuePerPartition(rc, c));
    }

    // setting pending, as it would be all zeros given we pass a null LeafQueue
    root.setPending(demandCap);
    return root;
  }


  /**
   * Converts a {@code FederationGlobalView} object to a {@code GlobalFederationTempQueuePerPartition}.
   *
   * @param rc the ResourceCalculator to use.
   * @param globalView the input {@code FederationGlobalView}
   *
   * @return the resulting {@code GlobalFederationTempQueuePerPartition}
   */
  public static GlobalFederationTempQueuePerPartition getAsMergedTempQueuePerPartition(
      ResourceCalculator rc, FederationGlobalView globalView) {
    GlobalFederationTempQueuePerPartition gRoot =
        GlobalQueuesConversionUtils.getAsTempQueuePerPartition(rc, globalView.getGlobal());
    gRoot.setTotalPartitionUnassigned(
        Resources.subtract(gRoot.getGuaranteed(), gRoot.getUsed()));
    for (FederationQueue f : globalView.getSubClusters()) {
      f.setTotalUnassigned(Resources.subtract(f.getTotCap(),
          f.getUsedCap()));
    }
    recursiveMerge(rc, gRoot, globalView.getSubClusters());
    return gRoot;
  }

  private static void recursiveMerge(ResourceCalculator rc,
      GlobalFederationTempQueuePerPartition gRoot, List<FederationQueue> lRoots) {

    Map<SubClusterId, GlobalFederationTempQueuePerPartition> lRootsAsTqpp =
        new TreeMap<>();
    for (FederationQueue localRoot : lRoots) {
      GlobalFederationTempQueuePerPartition tq =
          GlobalQueuesConversionUtils.getAsTempQueuePerPartition(rc, localRoot);
      tq.setTotalPartitionUnassigned(localRoot.getTotalUnassigned());
      lRootsAsTqpp.put(localRoot.getSubClusterId(), tq);
    }
    gRoot.addAllLocalSelf(lRootsAsTqpp);
    for (TempQueuePerPartition gChild : gRoot.getChildren()) {
      ((GlobalFederationTempQueuePerPartition) gChild)
          .setTotalPartitionUnassigned(gRoot.getTotalPartitionUnassigned());
      List<FederationQueue> temp = new ArrayList<>();
      for (FederationQueue lRoot : lRoots) {
        FederationQueue localChild = lRoot.getChildByName(gChild.getQueueName());
        // NOTE: the totalUnassigned is passed by reference so that local
        // updates affect
        // it globally.
        localChild.setTotalUnassigned(lRoot.getTotalUnassigned());
        temp.add(localChild);
      }
      recursiveMerge(rc, (GlobalFederationTempQueuePerPartition) gChild, temp);
    }
  }

  private static String generateQueueName(FederationQueue root, FederationQueue child,
      boolean forQueueList) {
    String childName = child.getQueueName();

    if (forQueueList) {
      childName = (root.getQueueName().equals("root")) ? child.getQueueName()
          : child.getQueueName().substring(root.getQueueName().length() + 1,
              child.getQueueName().length());
    }
    if (child.getChildren() == null || child.getChildren().size() == 0) {
      childName += child.getQueueName().hashCode();
    }
    return childName;
  }

  /**
   * This method exports this FederationQueue hierarchy as a
   * {@code CapacitySchedulerConfiguration}.
   *
   * @param baseConf a starting configuration
   * @param root the FederationQueue to export as CapacitySchedulerConfiguration
   * @return the CapacitySchedulerConfiguration representing this queue
   *         hierarchy
   */
  public static CapacitySchedulerConfiguration exportAsCapacitySchedulerConfiguration(
          Configuration baseConf, FederationQueue root) {
    CapacitySchedulerConfiguration conf =
            new CapacitySchedulerConfiguration(baseConf, false);
    recursiveExportToCSC(conf, root);

    return conf;
  }

  private static void recursiveExportToCSC(CapacitySchedulerConfiguration conf,
                                           FederationQueue root) {

    String queueName = root.getQueueName();

    if (!queueName.equals("root")) {
      queueName = "root." + queueName;
    }

    Resource sumOfChildren = Resource.newInstance(0, 0);
    List<String> childrenAsString = new ArrayList<>();

    for (FederationQueue child : root.getChildren().values()) {

      String childName = generateQueueName(root, child, true);

      childrenAsString.add(childName);
      Resources.addTo(sumOfChildren, child.getGuarCap());
    }
    conf.setQueues(queueName,
            childrenAsString.toArray(new String[childrenAsString.size()]));

    for (FederationQueue child : root.getChildren().values()) {

      if (root.getGuarCap().getMemorySize() > 0) {
        double percentageCapacity = 100.0 * child.getGuarCap().getMemorySize()
                / (double) sumOfChildren.getMemorySize();

        conf.setCapacity("root." + generateQueueName(root, child, false),
                (percentageCapacity >= 0) ? (float) percentageCapacity : 0);
      } else {
        // if the parent is "disabled" by setting it to zero, propagate down
        conf.setCapacity("root." + generateQueueName(root, child, false), 0);
      }
    }

    // do recursion
    for (FederationQueue child : root.getChildren().values()) {
      recursiveExportToCSC(conf, child);
    }

  }

  /**
   * This method updates the global and local values of a ideal allocation of a
   * {@code FederationGlobalView} based on the input
   * {@code GlobalFederationTempQueuePerPartition}.
   *
   * @param rootAsTQ a {@code GlobalFederationTempQueuePerPartition} that
   *          contains the ideal allocation for both global and local FederationQueue.
   * @param global the {@code FederationGlobalView} that we want to update.
   */
  public static void updateIdealAlloc(
      GlobalFederationTempQueuePerPartition rootAsTQ,
      FederationGlobalView global) {
    recursiveUpdate(global.getGlobal(), global.getSubClusters(), rootAsTQ);
  }

  private static void recursiveUpdate(FederationQueue globalRoot,
      List<FederationQueue> localRoots,
      GlobalFederationTempQueuePerPartition inputGTQ) {

    // update this node
    globalRoot.setIdealAlloc(inputGTQ.getIdealAssigned());

    for (FederationQueue local : localRoots) {
      local.setIdealAlloc(inputGTQ.getLocalSelf(local.getSubClusterId()).getIdealAssigned());
    }

    // recurse to all children (both global and local)
    for (FederationQueue globalChild : globalRoot.getChildren().values()) {
      TempQueuePerPartition t =
          inputGTQ.getChildrenByName(globalChild.getQueueName());

      List<FederationQueue> localChildren = new ArrayList<>();
      for (FederationQueue lChild : localRoots) {
        localChildren.add(lChild.getChildByName(globalChild.getQueueName()));
      }

      recursiveUpdate(globalChild, localChildren,
          (GlobalFederationTempQueuePerPartition) t);
    }

  }

}
