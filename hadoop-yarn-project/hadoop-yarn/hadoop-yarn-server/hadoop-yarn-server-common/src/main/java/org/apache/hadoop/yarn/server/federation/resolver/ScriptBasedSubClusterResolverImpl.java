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
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.FEDERATION_CLUSTER_RESOLVER_SCRIPT_FILE_NAME;

/**
 *
 * Default simple sub-cluster and rack resolver class.
 *
 * This class expects a three-column comma separated file, specified in
 * yarn.federation.machine-list. Each line of the file should be of the format:
 *
 * nodeName, subClusterId, rackName
 *
 * Lines that do not follow this format will be ignored. This resolver only
 * loads the file when load() is explicitly called; it will not react to changes
 * to the file.
 *
 * It is case-insensitive on the rack and node names and ignores
 * leading/trailing whitespace.
 *
 */
public class ScriptBasedSubClusterResolverImpl extends AbstractSubClusterResolver
    implements SubClusterResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(ScriptBasedSubClusterResolverImpl.class);
  static final String MODE_NODE = "node";
  static final String MODE_RACK = "rack";

  private Configuration conf;
  private String scriptName;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (conf != null) {
      scriptName = conf.get(FEDERATION_CLUSTER_RESOLVER_SCRIPT_FILE_NAME);
    } else {
      scriptName = null;
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public SubClusterId getSubClusterForNode(String nodeName) throws YarnException {
    SubClusterId subClusterId = this.getNodeToSubCluster().get(nodeName);

    if (subClusterId == null) {
      Set<String> subClusters = resolve(nodeName, MODE_NODE);
      if (subClusters != null) {
        subClusterId = SubClusterId.newInstance(subClusters.iterator().next().trim());
      }
    }

    if (subClusterId == null) {
      throw new YarnException("Cannot find subClusterId for node " + nodeName);
    } else {
      getNodeToSubCluster().put(nodeName, subClusterId);
    }

    return subClusterId;
  }

  @Override
  public Set<SubClusterId> getSubClustersForRack(String rackName) throws YarnException {
    Set<SubClusterId> subClusterIds = this.getRackToSubClusters().get(rackName);

    if (subClusterIds == null) {
      Set<String> subClusters = resolve(rackName, MODE_RACK);
      if (CollectionUtils.isNotEmpty(subClusters)) {
        subClusterIds =
            subClusters.stream().map(subCluster -> SubClusterId.newInstance(subCluster.trim()))
                .collect(Collectors.toSet());
      }
    }

    if (CollectionUtils.isEmpty(subClusterIds)) {
      throw new YarnException("Cannot resolve rack " + rackName);
    } else {
      getRackToSubClusters().put(rackName, subClusterIds);
    }

    return subClusterIds;
  }

  @Override
  public void load() {
  }

  public Set<String> resolve(String name, String mode) throws YarnException {
    if (scriptName == null) {
      throw new YarnException("Script file name should not be null!");
    }
    Set<String> ret = new HashSet();
    String output = runResolveCommand(name, scriptName, mode);
    if (output != null) {
      StringTokenizer allSwitchInfo = new StringTokenizer(output);
      while (allSwitchInfo.hasMoreTokens()) {
        String switchInfo = allSwitchInfo.nextToken();
        ret.add(switchInfo);
      }
    } else {
      return null;
    }
    return ret;
  }

  protected String runResolveCommand(String arg, String commandScriptName, String mode) {
    if (StringUtils.isNotBlank(arg)) {
      return null;
    }
    StringBuilder allOutput = new StringBuilder();
    List<String> cmdList = new ArrayList<String>();
    cmdList.add(mode);
    cmdList.add(commandScriptName);
    cmdList.add(arg);
    File dir = null;
    String userDir;
    if ((userDir = System.getProperty("user.dir")) != null) {
      dir = new File(userDir);
    }
    ShellCommandExecutor s =
        new ShellCommandExecutor(cmdList.toArray(new String[cmdList.size()]), dir);
    try {
      s.execute();
      allOutput.append(s.getOutput());
    } catch (Exception e) {
      LOG.warn("Exception running " + s, e);
      return null;
    }
    return allOutput.toString();
  }
}
