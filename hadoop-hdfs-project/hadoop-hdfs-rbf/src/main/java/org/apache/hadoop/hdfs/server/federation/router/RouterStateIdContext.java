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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.federation.protocol.proto.HdfsServerFederationProtos;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.ha.ReadOnly;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.InvalidProtocolBufferException;


/**
 * This is the router implementation to hold the state Ids for all
 * namespaces. This object is only updated by responses from NameNodes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class RouterStateIdContext implements AlignmentContext {

  private final HashSet<String> coordinatedMethods;
  /**
   * Collection of last-seen namespace state Ids for a set of namespaces.
   * Each value is globally shared by all outgoing connections to a particular namespace,
   * so updates should only be performed using reliable responses from NameNodes.
   */
  private final ConcurrentHashMap<String, LongAccumulator> namespaceIdMap;
  // Size limit for the map of state Ids to send to clients.
  private final int maxSizeOfFederatedStateToPropagate;

  RouterStateIdContext(Configuration conf) {
    this.coordinatedMethods = new HashSet<>();
    // For now, only ClientProtocol methods can be coordinated, so only checking
    // against ClientProtocol.
    for (Method method : ClientProtocol.class.getDeclaredMethods()) {
      if (method.isAnnotationPresent(ReadOnly.class)
          && method.getAnnotationsByType(ReadOnly.class)[0].isCoordinated()) {
        coordinatedMethods.add(method.getName());
      }
    }

    namespaceIdMap = new ConcurrentHashMap<>();

    maxSizeOfFederatedStateToPropagate =
        conf.getInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_FEDERATED_STATE_PROPAGATION_MAXSIZE,
        RBFConfigKeys.DFS_ROUTER_OBSERVER_FEDERATED_STATE_PROPAGATION_MAXSIZE_DEFAULT);
  }

  /**
   * Adds the {@link #namespaceIdMap} to the response header that will be sent to a client.
   */
  public void setResponseHeaderState(RpcResponseHeaderProto.Builder headerBuilder) {
    headerBuilder.getRouterFederatedStateMap().forEach(
        (k, v) -> headerBuilder.getRouterFederatedStateMap()
            .put(k, Long.max(v, getNamespaceStateId(k).get())));
  }

  public LongAccumulator getNamespaceStateId(String nsId) {
    return namespaceIdMap.computeIfAbsent(nsId, key -> new LongAccumulator(Math::max, Long.MIN_VALUE));
  }

  public void removeNamespaceStateId(String nsId) {
    namespaceIdMap.remove(nsId);
  }

  /**
   * Utility function to parse routerFederatedState field in RPC headers.
   */
  public static Map<String, Long> getRouterFederatedStateMap(ByteString byteString) {
    if (byteString != null) {
      HdfsServerFederationProtos.RouterFederatedStateProto federatedState;
      try {
        federatedState = HdfsServerFederationProtos.RouterFederatedStateProto.parseFrom(byteString);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      return federatedState.getNamespaceStateIdsMap();
    } else {
      return Collections.emptyMap();
    }
  }

  public static long getClientStateIdFromCurrentCall(String nsId) {
    Long clientStateID = Long.MIN_VALUE;
    Server.Call call = Server.getCurCall().get();
    if (call != null) {
      Map<String, Long> clientFederatedStateIds = call.getFederatedNamespaceState();
      clientStateID = clientFederatedStateIds.getOrDefault(nsId, Long.MIN_VALUE);
    }
    return clientStateID;
  }

  public static void updateClientStateIdToCurrentCall(String nsId, long stateId) {
    Server.Call call = Server.getCurCall().get();
    if (call != null) {
      Map<String, Long> clientFederatedStateIds = call.getFederatedNamespaceState();
      clientFederatedStateIds.put(nsId, stateId);
    }
  }

  @Override
  public void updateResponseState(RpcResponseHeaderProto.Builder header) {
    if (namespaceIdMap.size() <= maxSizeOfFederatedStateToPropagate) {
      setResponseHeaderState(header);
    }
  }

  @Override
  public void receiveResponseState(RpcResponseHeaderProto header) {
    // Do nothing.
  }

  @Override
  public void updateRequestState(RpcRequestHeaderProto.Builder header) {
    // Do nothing.
  }

  /**
   * Routers do not update their state using information from clients
   * to avoid clients interfering with one another.
   */
  @Override
  public long receiveRequestState(RpcRequestHeaderProto header,
      long clientWaitTime) throws RetriableException {
    // Do nothing.
    return 0;
  }

  @Override
  public long getLastSeenStateId() {
    return 0;
  }

  @Override
  public boolean isCoordinatedCall(String protocolName, String methodName) {
    return protocolName.equals(ClientProtocol.class.getCanonicalName())
        && coordinatedMethods.contains(methodName);
  }
}
