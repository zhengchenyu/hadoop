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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol;

/**
 * Defines the contract to be implemented by the request interceptor classes,
 * that can be used to intercept and inspect messages sent from the application
 * master to the resource manager.
 */
public interface RequestInterceptor extends DistributedSchedulingAMProtocol,
    Configurable {
  /**
   * This method is called for initializing the interceptor. This is guaranteed
   * to be called only once in the lifetime of this instance.
   *
   * @param ctx AMRMProxy application context
   */
  void init(AMRMProxyApplicationContext ctx);

  /**
   * Recover interceptor state when NM recovery is enabled. AMRMProxy will
   * recover the data map into
   * AMRMProxyApplicationContext.getRecoveredDataMap(). All interceptors should
   * recover state from it.
   *
   * For example, registerRequest has to be saved by the last interceptor (i.e.
   * the one that actually connects to RM), in order to re-register when RM
   * fails over.
   *
   * @param recoveredDataMap states for all interceptors recovered from NMSS
   */
  void recover(Map<String, byte[]> recoveredDataMap);

  /**
   * This method is called to release the resources held by the interceptor.
   * This will be called when the application pipeline is being destroyed. The
   * concrete implementations should dispose the resources and forward the
   * request to the next interceptor, if any.
   */
  void shutdown();

  /**
   * Sets the next interceptor in the pipeline. The concrete implementation of
   * this interface should always pass the request to the nextInterceptor after
   * inspecting the message. The last interceptor in the chain is responsible to
   * send the messages to the resource manager service and so the last
   * interceptor will not receive this method call.
   *
   * @param nextInterceptor the next interceptor to set
   */
  void setNextInterceptor(RequestInterceptor nextInterceptor);

  /**
   * Returns the next interceptor in the chain.
   * 
   * @return the next interceptor in the chain
   */
  RequestInterceptor getNextInterceptor();

  /**
   * Returns the context.
   * 
   * @return the context
   */
  AMRMProxyApplicationContext getApplicationContext();
}
