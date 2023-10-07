/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.dao;

import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.api.json.JSONUnmarshaller;

/**
 * This is a DAO class for the configuration of parameters for federation
 * policies. This generalizes several possible configurations as two lists of
 * {@link SubClusterIdInfo} and corresponding weights as a {@link Float}. The
 * interpretation of the weight is left to the logic in the policy.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
@XmlRootElement(name = "federation-policy")
@XmlAccessorType(XmlAccessType.FIELD)
public class WeightedPolicyInfo {

  @XmlAccessorType(XmlAccessType.FIELD)
  public static class PolicyWeights {
    Map<SubClusterIdInfo, Float> weigths;

    public PolicyWeights() {
    }

    public PolicyWeights(Map<SubClusterIdInfo, Float> weigths) {
      this.weigths = weigths;
    }

    static PolicyWeights create(Map<SubClusterIdInfo, Float> weigths) {
      return new PolicyWeights(weigths);
    }

    public Map<SubClusterIdInfo, Float> getWeigths() {
      return weigths;
    }

    @Override
    public String toString() {
      return weigths.toString();
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(WeightedPolicyInfo.class);
  private static JSONJAXBContext jsonjaxbContext = initContext();
  private Map<SubClusterIdInfo, Float> routerPolicyWeights = new HashMap<>();
  private Map<SubClusterIdInfo, Float> amrmPolicyWeights = new HashMap<>();
  // To maintain backward compatibility, default weights are retained in the original variables
  private Map<String, PolicyWeights> routerPolicyWeightsMap = new HashMap<>();
  private Map<String, PolicyWeights> amrmPolicyWeightsMap = new HashMap<>();
  private float headroomAlpha;

  public WeightedPolicyInfo() {
    // JAXB needs this
  }

  private void initial() {
    if (!routerPolicyWeightsMap.containsKey(FederationPolicyUtils.DEFAULT_POLICY_KEY)) {
      routerPolicyWeightsMap.put(FederationPolicyUtils.DEFAULT_POLICY_KEY, PolicyWeights.create(routerPolicyWeights));
    }
    if (!amrmPolicyWeightsMap.containsKey(FederationPolicyUtils.DEFAULT_POLICY_KEY)) {
      amrmPolicyWeightsMap.put(FederationPolicyUtils.DEFAULT_POLICY_KEY, PolicyWeights.create(amrmPolicyWeights));
    }
  }

  private static JSONJAXBContext initContext() {
    try {
      return new JSONJAXBContext(JSONConfiguration.DEFAULT,
          WeightedPolicyInfo.class);
    } catch (JAXBException e) {
      LOG.error("Error parsing the policy.", e);
    }
    return null;
  }

  /**
   * Deserializes a {@link WeightedPolicyInfo} from a byte UTF-8 JSON
   * representation.
   *
   * @param bb the input byte representation.
   *
   * @return the {@link WeightedPolicyInfo} represented.
   *
   * @throws FederationPolicyInitializationException if a deserialization error
   *           occurs.
   */
  public static WeightedPolicyInfo fromByteBuffer(ByteBuffer bb)
      throws FederationPolicyInitializationException {

    if (jsonjaxbContext == null) {
      throw new FederationPolicyInitializationException(
          "JSONJAXBContext should" + " not be null.");
    }

    try {
      JSONUnmarshaller unmarshaller = jsonjaxbContext.createJSONUnmarshaller();
      final byte[] bytes = new byte[bb.remaining()];
      bb.get(bytes);
      String params = new String(bytes, StandardCharsets.UTF_8);

      WeightedPolicyInfo weightedPolicyInfo = unmarshaller.unmarshalFromJSON(
          new StringReader(params), WeightedPolicyInfo.class);
      weightedPolicyInfo.initial();
      return weightedPolicyInfo;
    } catch (JAXBException j) {
      throw new FederationPolicyInitializationException(j);
    }
  }

  /**
   * Getter of the router weights.
   *
   * @return the router weights.
   */
  public Map<SubClusterIdInfo, Float> getRouterPolicyWeights() {
    return getRouterPolicyWeights(FederationPolicyUtils.DEFAULT_POLICY_KEY);
  }

  public Map<SubClusterIdInfo, Float> getRouterPolicyWeights(String tag) {
    if (tag != null && routerPolicyWeightsMap.containsKey(tag)) {
      return routerPolicyWeightsMap.get(tag).getWeigths();
    }
    return routerPolicyWeightsMap.get(FederationPolicyUtils.DEFAULT_POLICY_KEY).getWeigths();
  }

  public Map<String, PolicyWeights> getRouterPolicyWeightsMap() {
    return routerPolicyWeightsMap;
  }

  /**
   * Setter method for Router weights.
   *
   * @param policyWeights the router weights.
   */
  public void setRouterPolicyWeights(Map<SubClusterIdInfo, Float> policyWeights) {
    this.routerPolicyWeightsMap.put(FederationPolicyUtils.DEFAULT_POLICY_KEY, PolicyWeights.create(policyWeights));
  }

  public void setRouterPolicyWeights(String tag, Map<SubClusterIdInfo, Float> policyWeights) {
    this.routerPolicyWeightsMap.put(tag, PolicyWeights.create(policyWeights));
  }

  /**
   * Getter for AMRMProxy weights.
   *
   * @return the AMRMProxy weights.
   */
  public Map<SubClusterIdInfo, Float> getAMRMPolicyWeights() {
    return getAMRMPolicyWeights(FederationPolicyUtils.DEFAULT_POLICY_KEY);
  }

  public Map<SubClusterIdInfo, Float> getAMRMPolicyWeights(String tag) {
    if (tag != null && amrmPolicyWeightsMap.containsKey(tag)) {
      return amrmPolicyWeightsMap.get(tag).getWeigths();
    }
    return amrmPolicyWeightsMap.get(FederationPolicyUtils.DEFAULT_POLICY_KEY).getWeigths();
  }

  public Map<String, PolicyWeights> getAmrmPolicyWeightsMap() {
    return amrmPolicyWeightsMap;
  }

  /**
   * Setter method for ARMRMProxy weights.
   *
   * @param policyWeights the amrmproxy weights.
   */
  public void setAMRMPolicyWeights(Map<SubClusterIdInfo, Float> policyWeights) {
    amrmPolicyWeightsMap.put(FederationPolicyUtils.DEFAULT_POLICY_KEY, PolicyWeights.create(policyWeights));
  }

  public void setAMRMPolicyWeights(String tag, Map<SubClusterIdInfo, Float> policyWeights) {
    amrmPolicyWeightsMap.put(tag, PolicyWeights.create(policyWeights));
  }

  /**
   * Converts the policy into a byte array representation in the input
   * {@link ByteBuffer}.
   *
   * @return byte array representation of this policy configuration.
   *
   * @throws FederationPolicyInitializationException if a serialization error
   *           occurs.
   */
  public ByteBuffer toByteBuffer()
      throws FederationPolicyInitializationException {
    if (jsonjaxbContext == null) {
      throw new FederationPolicyInitializationException(
          "JSONJAXBContext should" + " not be null.");
    }
    try {
      String s = toJSONString();
      return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    } catch (JAXBException j) {
      throw new FederationPolicyInitializationException(j);
    }
  }

  private String toJSONString() throws JAXBException {
    JSONMarshaller marshaller = jsonjaxbContext.createJSONMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    StringWriter sw = new StringWriter(256);
    marshaller.marshallToJSON(this, sw);
    return sw.toString();
  }

  @Override
  public boolean equals(Object other) {

    if (other == null || !other.getClass().equals(this.getClass())) {
      return false;
    }

    Map<String, PolicyWeights> otherRouterWeightsByTag =
        ((WeightedPolicyInfo) other).getRouterPolicyWeightsMap();
    Map<String, PolicyWeights> otherAmrmWeightsByTag =
        ((WeightedPolicyInfo) other).getAmrmPolicyWeightsMap();

    if (otherRouterWeightsByTag.size() != this.getRouterPolicyWeightsMap().size()) {
      return false;
    }

    for (Map.Entry<String, PolicyWeights> entry :
        otherRouterWeightsByTag.entrySet()) {
      Map<SubClusterIdInfo, Float> tmpWeights =
          getRouterPolicyWeightsMap().get(entry.getKey()).getWeigths();
      if (tmpWeights == null || !CollectionUtils.isEqualCollection(tmpWeights.entrySet(),
          entry.getValue().getWeigths().entrySet())) {
        return false;
      }
    }

    if (otherAmrmWeightsByTag.size() != this.getAmrmPolicyWeightsMap().size()) {
      return false;
    }

    for (Map.Entry<String, PolicyWeights> entry : otherAmrmWeightsByTag.entrySet()) {
      Map<SubClusterIdInfo, Float> tmpWeights =
          getAmrmPolicyWeightsMap().get(entry.getKey()).getWeigths();
      if (tmpWeights == null || !CollectionUtils.isEqualCollection(tmpWeights.entrySet(),
          entry.getValue().getWeigths().entrySet())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().
        append(this.amrmPolicyWeights).
        append(this.routerPolicyWeights).
        append(this.amrmPolicyWeightsMap).
        append(this.routerPolicyWeightsMap).
        toHashCode();
  }

  /**
   * Return the parameter headroomAlpha, used by policies that balance
   * weight-based and load-based considerations in their decisions.
   *
   * For policies that use this parameter, values close to 1 indicate that most
   * of the decision should be based on currently observed headroom from various
   * sub-clusters, values close to zero, indicate that the decision should be
   * mostly based on weights and practically ignore current load.
   *
   * @return the value of headroomAlpha.
   */
  public float getHeadroomAlpha() {
    return headroomAlpha;
  }

  /**
   * Set the parameter headroomAlpha, used by policies that balance weight-based
   * and load-based considerations in their decisions.
   *
   * For policies that use this parameter, values close to 1 indicate that most
   * of the decision should be based on currently observed headroom from various
   * sub-clusters, values close to zero, indicate that the decision should be
   * mostly based on weights and practically ignore current load.
   *
   * @param headroomAlpha the value to use for balancing.
   */
  public void setHeadroomAlpha(float headroomAlpha) {
    this.headroomAlpha = headroomAlpha;
  }

  @Override
  public String toString() {
    try {
      return toJSONString();
    } catch (JAXBException e) {
      e.printStackTrace();
      return "Error serializing to string.";
    }
  }

  @XmlRootElement
  @XmlAccessorType(XmlAccessType.FIELD)
  static class MapEntry {
    Map<String, String> map3;

    MapEntry() {
    }

    MapEntry(Map<String, String> map3) {
      this.map3 = map3;
    }
    static MapEntry create(Map<String, String> map3) {
      return new MapEntry(map3);
    }
  }


  @XmlRootElement(name = "TestClass")
  @XmlAccessorType(XmlAccessType.FIELD)
  static class TestClass {
    Map<String, String> map1 = new HashMap<>();
    Map<String, MapEntry> map2 = new HashMap<>();

    TestClass() {
      map1.put("k1", "v111");
      map1.put("k2", "v222");
      Map<String, String> map21 = new HashMap<>();
      Map<String, String> map22 = new HashMap<>();
      map21.put("k11", "v11");
      map21.put("k12", "v12");
      map22.put("k21", "v21");
      map22.put("k22", "v22");
      map2.put("k1", MapEntry.create(map21));
      map2.put("k2", MapEntry.create(map22));
    }
  }

  public static void main(String[] args) throws Exception {
    TestClass testClass = new TestClass();
    JSONJAXBContext jsonjaxbContext = new JSONJAXBContext(JSONConfiguration.DEFAULT, TestClass.class);
    JSONMarshaller marshaller = jsonjaxbContext.createJSONMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    StringWriter sw = new StringWriter(256);
    marshaller.marshallToJSON(testClass, sw);
    System.out.println(sw);
    System.out.println("--------------");
    SubClusterIdInfo subClusterIdInfo1 = new SubClusterIdInfo(SubClusterId.newInstance("SC1"));
    SubClusterIdInfo subClusterIdInfo2 = new SubClusterIdInfo(SubClusterId.newInstance("SC2"));
    WeightedPolicyInfo weightedPolicyInfo = new WeightedPolicyInfo();
    weightedPolicyInfo.routerPolicyWeights.put(subClusterIdInfo1, 0.6f);
    weightedPolicyInfo.routerPolicyWeights.put(subClusterIdInfo2, 0.4f);
    weightedPolicyInfo.amrmPolicyWeights.put(subClusterIdInfo1, 0.3f);
    weightedPolicyInfo.amrmPolicyWeights.put(subClusterIdInfo2, 0.7f);
    weightedPolicyInfo.headroomAlpha = 1.0F;
    Map<SubClusterIdInfo, Float> weightedPolicyInfo1 = new HashMap<>();
    weightedPolicyInfo1.put(subClusterIdInfo1, 1f);
    weightedPolicyInfo1.put(subClusterIdInfo2, 0f);
    Map<SubClusterIdInfo, Float> weightedPolicyInfo2 = new HashMap<>();
    weightedPolicyInfo2.put(subClusterIdInfo1, 0f);
    weightedPolicyInfo2.put(subClusterIdInfo2, 1f);
    weightedPolicyInfo.routerPolicyWeightsMap.put("LABEL1", PolicyWeights.create(weightedPolicyInfo1));
    weightedPolicyInfo.routerPolicyWeightsMap.put("LABEL2", PolicyWeights.create(weightedPolicyInfo2));
    weightedPolicyInfo.amrmPolicyWeightsMap.put("LABEL1", PolicyWeights.create(weightedPolicyInfo1));
    weightedPolicyInfo.amrmPolicyWeightsMap.put("LABEL2", PolicyWeights.create(weightedPolicyInfo2));
    jsonjaxbContext = new JSONJAXBContext(JSONConfiguration.DEFAULT, WeightedPolicyInfo.class);
    marshaller = jsonjaxbContext.createJSONMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    sw = new StringWriter(256);
    marshaller.marshallToJSON(weightedPolicyInfo, sw);
    System.out.println(sw);
  }
}
