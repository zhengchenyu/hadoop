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

package org.apache.hadoop.fs.s3a.test;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.s3a.S3ATestConstants.KEY_BUCKET_WITH_MANY_OBJECTS;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.KEY_REQUESTER_PAYS_FILE;

/**
 * Provides S3A filesystem URIs for public data sets for specific use cases.
 *
 * This allows for the contract between S3A tests and the existence of data sets
 * to be explicit and also standardizes access and configuration of
 * replacements.
 *
 * Bucket specific configuration such as endpoint or requester pays should be
 * configured within "hadoop-tools/hadoop-aws/src/test/resources/core-site.xml".
 *
 * Warning: methods may mutate the configuration instance passed in.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class PublicDatasetTestUtils {

  /**
   * Private constructor for utility class.
   */
  private PublicDatasetTestUtils() {}

  /**
   * Default path for an object inside a requester pays bucket: {@value}.
   */
  private static final String DEFAULT_REQUESTER_PAYS_FILE
      = "s3a://usgs-landsat/collection02/catalog.json";

  /**
   * Default bucket for an S3A file system with many objects: {@value}.
   *
   * We use a subdirectory to ensure we have permissions on all objects
   * contained within as well as permission to inspect the directory itself.
   */
  private static final String DEFAULT_BUCKET_WITH_MANY_OBJECTS
      = "s3a://usgs-landsat/collection02/level-1/";

  /**
   * Provide a URI for a directory containing many objects.
   *
   * Unless otherwise configured,
   * this will be {@value DEFAULT_BUCKET_WITH_MANY_OBJECTS}.
   *
   * @param conf Hadoop configuration
   * @return S3A FS URI
   */
  public static String getBucketPrefixWithManyObjects(Configuration conf) {
    return fetchFromConfig(conf,
        KEY_BUCKET_WITH_MANY_OBJECTS, DEFAULT_BUCKET_WITH_MANY_OBJECTS);
  }

  /**
   * Provide a URI to an object within a requester pays enabled bucket.
   *
   * Unless otherwise configured,
   * this will be {@value DEFAULT_REQUESTER_PAYS_FILE}.
   *
   * @param conf Hadoop configuration
   * @return S3A FS URI
   */
  public static String getRequesterPaysObject(Configuration conf) {
    return fetchFromConfig(conf,
        KEY_REQUESTER_PAYS_FILE, DEFAULT_REQUESTER_PAYS_FILE);
  }

  private static String fetchFromConfig(Configuration conf, String key, String defaultValue) {
    String value = conf.getTrimmed(key, defaultValue);

    S3ATestUtils.assume("Empty test property: " + key, !value.isEmpty());

    return value;
  }

}
