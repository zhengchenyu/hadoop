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

package org.apache.hadoop.fs.contract.localfs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractVectoredReadTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.validateVectoredReadResult;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class TestLocalFSContractVectoredRead extends AbstractContractVectoredReadTest {

  public TestLocalFSContractVectoredRead(String bufferType) {
    super(bufferType);
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LocalFSContract(conf);
  }

  @Test
  public void testChecksumValidationDuringVectoredRead() throws Exception {
    Path testPath = path("big_range_checksum");
    LocalFileSystem localFs = (LocalFileSystem) getFileSystem();
    final byte[] datasetCorrect = ContractTestUtils.dataset(DATASET_LEN, 'a', 32);
    try (FSDataOutputStream out = localFs.create(testPath, true)){
      out.write(datasetCorrect);
    }
    Path checksumPath = localFs.getChecksumFile(testPath);
    Assertions.assertThat(localFs.exists(checksumPath))
            .describedAs("Checksum file should be present")
            .isTrue();
    CompletableFuture<FSDataInputStream> fis = localFs.openFile(testPath).build();
    List<FileRange> someRandomRanges = new ArrayList<>();
    someRandomRanges.add(FileRange.createFileRange(10, 1024));
    someRandomRanges.add(FileRange.createFileRange(1025, 1024));
    try (FSDataInputStream in = fis.get()){
      in.readVectored(someRandomRanges, getAllocate());
      validateVectoredReadResult(someRandomRanges, datasetCorrect);
    }
    final byte[] datasetCorrupted = ContractTestUtils.dataset(DATASET_LEN, 'a', 64);
    try (FSDataOutputStream out = localFs.getRaw().create(testPath, true)){
      out.write(datasetCorrupted);
    }
    CompletableFuture<FSDataInputStream> fisN = localFs.openFile(testPath).build();
    try (FSDataInputStream in = fisN.get()){
      in.readVectored(someRandomRanges, getAllocate());
      // Expect checksum exception when data is updated directly through
      // raw local fs instance.
      intercept(ChecksumException.class,
          () -> validateVectoredReadResult(someRandomRanges, datasetCorrupted));
    }
  }
}
