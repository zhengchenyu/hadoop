package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestHDFSBasic {

  private static int NUM = 1024 * 1024 * 10;
  private static String CONTENT = "hello";
  private static int CONTENT_LEN = CONTENT.length();

  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUp() throws IOException {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(10)
        .format(true).build();
  }

  @AfterClass
  public static void shutDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void test() throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.mkdirs(new Path("/tmp"));
    FSDataOutputStream out = fs.create(new Path("/tmp/a"));
    for (int i = 0; i < NUM; i++) {
      out.writeBytes(CONTENT);
    }
    out.flush();
    out.close();
    FSDataInputStream in = fs.open(new Path("/tmp/a"));
    byte[] bytes = new byte[NUM * CONTENT_LEN];
    in.readFully(bytes);
    for (int i = 0; i < NUM; i++) {
      assertEquals("hello", new String(bytes, CONTENT_LEN * i, CONTENT_LEN));
    }
  }

  @Test
  public void testEC() throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.mkdirs(new Path("/ec"));
    fs.setErasureCodingPolicy(new Path("/ec"), "RS-6-3-1024k");
    FSDataOutputStream out = fs.create(new Path("/ec/a"));
    for (int i = 0; i < NUM; i++) {
      out.writeBytes(CONTENT);
    }
    out.flush();
    out.close();
    FSDataInputStream in = fs.open(new Path("/ec/a"));
    byte[] bytes = new byte[NUM * CONTENT_LEN];
    in.readFully(bytes);
    for (int i = 0; i < NUM; i++) {
      assertEquals("hello", new String(bytes, CONTENT_LEN * i, CONTENT_LEN));
    }
  }
}
