/**
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
 */
package org.apache.hadoop.hbase.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.thrift.ThriftMetrics.ThriftServerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseThriftTestingUtility {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseThriftTestingUtility.class);
  private Thread thriftServerThread;
  private volatile Exception thriftServerException;
  private ThriftServer thriftServer;
  private int testServerPort;

  public int getServerPort() {
    return testServerPort;
  }

  public void startThriftServer(Configuration conf, ThriftServerType t) throws Exception {
    List<String> args = new ArrayList<>();
    testServerPort = HBaseTestingUtility.randomFreePort();
    args.add("-" + Constants.PORT_OPTION);
    args.add(String.valueOf(testServerPort));
    if (t == ThriftServerType.ONE) {
      thriftServer = new ThriftServer(conf);
    } else if (t == ThriftServerType.TWO) {
      thriftServer = new org.apache.hadoop.hbase.thrift2.ThriftServer(conf);
    }

    LOG.info("Starting Thrift Server {} on port: {} ", t, testServerPort);

    thriftServerException = null;
    thriftServerThread = new Thread(() -> {
      try {
        thriftServer.run(args.toArray(args.toArray(new String[args.size()])));
      } catch (Exception e) {
        thriftServerException = e;
      }
    });
    thriftServerThread.setName(ThriftServer.class.getSimpleName());
    thriftServerThread.start();

    if (thriftServerException != null) {
      LOG.error("HBase Thrift server threw an exception ", thriftServerException);
      throw thriftServerException;
    }

    // wait up to 10s for the server to start
    boolean isServing = false;
    int i = 0;
    while (i++ < 100) {
      if (thriftServer.tserver == null || !thriftServer.tserver.isServing()) {
        Thread.sleep(100);
      } else {
        isServing = true;
        break;
      }
    }

    if (!isServing) {
      throw new IOException("Failed to start thrift server " + t);
    }

    LOG.info("Started Thrift Server {} on port {}", t, testServerPort);
  }

  public void stopThriftServer() throws Exception{
    LOG.debug("Stopping Thrift Server");
    thriftServer.stop();
    thriftServerThread.join();
  }
}