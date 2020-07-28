/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal.raft;

import alluxio.master.AbstractPrimarySelector;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.copycat.server.CopycatServer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A primary selector backed by a Raft consensus cluster.
 */
@ThreadSafe
public class RaftPrimarySelector extends AbstractPrimarySelector {
  private static final Logger LOG = LoggerFactory.getLogger(RaftPrimarySelector.class);

  private RaftServer mServer;
  private Listener<CopycatServer.State> mStateListener;

  /**
   * @param server reference to the server backing this selector
   */
  public void init(RaftServer server) {
    mServer = Preconditions.checkNotNull(server, "server");
  }

  public void setServerState(State state) {
    setState(state);
  }

  @Override
  public void start(InetSocketAddress address) throws IOException {
    // The copycat cluster is owned by the outer {@link RaftJournalSystem}.
  }

  @Override
  public void stop() throws IOException {
    // The copycat cluster is owned by the outer {@link RaftJournalSystem}.
  }
}
