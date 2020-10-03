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

import alluxio.Constants;
import alluxio.ProcessUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.AddQuorumServerRequest;
import alluxio.grpc.JournalQueryRequest;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.LogUtils;
import alluxio.util.StreamUtils;
import alluxio.util.logging.SamplingLogger;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A state machine for managing all of Alluxio's journaled state. Entries applied to this state
 * machine will be forwarded to the appropriate internal master.
 *
 * The state machine starts by resetting all state, then applying the entries offered by Ratis.
 * When the master becomes primary, it should wait until the state machine is up to date and no
 * other primary master is serving, then call {@link #upgrade}. Once the state machine is upgraded,
 * it will ignore all entries appended by Ratis because those entries are applied to primary
 * master state before being written to Rati.
 *
 */
@ThreadSafe
public class JournalStateMachine extends BaseStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(JournalStateMachine.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 10L * Constants.MINUTE_MS);

  /** Journals managed by this applier. */
  private final Map<String, RaftJournal> mJournals;
  private final RaftJournalSystem mJournalSystem;
  private final SnapshotReplicationManager mSnapshotManager;
  @GuardedBy("this")
  private boolean mIgnoreApplys = false;
  @GuardedBy("this")
  private boolean mClosed = false;

  private volatile long mLastAppliedCommitIndex = -1;
  // The last special "primary start" sequence number applied to this state machine. These special
  // sequence numbers are identified by being negative.
  private volatile long mLastPrimaryStartSequenceNumber = 0;
  private volatile long mNextSequenceNumberToRead = 0;
  private volatile boolean mSnapshotting = false;
  private volatile boolean mIsLeader = false;

  // The start time of the most recent snapshot
  private volatile long mLastSnapshotStartTime = 0;
  /** Used to control applying to masters. */
  private BufferedJournalApplier mJournalApplier;
  private final SimpleStateMachineStorage mStorage = new SimpleStateMachineStorage();
  private RaftGroupId mRaftGroupId;
  private RaftServer mServer;

  /**
   * @param journals      master journals; these journals are still owned by the caller, not by the
   *                      journal state machine
   * @param journalSystem the raft journal system
   */
  public JournalStateMachine(Map<String, RaftJournal> journals, RaftJournalSystem journalSystem) {
    mJournals = journals;
    mJournalApplier = new BufferedJournalApplier(journals,
        () -> journalSystem.getJournalSinks(null));
    resetState();
    LOG.info("Initialized new journal state machine");
    mJournalSystem = journalSystem;
    mSnapshotManager = new SnapshotReplicationManager(journalSystem, mStorage);
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(server, groupId, raftStorage);
      mServer = server;
      mRaftGroupId = groupId;
      mStorage.init(raftStorage);
      loadSnapshot(mStorage.getLatestSnapshot());
    });
  }

  @Override
  public void reinitialize() throws IOException {
    LOG.info("Reinitializing state machine.");
    mStorage.loadLatestSnapshot();
    loadSnapshot(mStorage.getLatestSnapshot());
    unpause();
  }

  private void loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    if (snapshot == null) {
      LOG.info("No snapshot to load");
      return;
    }
    LOG.info("Loading Snapshot {}", snapshot);
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      throw new FileNotFoundException(
          String.format("The snapshot file %s does not exist", snapshotFile.getPath()));
    }
    try {
      resetState();
      setLastAppliedTermIndex(snapshot.getTermIndex());
      install(snapshotFile);
    } catch (Exception e) {
      throw new IOException(String.format("Failed to load snapshot %s", snapshot), e);
    }
  }

  @Override
  public long takeSnapshot() {
    if (mIsLeader) {
      return mSnapshotManager.maybeCopySnapshotFromFollower();
    } else {
      return takeLocalSnapshot();
    }
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return mStorage.getLatestSnapshot();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return mStorage;
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    CompletableFuture<Message> future = new CompletableFuture<>();
    try {
      JournalQueryRequest queryRequest = JournalQueryRequest.parseFrom(
          request.getContent().asReadOnlyByteBuffer());
      LOG.debug("Received query request: {}", queryRequest);
      // give snapshot manager a chance to handle snapshot related requests
      Message reply = mSnapshotManager.handleRequest(queryRequest);
      if (reply != null) {
        future.complete(reply);
        return future;
      }
      // Snapshot manager returned null indicating the request is not handled. Check and handle
      // other type of requests.
      if (queryRequest.hasAddQuorumServerRequest()) {
        AddQuorumServerRequest addRequest = queryRequest.getAddQuorumServerRequest();
        return CompletableFuture.supplyAsync(() -> {
          try {
            mJournalSystem.addQuorumServer(addRequest.getServerAddress());
          } catch (IOException e) {
            throw new CompletionException(e);
          }
          return Message.EMPTY;
        });
      }
    } catch (Exception e) {
      LOG.error("failed processing request {}", request, e);
      future.completeExceptionally(e);
      return future;
    }
    return super.query(request);
  }

  @Override
  public void close() {
    mSnapshotManager.close();
    mClosed = true;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      applyJournalEntryCommand(trx);
      return super.applyTransaction(trx);
    } catch (Exception e) {
      return RaftJournalUtils.completeExceptionally(e);
    }
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
    mIsLeader = false;
    mJournalSystem.notifyLeadershipStateChanged(false);
  }

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RaftProtos.RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
    if (roleInfoProto.getRole() != RaftProtos.RaftPeerRole.FOLLOWER) {
      return RaftJournalUtils.completeExceptionally(
          new IllegalStateException(String.format(
              "Server should be a follower when installing a snapshot from leader. Actual: %s",
              roleInfoProto.getRole())));
    }
    return mSnapshotManager.installSnapshotFromLeader();
  }

  @Override
  public void pause() {
    getLifeCycle().transition(LifeCycle.State.PAUSING);
    try {
      suspend();
    } catch (IOException e) {
      throw new IllegalStateException("State machine pause failed", e);
    }
    getLifeCycle().transition(LifeCycle.State.PAUSED);
  }

  /**
   * Unpause the StateMachine. This should be done after uploading new state to the StateMachine.
   */
  public void unpause() {
    getLifeCycle().startAndTransition(() -> {
      try {
        if (mJournalApplier.isSuspended()) {
          resume();
        }
      } catch (IOException e) {
        throw new IllegalStateException("State machine resume failed", e);
      }
    });
  }

  /**
   * Applies a journal entry commit to the state machine.
   * @param commit the commit
   */
  public void applyJournalEntryCommand(TransactionContext commit) {
    JournalEntry entry;
    try {
      entry = JournalEntry.parseFrom(
          commit.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e,
          "Encountered invalid journal entry in commit: %s.", commit);
      System.exit(-1);
      throw new IllegalStateException(e); // We should never reach here.
    }
    try {
      applyEntry(entry);
    } finally {
      Preconditions.checkState(commit.getLogEntry().getIndex() > mLastAppliedCommitIndex);
      mLastAppliedCommitIndex = commit.getLogEntry().getIndex();
    }
  }

  /**
   * Applies the journal entry, ignoring empty entries and expanding multi-entries.
   *
   * @param entry the entry to apply
   */
  private void applyEntry(JournalEntry entry) {
    Preconditions.checkState(
        entry.getAllFields().size() <= 1
            || (entry.getAllFields().size() == 2 && entry.hasSequenceNumber()),
        "Raft journal entries should never set multiple fields in addition to sequence "
            + "number, but found %s",
        entry);
    if (entry.getJournalEntriesCount() > 0) {
      // This entry aggregates multiple entries.
      for (JournalEntry e : entry.getJournalEntriesList()) {
        applyEntry(e);
      }
    } else if (entry.getSequenceNumber() < 0) {
      // Negative sequence numbers indicate special entries used to indicate that a new primary is
      // starting to serve.
      mLastPrimaryStartSequenceNumber = entry.getSequenceNumber();
    } else if (entry.toBuilder().clearSequenceNumber().build()
        .equals(JournalEntry.getDefaultInstance())) {
      // Ignore empty entries, they are created during snapshotting.
    } else {
      applySingleEntry(entry);
    }
  }

  @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
      justification = "All calls to applyJournalEntryCommand() are synchronized by ratis")
  private void applySingleEntry(JournalEntry entry) {
    if (mClosed) {
      return;
    }
    long newSN = entry.getSequenceNumber();
    if (newSN < mNextSequenceNumberToRead) {
      // This can happen due to retried writes. For example, if flushing [3, 4] fails, we will
      // retry, and the log may end up looking like [1, 2, 3, 4, 3, 4] if the original request
      // eventually succeeds. Once we've read the first "4", we must ignore the next two entries.
      LOG.debug("Ignoring duplicate journal entry with SN {} when next SN is {}", newSN,
          mNextSequenceNumberToRead);
      return;
    }
    if (newSN > mNextSequenceNumberToRead) {
      ProcessUtils.fatalError(LOG,
          "Unexpected journal entry. The next expected SN is %s, but"
              + " encountered an entry with SN %s. Full journal entry: %s",
          mNextSequenceNumberToRead, newSN, entry);
    }

    mNextSequenceNumberToRead++;
    if (!mIgnoreApplys) {
      mJournalApplier.processJournalEntry(entry);
    }
  }

  /**
   * Takes a snapshot of local state machine.
   * @return the index of last included entry, or {@link RaftLog#INVALID_LOG_INDEX} if it fails
   */
  public long takeLocalSnapshot() {
    // Snapshot format is [snapshotId, name1, bytes1, name2, bytes2, ...].
    if (mClosed) {
      SAMPLING_LOG.info("Skip taking snapshot because state machine is closed.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    if (mServer.getLifeCycleState() != LifeCycle.State.RUNNING) {
      SAMPLING_LOG.info("Skip taking snapshot because raft server is not in running state: "
          + "current state is {}.", mServer.getLifeCycleState());
      return RaftLog.INVALID_LOG_INDEX;
    }
    if (mJournalApplier.isSuspended()) {
      SAMPLING_LOG.info("Skip taking snapshot while journal application is suspended.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    if (!mJournalSystem.isSnapshotAllowed()) {
      SAMPLING_LOG.info("Skip taking snapshot when it is not allowed by the journal system.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    LOG.debug("Calling snapshot");
    Preconditions.checkState(!mSnapshotting, "Cannot call snapshot multiple times concurrently");
    mSnapshotting = true;
    try {
      mLastSnapshotStartTime = System.currentTimeMillis();
      long snapshotId = mNextSequenceNumberToRead - 1;
      TermIndex last = getLastAppliedTermIndex();
      File tempFile;
      try {
        tempFile = RaftJournalUtils.createTempSnapshotFile(mStorage);
      } catch (IOException e) {
        LogUtils.warnWithException(LOG, "Failed to create temp snapshot file", e);
        return RaftLog.INVALID_LOG_INDEX;
      }
      LOG.info("Taking a snapshot to file {}", tempFile);
      final File snapshotFile = mStorage.getSnapshotFile(last.getTerm(), last.getIndex());
      try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(tempFile))) {
        outputStream.writeLong(snapshotId);
        JournalUtils.writeToCheckpoint(outputStream, getStateMachines());
      } catch (Exception e) {
        tempFile.delete();
        LogUtils.warnWithException(LOG,
            "Failed to write snapshot {} to file {}", snapshotId, tempFile, e);
        return RaftLog.INVALID_LOG_INDEX;
      }
      try {
        final MD5Hash digest = MD5FileUtil.computeMd5ForFile(tempFile);
        LOG.info("Saving digest for snapshot file {}", snapshotFile);
        MD5FileUtil.saveMD5File(snapshotFile, digest);
        LOG.info("Renaming a snapshot file {} to {}", tempFile, snapshotFile);
        if (!tempFile.renameTo(snapshotFile)) {
          tempFile.delete();
          LOG.warn("Failed to rename snapshot from {} to {}", tempFile, snapshotFile);
          return RaftLog.INVALID_LOG_INDEX;
        }
        LOG.info("Completed snapshot up to SN {} in {}ms", snapshotId,
            System.currentTimeMillis() - mLastSnapshotStartTime);
      } catch (Exception e) {
        tempFile.delete();
        LogUtils.warnWithException(LOG,
            "Failed to complete snapshot: {} - {}", snapshotId, snapshotFile, e);
        return RaftLog.INVALID_LOG_INDEX;
      }
      try {
        mStorage.loadLatestSnapshot();
      } catch (Exception e) {
        snapshotFile.delete();
        LogUtils.warnWithException(LOG, "Failed to refresh latest snapshot: {}", snapshotId, e);
        return RaftLog.INVALID_LOG_INDEX;
      }
      return last.getIndex();
    } finally {
      mSnapshotting = false;
    }
  }

  private void install(File snapshotFile) {
    if (mClosed) {
      return;
    }
    if (mIgnoreApplys) {
      LOG.warn("Unexpected request to install a snapshot on a read-only journal state machine");
      return;
    }

    long snapshotId = 0L;
    try (DataInputStream stream =  new DataInputStream(new FileInputStream(snapshotFile))) {
      snapshotId = stream.readLong();
      JournalUtils.restoreFromCheckpoint(new CheckpointInputStream(stream), getStateMachines());
    } catch (Exception e) {
      JournalUtils.handleJournalReplayFailure(LOG, e, "Failed to install snapshot: %s", snapshotId);
      if (ServerConfiguration.getBoolean(PropertyKey.MASTER_JOURNAL_TOLERATE_CORRUPTION)) {
        return;
      }
    }

    if (snapshotId < mNextSequenceNumberToRead - 1) {
      LOG.warn("Installed snapshot for SN {} but next SN to read is {}", snapshotId,
          mNextSequenceNumberToRead);
    }
    mNextSequenceNumberToRead = snapshotId + 1;
    LOG.info("Successfully installed snapshot up to SN {}", snapshotId);
  }

  /**
   * Suspends applying to masters.
   *
   * @throws IOException
   */
  public void suspend() throws IOException {
    mJournalApplier.suspend();
  }

  /**
   * Resumes applying to masters.
   *
   * @throws IOException
   */
  public void resume() throws IOException {
    mJournalApplier.resume();
  }

  /**
   * Initiates catching up of masters to given sequence.
   *
   * @param sequence the target sequence
   * @return the future to track when catching up is done
   */
  public CatchupFuture catchup(long sequence) {
    return mJournalApplier.catchup(sequence);
  }

  private List<Journaled> getStateMachines() {
    return StreamUtils.map(RaftJournal::getStateMachine, mJournals.values());
  }

  private void resetState() {
    if (mClosed) {
      return;
    }
    if (mIgnoreApplys) {
      LOG.warn("Unexpected call to resetState() on a read-only journal state machine");
      return;
    }
    mJournalApplier.close();
    mJournalApplier = new BufferedJournalApplier(mJournals,
        () -> mJournalSystem.getJournalSinks(null));
    for (RaftJournal journal : mJournals.values()) {
      journal.getStateMachine().resetState();
    }
  }

  /**
   * Upgrades the journal state machine to primary mode.
   *
   * @return the last sequence number read while in secondary mode
   */
  public long upgrade() {
    // Resume the journal applier if was suspended.
    if (mJournalApplier.isSuspended()) {
      try {
        resume();
      } catch (IOException e) {
        ProcessUtils.fatalError(LOG, e, "State-machine failed to catch up after suspension.");
      }
    }
    mIgnoreApplys = true;
    return mNextSequenceNumberToRead - 1;
  }

  /**
   * @return the sequence number of the last entry applied to the state machine
   */
  public long getLastAppliedSequenceNumber() {
    return mNextSequenceNumberToRead - 1;
  }

  /**
   * @return the last primary term start sequence number applied to this state machine
   */
  public long getLastPrimaryStartSequenceNumber() {
    return mLastPrimaryStartSequenceNumber;
  }

  /**
   * @return whether the state machine is in the process of taking a snapshot
   */
  public boolean isSnapshotting() {
    return mSnapshotting;
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId raftPeerId) {
    if (mRaftGroupId == groupMemberId.getGroupId()) {
      mIsLeader = groupMemberId.getPeerId() == raftPeerId;
      mJournalSystem.notifyLeadershipStateChanged(mIsLeader);
    } else {
      LOG.warn("Received notification for unrecognized group {}, current group is {}",
          groupMemberId.getGroupId(), mRaftGroupId);
    }
  }

  /**
   * @return the snapshot replication manager
   */
  public SnapshotReplicationManager getSnapshotReplicationManager() {
    return mSnapshotManager;
  }
}
