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
import alluxio.grpc.CheckpointCommand;
import alluxio.grpc.MasterCheckpointPOptions;
import alluxio.grpc.MasterCheckpointPRequest;
import alluxio.grpc.MasterCheckpointPResponse;
import alluxio.grpc.MetaMasterMasterServiceGrpc;
import alluxio.master.journal.CatchupFuture;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.sink.JournalSink;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.StreamUtils;
import alluxio.util.logging.SamplingLogger;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.protocol.TermIndex;
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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A state machine for managing all of Alluxio's journaled state. Entries applied to this state
 * machine will be forwarded to the appropriate internal master.
 *
 * The state machine starts by resetting all state, then applying the entries offered by copycat.
 * When the master becomes primary, it should wait until the state machine is up to date and no
 * other primary master is serving, then call {@link #upgrade}. Once the state machine is upgraded,
 * it will ignore all entries appended by copycat because those entries are applied to primary
 * master state before being written to copycat.
 *
 */
@ThreadSafe
public class JournalStateMachine extends BaseStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(JournalStateMachine.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 30L * Constants.SECOND_MS);
  private static final long INVALID_SNAPSHOT = -1;
  private static final int SNAPSHOT_CHUNK_SIZE = 1 * Constants.MB;
  private static final long SNAPSHOT_PERIOD_ENTRIES =
      ServerConfiguration.global().getLong(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES);

  /**
   * Journals managed by this applier.
   */
  private final Map<String, RaftJournal> mJournals;
  private final RaftJournalSystem mJournalSystem;
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
  private volatile AtomicBoolean mDownloadingSnapshot = new AtomicBoolean(false);
  private AtomicBoolean mSendingSnapshot = new AtomicBoolean(false);
  private AtomicReference<File> mSnapshotToInstall = new AtomicReference<>();
  private AtomicReference<TermIndex> mTermIndexToInstall = new AtomicReference<>();
  // The start time of the most recent snapshot
  private volatile long mLastSnapshotStartTime = 0;
  /**
   * Used to control applying to masters.
   */
  private final BufferedJournalApplier mJournalApplier;
  private final SimpleStateMachineStorage mStorage = new SimpleStateMachineStorage();
  private final Map<String, SnapshotInfo> mSnapshotCache = new ConcurrentHashMap<>();
  private Object mRaftGroupId;

  private final ThreadFactory mThreadFactory = new ThreadFactoryBuilder()
      .setNameFormat("JournalStateMachinePool-%d").setDaemon(true).build();
  private final ExecutorService mExecutor = Executors.newSingleThreadExecutor(mThreadFactory);

  /**
   * @param journals     master journals; these journals are still owned by the caller, not by the
   *                     journal state machine
   * @param journalSinks a supplier for journal sinks
   * @param raftJournalSystem
   */
  public JournalStateMachine(Map<String, RaftJournal> journals,
      Supplier<Set<JournalSink>> journalSinks, RaftJournalSystem raftJournalSystem) {
    mJournals = journals;
    mJournalApplier = new BufferedJournalApplier(journals, journalSinks);
    resetState();
    LOG.info("Initialized new journal state machine");
    mJournalSystem = raftJournalSystem;
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
      RaftStorage raftStorage) throws IOException {
    getLifeCycle().startAndTransition(() -> {
      super.initialize(server, groupId, raftStorage);
      mRaftGroupId = groupId;
      mStorage.init(raftStorage);
      loadSnapshot(mStorage.getLatestSnapshot());
    });
  }

  @Override
  public void reinitialize() throws IOException {
    // load snapshot
    LOG.info("Reinitializing Statemachine.");
    mStorage.loadLatestSnapshot();
    loadSnapshot(mStorage.getLatestSnapshot());
    if (mJournalApplier.isSuspended()) {
      resume();
    }
  }

  private long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    if (snapshot == null) {
      LOG.warn("The snapshot info is null.");
      return RaftServerConstants.INVALID_LOG_INDEX;
    }
    LOG.info("loading Snapshot {}.", snapshot);
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      LOG.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try (DataInputStream in = new DataInputStream(
            new BufferedInputStream(new FileInputStream(snapshotFile)))) {
      resetState();
      setLastAppliedTermIndex(last);
      install(in);
    } catch (Exception e) {
      LOG.warn("Failed to load snapshot {}", snapshot, e);
      throw e;
    }
    return last.getIndex();
  }

  @Override
  public long takeSnapshot() {
    // TODO: if secondary master has a more recent snapshot, install it
    if (mJournalSystem.isLeader()) {
      return maybeInstallSnapshotFromSecondary();
    } else {
      return takeSnapshotInternal();
    }
  }

  private long
  maybeInstallSnapshotFromSecondary() {
    File tempFile = mSnapshotToInstall.get();
    if (tempFile == null) {
      SAMPLING_LOG.info("No snapshot to install");
      return INVALID_SNAPSHOT;
    }
    TermIndex lastApplied = getLastAppliedTermIndex();
    SnapshotInfo latestSnapshot = getLatestSnapshot();
    TermIndex lastInstalled = latestSnapshot == null ? null : latestSnapshot.getTermIndex();
    TermIndex toBeInstalled = mTermIndexToInstall.get();
    if (toBeInstalled == null) {
      LOG.info("No snapshot term index info");
      mTermIndexToInstall.set(null);
      mSnapshotToInstall.set(null);
      tempFile.delete();
      return INVALID_SNAPSHOT;
    }
    if (lastInstalled != null && toBeInstalled.compareTo(lastInstalled) < 0) {
      LOG.info("Snapshot to install {} is older than current {}", toBeInstalled, lastInstalled);
      mTermIndexToInstall.set(null);
      mSnapshotToInstall.set(null);
      tempFile.delete();
      return INVALID_SNAPSHOT;
    }
    final File snapshotFile = mStorage.getSnapshotFile(toBeInstalled.getTerm(), toBeInstalled.getIndex());
    LOG.info("Moving temp snapshot {} to file {}", tempFile, snapshotFile);
    try {
      final MD5Hash digest = MD5FileUtil.computeMd5ForFile(tempFile);
      MD5FileUtil.saveMD5File(snapshotFile, digest);
      tempFile.renameTo(snapshotFile);
    } catch (Exception e) {
      LOG.error("Failed to move temp snapshot {} to file {}", tempFile, snapshotFile, e);
      mTermIndexToInstall.set(null);
      mSnapshotToInstall.set(null);
      tempFile.delete();
      return INVALID_SNAPSHOT;
    }
    try {
      mStorage.loadLatestSnapshot();
    } catch (Exception e) {
      LOG.error("Failed loading snapshot file {}", snapshotFile, e);
      snapshotFile.delete();
      mTermIndexToInstall.set(null);
      mSnapshotToInstall.set(null);
      return INVALID_SNAPSHOT;
    }
    mTermIndexToInstall.set(null);
    mSnapshotToInstall.set(null);
    LOG.info("Completed moving snapshot at {} to file {}", toBeInstalled, snapshotFile);
    return toBeInstalled.getIndex();

//    // find latest follower snapshot
//    for (Map.Entry<String, SnapshotInfo> followerSnapshot : mSnapshotCache.entrySet()) {
//      if (mostRecentFollowerSnapshot == null
//          || followerSnapshot.getValue().getTermIndex().compareTo(
//              mostRecentFollowerSnapshot.getValue().getTermIndex()) > 0) {
//        mostRecentFollowerSnapshot = followerSnapshot;
//      }
//    }
//    // if a follower snapshot is newer
//    if (mostRecentFollowerSnapshot != null
//        && (getLatestSnapshot() == null
//        || mostRecentFollowerSnapshot.getValue().getTermIndex().compareTo(
//            getLatestSnapshot().getTermIndex()) > 0)) {
//      maybeScheduleSnapshotDownload(mostRecentFollowerSnapshot.getKey(), mostRecentFollowerSnapshot.getValue());
//    }
  }

//  private void downloadSnapshot(String follower, SnapshotInfo snapshotInfo) {
//    try {
//      //TODO
//    } finally {
//      mDownloading.set(false);
//    }
//  }
//
//  private void maybeScheduleSnapshotDownload(String follower, SnapshotInfo snapshotInfo) {
//    mDownloading.set(true);
//    mExecutor.execute(() -> downloadSnapshot(follower, snapshotInfo));
//  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
//    LOG.debug("Latest Snapshot Info {}", mSnapshotInfo);
//    return mSnapshotInfo;
    return mStorage.getLatestSnapshot();
  }

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RaftProtos.RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {

//    String leaderNodeId = RaftPeerId.valueOf(roleInfoProto.getSelf().getId())
//        .toString();
//
//    LOG.info("Received install snapshot notification form leader: {} with "
//        + "term index: {}", leaderNodeId, firstTermIndexInLog);
//
//    if (!roleInfoProto.getRole().equals(RaftProtos.RaftPeerRole.LEADER)) {
//      // A non-leader Ratis server should not send this notification.
//      LOG.error("Received Install Snapshot notification from non-leader "
//          + "node: {}. Ignoring the notification.", leaderNodeId);
//      return completeExceptionally(new RuntimeException("Received notification to "
//          + "install snapshot from non-leader OM node"));
//    }
// TODO: copy from master and install
//    CompletableFuture<TermIndex> future = CompletableFuture.supplyAsync(
//        () -> this.install(leaderNodeId),
//        installSnapshotExecutor);
    return CompletableFuture.completedFuture(firstTermIndexInLog);
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return mStorage;
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    LOG.info("Received query: {}", request);
    return super.query(request);
  }

  @Override
  public void close() {
    // resetState();
    mClosed = true;
  }

  @Override
  public TransactionContext startTransaction(
      RaftClientRequest raftClientRequest) throws IOException {
    return super.startTransaction(raftClientRequest);
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    try {
      applyJournalEntryCommand(trx);
      return super.applyTransaction(trx);
    } catch (Exception e) {
      return completeExceptionally(e);
    }
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
    mJournalSystem.setIsLeader(false);
    mSnapshotCache.clear();
  }

  @Override
  public void pause() {
    getLifeCycle().transition(LifeCycle.State.PAUSING);
    try {
      if (!mJournalApplier.isSuspended()) {
        suspend();
      }
      // workaround non-empty dir removal
      File backupDir = new File(mStorage.getSmDir().getParentFile(), "sm-backup-" + Instant.now().toString() + UUID.randomUUID().toString());
      LOG.info("statemachine pausing: renaming current states from {} to {}", mStorage.getSmDir(), backupDir);
      if (!mStorage.getSmDir().renameTo(backupDir)) {
        LOG.warn("statemachine dir rename failed");
        Files.move(mStorage.getSmDir().toPath(), backupDir.toPath());
      }
      LOG.info("statemachine paused: successfully renamed current states from {} to {}", mStorage.getSmDir(), backupDir);
    } catch (IOException e) {
      LOG.warn("statemachine pause failed", e);
      throw new IllegalStateException(e);
    }
    getLifeCycle().transition(LifeCycle.State.PAUSED);
  }

  /**
   * Unpause the StateMachine, re-initialize the DoubleBuffer and update the
   * lastAppliedIndex. This should be done after uploading new state to the
   * StateMachine.
   */
  public void unpause(long newLastAppliedSnaphsotIndex,
      long newLastAppliedSnapShotTermIndex) {
    getLifeCycle().startAndTransition(() -> {
      this.setLastAppliedTermIndex(TermIndex.newTermIndex(
          newLastAppliedSnapShotTermIndex, newLastAppliedSnaphsotIndex));
      try {
        if (mJournalApplier.isSuspended()) {
          resume();
        }
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  private static <T> CompletableFuture<T> completeExceptionally(Exception e) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(e);
    return future;
  }

  /**
     * Applies a journal entry commit to the state machine.
     *
     * This method is automatically discovered by the Copycat framework.
     *
   * @param commit the commit
   */
  public void applyJournalEntryCommand(TransactionContext commit) {
    JournalEntry entry;
    try {
      entry = JournalEntry.parseFrom(commit.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer());
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e,
          "Encountered invalid journal entry in 'commit': %s.", commit);
      System.exit(-1);
      throw new IllegalStateException(e); // We should never reach here.
    }
    try {
      applyEntry(entry);
    } finally {
      Preconditions.checkState(commit.getLogEntry().getIndex() > mLastAppliedCommitIndex);
      mLastAppliedCommitIndex = commit.getLogEntry().getIndex();
      // commit.close();
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
      justification = "All calls to applyJournalEntryCommand() are synchronized by copycat")
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
  private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

  private long takeSnapshotInternal() {
    // Snapshot format is [snapshotId, name1, bytes1, name2, bytes2, ...].
    if (mClosed) {
      return INVALID_SNAPSHOT;
    }
    LOG.debug("Calling snapshot");
    Preconditions.checkState(!mSnapshotting, "Cannot call snapshot multiple times concurrently");
    mSnapshotting = true;
    mLastSnapshotStartTime = System.currentTimeMillis();
    long snapshotId = mNextSequenceNumberToRead - 1;
    TermIndex last = getLastAppliedTermIndex();
    File tempFile;
    try {
      tempFile = File.createTempFile("ratis_snapshot_" + System.currentTimeMillis() + "_",
          ".dat", new File("/tmp/"));
    } catch (IOException e) {
      LOG.warn("Failed to create temp snapshot", e);
      return INVALID_SNAPSHOT;
    }
    LOG.info("Taking a snapshot to file {}", tempFile);
    final File snapshotFile = mStorage.getSnapshotFile(last.getTerm(), last.getIndex());
    try (FileOutputStream sws = new FileOutputStream(tempFile)) {
      buffer.putLong(0, snapshotId);
      sws.write(buffer.array());
      JournalUtils.writeToCheckpoint(sws, getStateMachines());
    } catch (Exception e) {
      tempFile.delete();
      LOG.warn("Failed to take snapshot: {}", snapshotId, e);
      return INVALID_SNAPSHOT;
    }
    try {
      final MD5Hash digest = MD5FileUtil.computeMd5ForFile(tempFile);
      LOG.info("Saving digest for snapshot file {}", snapshotFile);
      MD5FileUtil.saveMD5File(snapshotFile, digest);
      LOG.info("Renaming a snapshot file {} to {}", tempFile, snapshotFile);
      tempFile.renameTo(snapshotFile);
      LOG.info("Completed snapshot up to SN {} in {}ms", snapshotId,
          System.currentTimeMillis() - mLastSnapshotStartTime);
    } catch (Exception e) {
      tempFile.delete();
      LOG.warn("Failed to take snapshot: {}", snapshotId, e);
      return INVALID_SNAPSHOT;
    }
    try {
      mStorage.loadLatestSnapshot();
    } catch (Exception e) {
      snapshotFile.delete();
      LOG.warn("Failed to take snapshot: {}", snapshotId, e);
      return INVALID_SNAPSHOT;
    }
    mSnapshotting = false;
    // maybeSendSnapshotToPrimaryMaster();
    return last.getIndex();
  }

  public SnapshotInfo maybeSendSnapshotToPrimaryMaster(MetaMasterMasterServiceGrpc.MetaMasterMasterServiceStub metaClient) {
    if (mJournalSystem.isLeader()) {
      return null;
    }
    LOG.info("Heartbeat to check latest snapshot to send.");
    SnapshotInfo snapshot = getLatestSnapshot();
    if (snapshot == null) {
      LOG.info("No snapshot to send");
      return null;
    }
    if (mSendingSnapshot.compareAndSet(false, true)) {
      StreamObserver<MasterCheckpointPResponse> responseObserver =
          new ClientResponseObserver<MasterCheckpointPRequest, MasterCheckpointPResponse>() {
            private ClientCallStreamObserver<MasterCheckpointPRequest> mRequestStream;
            final File mSnapshotFile = mStorage.getSnapshotFile(snapshot.getTerm(), snapshot.getIndex());
            final long mLength = mSnapshotFile.length();
            long mOffset = 0;

            @Override
            public void onNext(MasterCheckpointPResponse value) {
              if (mRequestStream == null) {
                LOG.error("No request stream assigned");
                mSendingSnapshot.set(false);
                throw new IllegalStateException("No request stream assigned");
              }
              if (mClosed || mJournalSystem.isLeader()) {
                LOG.info("cancel sending snapshot due to state change");
                mRequestStream.onError(new IllegalStateException("cancel sending snapshot due to state change"));
                mSendingSnapshot.set(false);
                return;
              }
              switch (value.getCommand()) {
                case CheckpointCommand_Done:
                  mSendingSnapshot.set(false);
                  break;
                case CheckpointCommand_Send:
                  try (InputStream is = new FileInputStream(mSnapshotFile)) {
                    is.skip(mOffset);
                    boolean eof = false;
                    int chunkSize = SNAPSHOT_CHUNK_SIZE;
                    long available = mLength - mOffset;
                    if (available <= SNAPSHOT_CHUNK_SIZE) {
                      eof = true;
                      chunkSize = (int) available;
                    }
                    byte[] buffer = new byte[chunkSize];
                    IOUtils.readFully(is, buffer);
                    mRequestStream.onNext(MasterCheckpointPRequest.newBuilder()
                        .setOptions(MasterCheckpointPOptions.newBuilder()
                            .setOffset(mOffset)
                            .setEof(eof)
                            .setChunk(ByteString.copyFrom(buffer))
                            .setSnapshotTerm(snapshot.getTerm())
                            .setSnapshotIndex(snapshot.getIndex()))
                        .setMasterId(this.hashCode())
                        .build());
                    mOffset += chunkSize;
                    LOG.info("sent {} bytes from file {}", mOffset, mSnapshotFile.getPath());
                  } catch (FileNotFoundException e) {
                    LOG.warn("Cannot find snapshot {} at {}", mSnapshotFile, mOffset, e);
                    mRequestStream.onError(e);
                    mSendingSnapshot.set(false);
                  } catch (IOException e) {
                    LOG.warn("Error sending snapshot {} at {}", mSnapshotFile, mOffset, e);
                    mRequestStream.onError(e);
                    mSendingSnapshot.set(false);
                  }
                  break;
                case CheckpointCommand_Pause:
                  break;
                case CheckpointCommand_Unknown:
                  LOG.error("Unknown response sending snapshot {} at {}", mSnapshotFile, mOffset);
                  mRequestStream.onError(new IllegalStateException("Unknown response sending snapshot"));
                  mSendingSnapshot.set(false);
                  break;
              }
            }

            @Override
            public void onError(Throwable t) {
              LOG.error("Error sending snapshot {} at {}", mSnapshotFile, mOffset, t);
              mSendingSnapshot.set(false);
            }

            @Override
            public void onCompleted() {
              mSendingSnapshot.set(false);
              mRequestStream.onCompleted();
            }

            @Override
            public void beforeStart(ClientCallStreamObserver<MasterCheckpointPRequest> requestStream) {
              mRequestStream = requestStream;
            }
          };
      StreamObserver<MasterCheckpointPRequest> requestObserver = metaClient.masterCheckpoint(responseObserver);
      requestObserver.onNext(MasterCheckpointPRequest.newBuilder()
          .setMasterId(this.hashCode())
          .setOptions(MasterCheckpointPOptions.newBuilder()
              .setOffset(0)
              .setSnapshotTerm(snapshot.getTerm())
              .setSnapshotIndex(snapshot.getIndex())
              .build())
          .build());
    } else {
      LOG.info("Another send is in progress");
    }
    return null;
  }

//  public void maybeSendSnapshotToPrimaryMaster() {
//    mExecutor.execute(() -> sendSnapshotToPrimaryMaster());
//  }
//
//  private void sendSnapshotToPrimaryMaster() {
//  }

  private void install(DataInputStream inputStream) {
    if (mClosed) {
      return;
    }
    if (mIgnoreApplys) {
      LOG.warn("Unexpected request to install a snapshot on a read-only journal state machine");
      return;
    }

    long snapshotId = 0L;
    try {
      snapshotId = inputStream.readLong();
      JournalUtils.restoreFromCheckpoint(new CheckpointInputStream(inputStream), getStateMachines());
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
    mSnapshotCache.clear();
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
   * @return the start time of the most recent snapshot
   */
  public long getLastSnapshotStartTime() {
    return mLastSnapshotStartTime;
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
      mJournalSystem.setIsLeader(groupMemberId.getPeerId() == raftPeerId);
    }
  }

  public StreamObserver<MasterCheckpointPRequest> maybeCopySnapshotFromFollower(
      StreamObserver<MasterCheckpointPResponse> responseStreamObserver) {
    SnapshotInfo currentSnapshot = getLatestSnapshot();
    LOG.info("received upload snapshot request from follower");
    return new StreamObserver<MasterCheckpointPRequest>() {
      TermIndex mTermIndex;
      File mTempFile = null;
      FileOutputStream mOutputStream = null;
      long mBytesWritten = 0;
      @Override
      public void onNext(MasterCheckpointPRequest request) {
        try {
          onNextInternal(request);
        } catch (Exception e) {
          LOG.error("Unexpected exception uploading snapshot", e);
          responseStreamObserver.onError(e);
          cleanup();
        }
      }

      public void cleanup() {
        if (mTermIndex != null) {
          mDownloadingSnapshot.compareAndSet(true, false);
        }
        if (mOutputStream != null) {
          try {
            mOutputStream.close();
          } catch (IOException ioException) {
            LOG.error("Error closing snapshot file", ioException);
          }
        }
        if (mTempFile != null && !mTempFile.delete()) {
          LOG.error("Error deleting snapshot file {}", mTempFile.getPath());
        }
      }

      public void onNextInternal(MasterCheckpointPRequest request) throws IOException {
        if (mClosed || !mJournalSystem.isLeader()) {
          LOG.info("cancel snapshot upload request {} due to shutdown", request);
          responseStreamObserver.onCompleted();
          cleanup();
        }
        TermIndex termIndex = TermIndex.newTermIndex(
            request.getOptions().getSnapshotTerm(), request.getOptions().getSnapshotIndex());
        if (currentSnapshot != null && currentSnapshot.getTermIndex().compareTo(termIndex) >= 0) {
          // we have a newer one, close the request
          LOG.info("discard older upload request from {}. current {}, request {}",
              request.getMasterId(), currentSnapshot.getTermIndex(), termIndex);
          responseStreamObserver.onCompleted();
          cleanup();
          return;
        }
        if (currentSnapshot != null && termIndex.getIndex() - currentSnapshot.getIndex() < SNAPSHOT_PERIOD_ENTRIES / 4) {
          LOG.info("discard worthless upload request from {}. current {}, request {}",
              request.getMasterId(), currentSnapshot.getTermIndex(), termIndex);
          responseStreamObserver.onCompleted();
          cleanup();
          return;
        }
        if (mTermIndex == null) {
          // new start, check if there is already a download
          LOG.info("new upload request from {}. current {}, request {}", request.getMasterId(), currentSnapshot, termIndex);
          if (!mDownloadingSnapshot.compareAndSet(false, true)) {
            LOG.info("another upload is in progress");
            responseStreamObserver.onCompleted();
            return;
          }
          if (mSnapshotToInstall.get() != null) {
            LOG.info("another upload is pending install");
            mDownloadingSnapshot.set(false);
            responseStreamObserver.onCompleted();
            return;
          }
          mTermIndex = termIndex;
          // start a new file
          mTempFile = File.createTempFile("ratis_snapshot_" + System.currentTimeMillis() + "_",
              ".dat", new File("/tmp/"));
          mTempFile.deleteOnExit();
          responseStreamObserver.onNext(
              MasterCheckpointPResponse.newBuilder()
                  .setCommand(CheckpointCommand.CheckpointCommand_Send)
                  .build());
          LOG.info("requesting snapshot from {}", request.getMasterId());
        } else {
          if (!termIndex.equals(mTermIndex)) {
            throw new IOException(String.format(
                "mismatched term index when uploading the snapshot expected: %s actual: %s", mTermIndex, termIndex));
          }
          if (!request.getOptions().hasChunk()) {
            throw new IOException(String.format("A chunk is missing from the request %s", request));
          }
          // write the chunk
          if (mOutputStream == null) {
            LOG.info("start writing to temporary file {}", mTempFile.getPath());
            mOutputStream = new FileOutputStream(mTempFile);
          }
          long position = mOutputStream.getChannel().position();
          if (position != request.getOptions().getOffset()) {
            throw new IOException(String.format("Mismatched offset in file %d, expect %d, bytes written %d",
                position, request.getOptions().getOffset(), mBytesWritten));
          }
          mOutputStream.write(request.getOptions().getChunk().toByteArray());
          mBytesWritten += request.getOptions().getChunk().size();
          LOG.info("written {} bytes to snapshot file {}", mBytesWritten, mTempFile.getPath());
          if (request.getOptions().getEof()) {
            LOG.info("Completed writing to temporary file {} with size {}",
                mTempFile.getPath(), mOutputStream.getChannel().position());
            mOutputStream.close();
            mOutputStream = null;
            mSnapshotToInstall.set(mTempFile);
            mTermIndexToInstall.set(mTermIndex);
            if (mTermIndex != null) {
              mDownloadingSnapshot.compareAndSet(true, false);
            }
            LOG.info("Finished copying snapshot from follower to local file {}.", mTempFile);
            responseStreamObserver.onCompleted();
          } else {
            responseStreamObserver.onNext(
                MasterCheckpointPResponse.newBuilder()
                    .setCommand(CheckpointCommand.CheckpointCommand_Send)
                    .build());
          }
        }
      }

      @Override
      public void onError(Throwable t) {
        LOG.error("Received onError event.", t);
        cleanup();
      }

      @Override
      public void onCompleted() {
        if (mOutputStream != null) {
          LOG.error("Request completed with unfinished upload.");
          cleanup();
          return;
        }
      }
    };
  }
}
