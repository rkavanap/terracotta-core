/*
 * All content copyright (c) 2003-2007 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.tc.l2.handler;

import com.tc.async.api.AbstractEventHandler;
import com.tc.async.api.ConfigurationContext;
import com.tc.async.api.EventContext;
import com.tc.async.api.Sink;
import com.tc.l2.context.ManagedObjectSyncContext;
import com.tc.l2.context.SyncObjectsRequest;
import com.tc.l2.msg.ObjectSyncMessage;
import com.tc.l2.msg.RelayedCommitTransactionMessage;
import com.tc.l2.msg.ServerTxnAckMessage;
import com.tc.l2.msg.ServerTxnAckMessageFactory;
import com.tc.l2.objectserver.L2ObjectStateManager;
import com.tc.l2.objectserver.ServerTransactionFactory;
import com.tc.net.groups.NodeID;
import com.tc.net.protocol.tcm.ChannelID;
import com.tc.objectserver.api.ObjectManager;
import com.tc.objectserver.core.api.ServerConfigurationContext;
import com.tc.objectserver.tx.ServerTransaction;
import com.tc.objectserver.tx.TransactionBatchReader;
import com.tc.objectserver.tx.TransactionBatchReaderFactory;
import com.tc.objectserver.tx.TransactionalObjectManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class L2ObjectSyncHandler extends AbstractEventHandler {

  private final L2ObjectStateManager    l2ObjectStateMgr;
  private ObjectManager                 objectManager;
  private TransactionalObjectManager    txnObjectMgr;
  private TransactionBatchReaderFactory batchReaderFactory;

  private Sink                          dehydrateSink;
  private Sink sendSink;

  public L2ObjectSyncHandler(L2ObjectStateManager l2StateManager) {
    l2ObjectStateMgr = l2StateManager;
  }

  public void handleEvent(EventContext context) {
    if (context instanceof SyncObjectsRequest) {
      SyncObjectsRequest request = (SyncObjectsRequest) context;
      doSyncObjectsRequest(request);
    } else if (context instanceof ObjectSyncMessage) {
      ObjectSyncMessage syncMsg = (ObjectSyncMessage) context;
      doSyncObjectsResponse(syncMsg);
    } else if (context instanceof RelayedCommitTransactionMessage) {
      RelayedCommitTransactionMessage commitMessage = (RelayedCommitTransactionMessage) context;
      Set serverTxnIDs = processCommitTransactionMessage(commitMessage);
      ackTransactions(commitMessage, serverTxnIDs);
    } else {
      throw new AssertionError("Unknown context type : " + context.getClass().getName() + " : " + context);
    }
  }

  //TODO:: Implement throttling between active/passive
  private void ackTransactions(RelayedCommitTransactionMessage commitMessage, Set serverTxnIDs) {
    ServerTxnAckMessage msg = ServerTxnAckMessageFactory.createServerTxnAckMessage(commitMessage, serverTxnIDs);
    sendSink.add(msg);
  }

  // TODO::recycle msg after use
  private Set processCommitTransactionMessage(RelayedCommitTransactionMessage commitMessage) {
    try {
      final TransactionBatchReader reader = batchReaderFactory.newTransactionBatchReader(commitMessage);
      ServerTransaction txn;
      List txns = new ArrayList(reader.getNumTxns());
      Set serverTxnIDs = new HashSet(reader.getNumTxns());
      while ((txn = reader.getNextTransaction()) != null) {
        txns.add(txn);
        serverTxnIDs.add(txn.getServerTransactionID());
      }
      // TODO:: remove channelID.NULL_ID thingy
      txnObjectMgr.addTransactions(ChannelID.NULL_ID, txns, Collections.EMPTY_LIST);
      return serverTxnIDs;
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private void doSyncObjectsResponse(ObjectSyncMessage syncMsg) {
    ArrayList txns = new ArrayList(1);
    ServerTransaction txn = ServerTransactionFactory.createTxnFrom(syncMsg);
    txns.add(txn);
    // TODO:: remove channelID.NULL_ID thingy
    txnObjectMgr.addTransactions(ChannelID.NULL_ID, txns, Collections.EMPTY_LIST);
  }

  // TODO:: Update stats so that admin console reflects these data
  private void doSyncObjectsRequest(SyncObjectsRequest request) {
    NodeID nodeID = request.getNodeID();
    ManagedObjectSyncContext lookupContext = l2ObjectStateMgr.getSomeObjectsToSyncContext(nodeID, 500, dehydrateSink);
    // TODO:: Remove ChannelID from ObjectManager interface
    if (lookupContext != null) {
      objectManager.lookupObjectsAndSubObjectsFor(ChannelID.NULL_ID, lookupContext, -1);
    }
  }

  public void initialize(ConfigurationContext context) {
    super.initialize(context);
    ServerConfigurationContext oscc = (ServerConfigurationContext) context;
    this.batchReaderFactory = oscc.getTransactionBatchReaderFactory();
    this.objectManager = oscc.getObjectManager();
    this.txnObjectMgr = oscc.getTransactionalObjectManager();
    this.dehydrateSink = oscc.getStage(ServerConfigurationContext.OBJECTS_SYNC_DEHYDRATE_STAGE).getSink();
    this.sendSink = oscc.getStage(ServerConfigurationContext.OBJECTS_SYNC_SEND_STAGE).getSink();
  }

}
