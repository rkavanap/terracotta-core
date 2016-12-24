/*
 *
 *  The contents of this file are subject to the Terracotta Public License Version
 *  2.0 (the "License"); You may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *
 *  http://terracotta.org/legal/terracotta-public-license.
 *
 *  Software distributed under the License is distributed on an "AS IS" basis,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 *  the specific language governing rights and limitations under the License.
 *
 *  The Covered Software is Terracotta Core.
 *
 *  The Initial Developer of the Covered Software is
 *  Terracotta, Inc., a Software AG company
 *
 */
package com.tc.objectserver.handler;

import com.tc.async.api.EventHandlerException;
import com.tc.l2.msg.ReplicationAddPassiveIntent;
import com.tc.l2.msg.ReplicationIntent;
import com.tc.l2.msg.ReplicationMessage;
import com.tc.l2.msg.ReplicationReplicateMessageIntent;
import com.tc.l2.msg.SyncReplicationActivity;
import com.tc.net.ClientID;
import com.tc.net.NodeID;
import com.tc.net.groups.GroupManager;
import com.tc.object.ClientInstanceID;
import com.tc.object.EntityDescriptor;
import com.tc.object.EntityID;
import com.tc.object.tx.TransactionID;
import com.tc.util.Assert;
import com.tc.util.concurrent.SetOnceFlag;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;


public class ReplicationSenderTest {
  
  NodeID node = mock(NodeID.class);
  GroupManager groupMgr = mock(GroupManager.class);
  List<ReplicationIntent> collector = new LinkedList<>();
  ReplicationSender testSender = new ReplicationSender(groupMgr);
  EntityID entity = EntityID.NULL_ID;
  int concurrency = 1;
  
  public ReplicationSenderTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() throws Exception {
    doAnswer((invoke)-> {
      Object[] args = invoke.getArguments();
      collector.add(ReplicationReplicateMessageIntent.createReplicatedMessageEnvelope((NodeID)args[0], (ReplicationMessage)args[1], null));
      return null;
    }).when(groupMgr).sendTo(Matchers.any(NodeID.class), Matchers.any(ReplicationMessage.class));
  }
  
  private void makeAndSendSequence(Collection<SyncReplicationActivity.ActivityType> list) throws Exception {
    list.stream().forEach(msg->{
      ReplicationMessage rep = ReplicationMessage.createActivityContainer(makeMessage(msg));
      try {
        testSender.handleEvent(ReplicationReplicateMessageIntent.createReplicatedMessageEnvelope(node, rep, null));
      } catch (EventHandlerException exp) {
        throw new RuntimeException(exp);
      }
    });
  }
  
  private SyncReplicationActivity makeMessage(SyncReplicationActivity.ActivityType type) {
    switch (type) {
      case CREATE_ENTITY:
      case DESTROY_ENTITY:
      case INVOKE_ACTION:
      case NOOP:
      case RECONFIGURE_ENTITY:
        ClientID source = new ClientID(1);
        return SyncReplicationActivity.createReplicatedMessage(new EntityDescriptor(entity, ClientInstanceID.NULL_ID, 1), source, TransactionID.NULL_ID, TransactionID.NULL_ID, type, new byte[0], concurrency, "");
      case SYNC_BEGIN:
        return SyncReplicationActivity.createStartSyncMessage();
      case SYNC_END:
        return SyncReplicationActivity.createEndSyncMessage(new byte[0]);
      case SYNC_ENTITY_BEGIN:
        return SyncReplicationActivity.createStartEntityMessage(entity, 1, new byte[0], 0);
      case SYNC_ENTITY_CONCURRENCY_BEGIN:
        return SyncReplicationActivity.createStartEntityKeyMessage(entity, 1, concurrency);
      case SYNC_ENTITY_CONCURRENCY_END:
        return SyncReplicationActivity.createEndEntityKeyMessage(entity, 1, concurrency++);
      case SYNC_ENTITY_CONCURRENCY_PAYLOAD:
        return SyncReplicationActivity.createPayloadMessage(entity, 1, concurrency, new byte[0], "");
      case SYNC_ENTITY_END:
        return SyncReplicationActivity.createEndEntityMessage(entity, 1);
      default:
        throw new AssertionError("bad message type");
    }
  }
  
  @After
  public void tearDown() {
    
  }
  
  @Test
  public void filterSCDC() throws Exception {  // Sync-Create-Delete-Create
    entity = new EntityID("TEST", "test");
    List<ReplicationMessage> origin = new LinkedList<>();
    List<ReplicationMessage> validation = new LinkedList<>();
    buildTest(origin, validation, SyncReplicationActivity.createStartMessage(), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), true);  
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_PAYLOAD), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);

    origin.stream().forEach(msg-> {
      try {
        testSender.handleEvent(ReplicationReplicateMessageIntent.createReplicatedMessageEnvelope(node, msg, null));
      } catch (EventHandlerException h) {
        throw new RuntimeException(h);
      }
    });
    System.err.println("filter SDSC");
    validateCollector(validation);
  }  
  
  @Test
  public void filterCDC() throws Exception {  // Create-Delete-Create
    entity = new EntityID("TEST", "test");
 //  creates and sync can no longer intermix
    List<ReplicationMessage> origin = new LinkedList<>();
    List<ReplicationMessage> validation = new LinkedList<>();
    buildTest(origin, validation, SyncReplicationActivity.createStartMessage(), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_BEGIN), false);
 // this create is not part of the sync set so everything should pass through
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.CREATE_ENTITY), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);  // invoke actions are valid since the stream is working off the create

    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
// create and destroy can no longer happen concurrently with sync
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);

    origin.stream().forEach(msg-> {
      try {
        testSender.handleEvent(ReplicationReplicateMessageIntent.createReplicatedMessageEnvelope(node, msg, null));
      } catch (EventHandlerException h) {
        throw new RuntimeException(h);
      }
    });
    
    System.err.println("filter CDC");
    validateCollector(validation);
  }  

  @Test
  public void filterValidation() throws Exception {
    entity = new EntityID("TEST", "test");
    List<ReplicationMessage> origin = new LinkedList<>();
    List<ReplicationMessage> validation = new LinkedList<>();
    buildTest(origin, validation, SyncReplicationActivity.createStartMessage(), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.CREATE_ENTITY), true);//  this will be replicated, it's up to the passive to drop it on the floor if it hasn't seen a sync yet
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), true);   
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_PAYLOAD), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);

    origin.stream().forEach(msg-> {
      try {
        testSender.handleEvent(ReplicationReplicateMessageIntent.createReplicatedMessageEnvelope(node, msg, null));
      } catch (EventHandlerException h) {
        throw new RuntimeException(h);
      }
    });
    
    validateCollector(validation);
  }
  
  @Test
  public void validateSyncState() {
    entity = new EntityID("TEST", "test");
    List<ReplicationMessage> origin = new LinkedList<>();
    List<ReplicationMessage> validation = new LinkedList<>();
    buildTest(origin, validation, SyncReplicationActivity.createStartMessage(), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.CREATE_ENTITY), false);//  this will be replicated, it's up to the passive to drop it on the floor if it hasn't seen a sync yet
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), true);   
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_BEGIN), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_PAYLOAD), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.NOOP), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_CONCURRENCY_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), true);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_ENTITY_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.SYNC_END), false);
    buildTest(origin, validation, makeMessage(SyncReplicationActivity.ActivityType.INVOKE_ACTION), false);

    SetOnceFlag started = new SetOnceFlag();
    SetOnceFlag finished = new SetOnceFlag();
    origin.stream().forEach(msg-> {
      try {
        if (msg.getReplicationType() == SyncReplicationActivity.ActivityType.SYNC_BEGIN) {
          started.set();
        } else if (msg.getReplicationType() == SyncReplicationActivity.ActivityType.SYNC_END) {
          finished.set();
        }
        SetOnceFlag sent = new SetOnceFlag();
        SetOnceFlag notsent = new SetOnceFlag();
        ReplicationIntent intent = (msg.getReplicationType() == SyncReplicationActivity.ActivityType.SYNC_START) ? 
                ReplicationAddPassiveIntent.createAddPassiveEnvelope(node, msg, ()->sent.set(), ()->notsent.set()) :
                ReplicationReplicateMessageIntent.createReplicatedMessageDebugEnvelope(node, msg, ()->sent.set(), ()->notsent.set());
        testSender.handleEvent(intent);
        Assert.assertEquals(started.isSet() + " " + finished.isSet(), started.isSet() && !finished.isSet(), testSender.isSyncOccuring(node));
        if (!testSender.isSyncOccuring(node) && msg.getReplicationType() != SyncReplicationActivity.ActivityType.NOOP) {
          Assert.assertTrue(msg, sent.isSet());
        }
      } catch (EventHandlerException h) {
        throw new RuntimeException(h);
      }
    });
    
    validateCollector(validation);
  }
  
  private void validateCollector(Collection<ReplicationMessage> valid) {
    Iterator<ReplicationMessage> next = valid.iterator();
    collector.stream().forEach(bareIntent->{
      ReplicationReplicateMessageIntent cmsg = (ReplicationReplicateMessageIntent) bareIntent;
      if ((cmsg.getMessage().getReplicationType() != SyncReplicationActivity.ActivityType.SYNC_START) && (cmsg.getMessage().getReplicationType() != SyncReplicationActivity.ActivityType.NOOP)) {
        ReplicationMessage vmsg = next.next();
        if (vmsg.getReplicationType() != SyncReplicationActivity.ActivityType.SYNC_BEGIN &&
            vmsg.getReplicationType() != SyncReplicationActivity.ActivityType.SYNC_END) {
          Assert.assertEquals(vmsg + "!=" + cmsg.getMessage(), vmsg.getEntityID(), cmsg.getMessage().getEntityID());
        }
        Assert.assertEquals(vmsg + "!=" + cmsg.getMessage(), vmsg.getReplicationType(), cmsg.getMessage().getReplicationType());
        Assert.assertEquals(vmsg + "!=" + cmsg.getMessage(), vmsg.getConcurrency(), cmsg.getMessage().getConcurrency());
        System.err.println(vmsg.getReplicationType() + " on " + vmsg.getEntityID());
      }
    });
  }
  
  private void buildTest(List<ReplicationMessage> origin, List<ReplicationMessage> validation, SyncReplicationActivity activity, boolean filtered) {
    ReplicationMessage msg = ReplicationMessage.createActivityContainer(activity);
    origin.add(msg);
    if (!filtered) validation.add(msg);
  }
}
