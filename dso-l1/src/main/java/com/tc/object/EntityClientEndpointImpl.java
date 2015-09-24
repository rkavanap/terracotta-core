package com.tc.object;

import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.InvocationBuilder;

import com.tc.entity.VoltronEntityMessage;
import com.tc.util.Assert;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Future;


public class EntityClientEndpointImpl implements EntityClientEndpoint {
  private final InvocationHandler invocationHandler;
  private final byte[] configuration;
  private final EntityDescriptor entityDescriptor;
  private final Runnable closeHook;
  private EndpointDelegate delegate;

  /**
   * @param entityDescriptor The server-side entity and corresponding client-side instance ID.
   * @param invocationHandler Called to handle "invokeAction" requests made on this end-point.
   * @param entityConfiguration Opaque byte[] describing how to configure the entity to be built on top of this end-point.
   * @param closeHook A Runnable which will be run last when the end-point is closed.
   */
  public EntityClientEndpointImpl(EntityDescriptor entityDescriptor, InvocationHandler invocationHandler, byte[] entityConfiguration, Runnable closeHook) {
    this.entityDescriptor = entityDescriptor;
    this.invocationHandler = invocationHandler;
    this.configuration = entityConfiguration;
    Assert.assertNotNull("Endpoint didn't have close hook", closeHook);
    this.closeHook = closeHook;
  }

  @Override
  public byte[] getEntityConfiguration() {
    return configuration;
  }

  @Override
  public void setDelegate(EndpointDelegate delegate) {
    Assert.assertNull(this.delegate);
    this.delegate = delegate;
  }
  
  public void handleMessage(byte[] message) {
    if (null != this.delegate) {
      this.delegate.handleMessage(message);
    }
  }

  @Override
  public InvocationBuilder beginInvoke() {
    return new InvocationBuilderImpl();
  }

  private class InvocationBuilderImpl implements InvocationBuilder {
    private boolean invoked = false;
    private byte[] payload;
    private final Set<VoltronEntityMessage.Acks> acks = EnumSet.noneOf(VoltronEntityMessage.Acks.class);
    private boolean requiresReplication = false;

    // TODO: fill in durability/consistency options here.

    @Override
    public synchronized InvocationBuilderImpl payload(byte[] pl) {
      checkInvoked();
      this.payload = pl;
      return this;
    }

    @Override
    public InvocationBuilder ackReceived() {
      acks.add(VoltronEntityMessage.Acks.RECEIVED);
      return this;
    }

    @Override
    public InvocationBuilder ackCompleted() {
      acks.add(VoltronEntityMessage.Acks.APPLIED);
      return this;
    }

    @Override
    public InvocationBuilder replicate(boolean requiresReplication) {
      this.requiresReplication = requiresReplication;
      return this;
    }

    @Override
    public synchronized Future<byte[]> invoke() {
      checkInvoked();
      invoked = true;
      return invocationHandler.invokeAction(entityDescriptor, this.acks, this.requiresReplication, this.payload);
    }

    private void checkInvoked() {
      if (invoked) {
        throw new IllegalStateException("Already invoked");
      }
    }
  }

  @Override
  public byte[] getExtendedReconnectData() {
    byte[] reconnectData = null;
    if (null != this.delegate) {
      reconnectData = this.delegate.createExtendedReconnectData();
    }
    if (null == reconnectData) {
      reconnectData = new byte[0];
    }
    return reconnectData;
  }

  @Override
  public void close() {
    this.closeHook.run();
  }

  @Override
  public void didCloseUnexpectedly() {
    // NOTE:  We do NOT run the close hook in this situation since it is assuming that the close was requested and that the
    // underlying connection is still viable.
    if (null != this.delegate) {
      this.delegate.didDisconnectUnexpectedly();
    }
  }
}
