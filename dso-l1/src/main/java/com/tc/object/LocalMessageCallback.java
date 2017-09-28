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
package com.tc.object;

import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;
import org.terracotta.entity.MessageCallback;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

public class LocalMessageCallback<M extends EntityMessage, R extends EntityResponse> implements LocalCallback {
  private final MessageCallback<R> messageCallback;
  private final MessageCodec<M, R> codec;

  public LocalMessageCallback(MessageCallback<R> messageCallback, MessageCodec<M, R> codec) {
    this.messageCallback = messageCallback;
    this.codec = codec;
  }

  @Override
  public void onCompletion(byte[] responsePayload) {
    try {
      messageCallback.onCompletion(codec.decodeResponse(responsePayload));
    } catch (MessageCodecException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onError(EntityException exception) {
    messageCallback.onError(exception);
  }
}