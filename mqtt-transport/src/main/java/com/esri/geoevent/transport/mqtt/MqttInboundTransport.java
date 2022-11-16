/*
  Copyright 1995-2019 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/

package com.esri.geoevent.transport.mqtt;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.messaging.ByteListener;
import com.esri.ges.transport.InboundTransport;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.transport.TransportType;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class MqttInboundTransport extends MqttTransportBase implements InboundTransport
{
  private ByteListener byteListener = null;

  public MqttInboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
    TransportType transportType = definition.getTransportType();
    if (transportType == null || !transportType.equals(TransportType.INBOUND)) {
      throw new ComponentException(LOGGER.translate("TRANSPORT_INIT_ERROR", this.getClass().getName()));
    }
  }

  @Override
  public void setByteListener(ByteListener byteListener)
  {
    this.byteListener = byteListener;
  }

  @Override
  public boolean isClusterable()
  {
    return false;
  }

  @Override
  public void messageArrived(String topic, MqttMessage message)
  {
    try
    {
      LOGGER.debug("Message arrived on topic {0}: ( {1} )", topic, message);
      byte[] bytes = message.getPayload();
      if (bytes != null && bytes.length > 0)
      {
        LOGGER.trace("Received {0} bytes", bytes.length);

        String str = new String(bytes);
        str = str + '\n';

        LOGGER.trace("Byte String received {0}", str);

        byte[] newBytes = str.getBytes();

        ByteBuffer bb = ByteBuffer.allocate(newBytes.length);
        try
        {
          bb.put(newBytes);
          bb.flip();
          byteListener.receive(bb, "");
          bb.clear();

          LOGGER.trace("{0} received bytes sent on to the adaptor.", newBytes.length);
        }
        catch (BufferOverflowException boe)
        {
          LOGGER.debug("BUFFER_OVERFLOW_ERROR", boe);
          bb.clear();
        }
        catch (Exception error)
        {
          String errorMsg = LOGGER.translate("UNEXPECTED_ERROR", error.getMessage());
          setErrorState(errorMsg, error);
        }
      }
    }
    catch (RuntimeException e)
    {
      LOGGER.debug("ERROR_PUBLISHING", e);
    }
  }
}
