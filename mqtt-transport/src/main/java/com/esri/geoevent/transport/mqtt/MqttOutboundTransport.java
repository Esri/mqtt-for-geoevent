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
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.TransportDefinition;
import com.esri.ges.transport.TransportType;
import java.nio.ByteBuffer;

public class MqttOutboundTransport extends MqttTransportBase implements GeoEventAwareTransport
{
  public MqttOutboundTransport(TransportDefinition definition) throws ComponentException
  {
    super(definition);
    TransportType transportType = definition.getTransportType();
    if (transportType == null || !transportType.equals(TransportType.OUTBOUND)) {
      throw new ComponentException(LOGGER.translate("TRANSPORT_INIT_ERROR", this.getClass().getName()));
    }
  }

  @Override
  public void receive(ByteBuffer buffer, String channelId)
  {
    receive(buffer, channelId, null);
  }

  @Override
  public void receive(ByteBuffer buffer, String channelID, GeoEvent geoEvent)
  {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    try
    {
      publish(bytes, geoEvent);
      setErrorMessage(null);
    } catch (Exception error) {
      String errorMsg = LOGGER.translate("ERROR_PUBLISHING", error.getMessage());
      setErrorState(errorMsg, error);
    }
  }
}
