/*
  Copyright 1995-2015 Esri

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

import java.nio.ByteBuffer;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.GeoEventAwareTransport;
import com.esri.ges.transport.OutboundTransportBase;
import com.esri.ges.transport.TransportDefinition;

public class MqttOutboundTransport extends OutboundTransportBase implements GeoEventAwareTransport
{

	private static final BundleLogger log = BundleLoggerFactory.getLogger(MqttOutboundTransport.class);

	private MqttTransportUtil mqtt = new MqttTransportUtil();
	private MqttClient mqttClient;

	public MqttOutboundTransport(TransportDefinition definition) throws ComponentException
	{
		super(definition);
		if (log.isTraceEnabled())
		{
			log.trace("Created MQTT Outbound Transport");
		}
	}

	@Override
	public void start() throws RunningException
	{
		try
		{
			if (log.isTraceEnabled())
			{
				log.trace("Starting MQTT Outbound Transport");
			}
			setRunningState(RunningState.STARTING);

			mqtt.applyProperties(this);
			mqttClient = mqtt.createMqttClient();

			if (log.isTraceEnabled())
			{
				log.trace("Started MQTT Outbound Transport");
			}
			setRunningState(RunningState.STARTED);

		} catch (Exception e)
		{
			log.error("INIT_ERROR", e, this.getClass().getName());
			setRunningState(RunningState.ERROR);
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
		if (log.isTraceEnabled())
		{
			log.trace("receive(" + channelID + "): " + geoEvent);
		}
		String topic = mqtt.getTopic();
		if (geoEvent != null && topic.contains("$"))
		{
			// Do field value substitution like "${field1}/${field2}"
			if (log.isTraceEnabled())
			{
				log.trace("received geoEvent, creating output topic from field values using template '" + topic + "'.");
			}
			topic = geoEvent.formatString(topic);
		}
		
		if (log.isTraceEnabled())
		{
			log.trace("Publishing outgoing bytes to topic '" + topic + "'.");
		}

		if (mqtt.isTopicValid(topic))
		{
			try
			{
				if (mqttClient == null || !mqttClient.isConnected())
					mqttClient = mqtt.createMqttClient();

				byte[] b = new byte[buffer.remaining()];
				buffer.get(b);

				mqttClient.publish(topic, b, mqtt.getQos(), mqtt.isRetain());
			} catch (Exception e)
			{
				log.error("ERROR_PUBLISHING", e);
			}
		} else
		{
			log.error(
					"GeoEvent Topic '" + topic + "' is not valid, GeoEvent not published to MQTT output: " + geoEvent);
		}
	}

	@Override
	public synchronized void stop()
	{
		if (log.isTraceEnabled())
		{
			log.trace("Stopping MQTT output...");
		}
		setRunningState(RunningState.STOPPING);

		try
		{
			mqtt.disconnectMqtt(mqttClient);
		} finally
		{
			mqttClient = null;
		}

		if (log.isTraceEnabled())
		{
			log.trace("Stopped MQTT output.");
		}
		setRunningState(RunningState.STOPPED);
	}

}
