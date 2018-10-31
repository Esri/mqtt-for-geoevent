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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;

public class MqttInboundTransport extends InboundTransportBase implements Runnable
{

	private static final BundleLogger log = BundleLoggerFactory.getLogger(MqttInboundTransport.class);

	private MqttTransportUtil mqtt = new MqttTransportUtil();
	private Thread thread = null;
	private MqttClient mqttClient;

	public MqttInboundTransport(TransportDefinition definition) throws ComponentException
	{
		super(definition);
		if (log.isTraceEnabled())
		{
			log.trace("Created MQTT Inbound Transport");
		}
	}

	@SuppressWarnings("incomplete-switch")
	public void start() throws RunningException
	{
		try
		{
			switch (getRunningState())
			{
			case STARTING:
			case STARTED:
			case STOPPING:
				return;
			}
			if (log.isTraceEnabled())
			{
				log.trace("Starting MQTT Inbound Transport");
			}
			setRunningState(RunningState.STARTING);
			thread = new Thread(this);
			thread.start();
		} catch (Exception e)
		{
			log.error("UNEXPECTED_ERROR_STARTING", e);
			stop();
		}
	}

	@Override
	public void run()
	{
		try
		{
			mqtt.applyProperties(this);
			mqttClient = mqtt.createMqttClient(new MqttCallback()
			{

				@Override
				public void messageArrived(String topic, MqttMessage message) throws Exception
				{
					try
					{
						if (log.isTraceEnabled())
						{
							log.trace("messageArrived: " + topic + "(" + message + ")");
						}
						receive(message.getPayload());
					} catch (RuntimeException e)
					{
						e.printStackTrace();
					}
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken token)
				{
					// not used
				}

				@Override
				public void connectionLost(Throwable cause)
				{
					log.error("CONNECTION_LOST", cause.getLocalizedMessage());
				}
			});

			mqttClient.subscribe(mqtt.getTopic(), mqtt.getQos());

			if (log.isTraceEnabled())
			{
				log.trace("Started MQTT Inbound Transport");
			}
			setRunningState(RunningState.STARTED);

		} catch (Throwable ex)
		{
			log.error("UNEXPECTED_ERROR", ex);
			setRunningState(RunningState.ERROR);
			setErrorMessage("UNEXPECTED_ERROR:" + ex.getMessage());
		}
	}

	private void receive(byte[] bytes)
	{
		if (bytes != null && bytes.length > 0)
		{
			if (log.isTraceEnabled())
			{
				log.trace("Received some bytes");
			}
			String str = new String(bytes);
			str = str + '\n';
			if (log.isDebugEnabled())
			{
				log.debug("Byte String received '" + str + "'");
			}
			byte[] newBytes = str.getBytes();

			ByteBuffer bb = ByteBuffer.allocate(newBytes.length);
			try
			{
				bb.put(newBytes);
				bb.flip();
				byteListener.receive(bb, "");
				bb.clear();
				if (log.isTraceEnabled())
				{
					log.trace("Received bytes sent to adaptor");
				}
			} catch (BufferOverflowException boe)
			{
				log.error("BUFFER_OVERFLOW_ERROR", boe);
				bb.clear();
			} catch (Exception e)
			{
				log.error("UNEXPECTED_ERROR2", e);
				stop();
				setRunningState(RunningState.ERROR);
				setErrorMessage("Unexcpected Error: " + e.getMessage());
			}
		}
	}

	public synchronized void stop()
	{
		if (log.isTraceEnabled())
		{
			log.trace("Stopping...");
		}
		try
		{
			mqtt.disconnectMqtt(mqttClient);

		} finally
		{
			mqttClient = null;
		}
		setRunningState(RunningState.STOPPED);
		if (log.isTraceEnabled())
		{
			log.trace("Stopped");
		}
	}

	@Override
	public boolean isClusterable()
	{
		return false;
	}
}
