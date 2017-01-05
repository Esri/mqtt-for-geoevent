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

import java.net.MalformedURLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

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

	private static final BundleLogger	log	= BundleLoggerFactory.getLogger(MqttOutboundTransport.class);

	private int												port;
	private String										host;
	private boolean										ssl;
	private String										topic;
	private MqttClient								mqttClient;
	private String										username;
	private char[]										password;

	public MqttOutboundTransport(TransportDefinition definition) throws ComponentException
	{
		super(definition);
	}

	@Override
	public void start() throws RunningException
	{
		try
		{
			setRunningState(RunningState.STARTING);
			applyProperties();
			connectMqtt();
			setRunningState(RunningState.STARTED);
		}
		catch (Exception e)
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
		String topic = this.topic;
		if (geoEvent != null && topic.contains("$"))
		{
			topic = geoEvent.formatString(topic);
			if (topic.isEmpty() || topic.startsWith("$"))
			{
				return;
			}
		}

		try
		{
			if (mqttClient == null || !mqttClient.isConnected())
				connectMqtt();

			byte[] b = new byte[buffer.remaining()];
			buffer.get(b);

			mqttClient.publish(topic, b, 2, true);
		}
		catch (Exception e)
		{
			log.error("ERROR_PUBLISHING", e);
		}
	}

	private void connectMqtt() throws MqttException
	{
		String url = (ssl ? "ssl://" : "tcp://") + host + ":" + Integer.toString(port);
		mqttClient = new MqttClient(url, MqttClient.generateClientId(), new MemoryPersistence());

		MqttConnectOptions options = new MqttConnectOptions();

		// Connect with username and password if both are available.
		if (username != null && password != null && !username.isEmpty() && password.length > 0)
		{
			options.setUserName(username);
			options.setPassword(password);
		}

		if (ssl)
		{
			// Support TLS only (1.0-1.2) as even SSL 3.0 has well known exploits
			java.util.Properties sslProperties = new java.util.Properties();
			sslProperties.setProperty("com.ibm.ssl.protocol", "TLS");
			options.setSSLProperties(sslProperties);
		}

		options.setCleanSession(true);
		mqttClient.connect(options);
	}

	private void applyProperties() throws Exception
	{
		ssl = false;
		port = 1883;
		host = "iot.eclipse.org"; // default
		if (getProperty("host").isValid())
		{
			String value = (String) getProperty("host").getValue();
			if (!value.trim().equals(""))
			{
				Matcher matcher = Pattern.compile("^(?:(tcp|ssl)://)?([-.a-z0-9]+)(?::([0-9]+))?$", Pattern.CASE_INSENSITIVE).matcher(value);
				if (matcher.matches())
				{
					ssl = "ssl".equalsIgnoreCase(matcher.group(1));
					host = matcher.group(2);
					port = matcher.start(3) > -1 ? Integer.parseInt(matcher.group(3)) :
									ssl ? 8883 : 1883; 
				}
				else
				{
					throw new MalformedURLException("Invalid MQTT Host URL");
				}
			}
		}

		topic = "topic/actuators/light"; // default
		if (getProperty("topic").isValid())
		{
			String value = (String) getProperty("topic").getValue();
			if (!value.trim().equals(""))
			{
				topic = value;
			}
		}
		
		//Get the username as a simple String.
		username = null;
		if (getProperty("username").isValid())
		{
			String value = (String) getProperty("username").getValue();
			if (value != null)
			{
				username = value.trim();
			}
		}

		//Get the password as a DecryptedValue an convert it to an Char array.
		password = null;
		if (getProperty("password").isValid())
		{
			String value = (String) getProperty("password").getDecryptedValue();
			if (value != null)
			{
				password = value.toCharArray();
			}
		}
	}

	@Override
	public synchronized void stop()
	{
		setRunningState(RunningState.STOPPING);

		if (this.mqttClient != null)
			disconnectMqtt();

		setRunningState(RunningState.STOPPED);
	}

	private void disconnectMqtt()
	{
		try
		{
			if (mqttClient != null)
			{
				if (mqttClient.isConnected())
				{
					mqttClient.disconnect();
					mqttClient.close();
				}
			}
		}
		catch (MqttException e)
		{
			log.error("UNABLE_TO_CLOSE", e);
		}
		finally
		{
			mqttClient = null;
		}
	}

}
