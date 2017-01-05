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
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.component.RunningException;
import com.esri.ges.core.component.RunningState;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.InboundTransportBase;
import com.esri.ges.transport.TransportDefinition;

public class MqttInboundTransport extends InboundTransportBase implements Runnable
{

	private static final BundleLogger	log			= BundleLoggerFactory.getLogger(MqttInboundTransport.class);

	private Thread										thread	= null;
	private boolean										secure;
	private int												port;
	private String										host;
	private String										topic;
	private int												qos;
	private MqttClient								mqttClient;
	private String										username;
	private char[]										password;

	public MqttInboundTransport(TransportDefinition definition) throws ComponentException
	{
		super(definition);
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
			setRunningState(RunningState.STARTING);
			thread = new Thread(this);
			thread.start();
		}
		catch (Exception e)
		{
			log.error("UNEXPECTED_ERROR_STARTING", e);
			stop();
		}
	}

	@Override
	public void run()
	{
		this.receiveData();
	}

	private void receiveData()
	{
		try
		{
			applyProperties();
			setRunningState(RunningState.STARTED);

			String url = (secure ? "ssl://" : "tcp://") + host + ":" + Integer.toString(port);
			mqttClient = new MqttClient(url, MqttClient.generateClientId(), new MemoryPersistence());

			mqttClient.setCallback(new MqttCallback()
				{

					@Override
					public void messageArrived(String topic, MqttMessage message) throws Exception
					{
						try
						{
							receive(message.getPayload());
						}
						catch (RuntimeException e)
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

			MqttConnectOptions options = new MqttConnectOptions();

			// Connect with username and password if both are available.
			if (username != null && password != null && !username.isEmpty() && password.length > 0)
			{
				options.setUserName(username);
				options.setPassword(password);
			}

			if (secure)
			{
				// Support TLS only (1.0-1.2) as even SSL 3.0 has well known exploits
				java.util.Properties sslProperties = new java.util.Properties();
				sslProperties.setProperty("com.ibm.ssl.protocol", "TLS");
				options.setSSLProperties(sslProperties);
			}

			options.setCleanSession(true);
			mqttClient.connect(options);
			mqttClient.subscribe(topic, qos);

		}
		catch (Throwable ex)
		{
			log.error("UNEXPECTED_ERROR", ex);
			setRunningState(RunningState.ERROR);
		}
	}

	private void receive(byte[] bytes)
	{
		if (bytes != null && bytes.length > 0)
		{
			String str = new String(bytes);
			str = str + '\n';
			byte[] newBytes = str.getBytes();

			ByteBuffer bb = ByteBuffer.allocate(newBytes.length);
			try
			{
				bb.put(newBytes);
				bb.flip();
				byteListener.receive(bb, "");
				bb.clear();
			}
			catch (BufferOverflowException boe)
			{
				log.error("BUFFER_OVERFLOW_ERROR", boe);
				bb.clear();
				setRunningState(RunningState.ERROR);
			}
			catch (Exception e)
			{
				log.error("UNEXPECTED_ERROR2", e);
				stop();
				setRunningState(RunningState.ERROR);
			}
		}
	}

	private void applyProperties() throws Exception
	{
		secure = false;
		if (getProperty("secure").isValid()) {
			secure = (Boolean) getProperty("secure").getValue();
		}
		if (secure)
		{
			port = 8883;
			if (getProperty("securePort").isValid())
			{
				int value = (Integer) getProperty("securePort").getValue();
				if (value > 0 && value != port) {
					port = value;
				}
			}
		}
		else
		{
			port = 1883;
			if (getProperty("port").isValid())
			{
				int value = (Integer) getProperty("port").getValue();
				if (value > 0 && value != port) {
					port = value;
				}
			}
		}

		host = "iot.eclipse.org"; // default
		if (getProperty("host").isValid())
		{
			String value = (String) getProperty("host").getValue();
			if (!value.trim().equals(""))
			{
				host = value;
			}
		}

		topic = "topic/sensor"; // default
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

		qos = 0;
		if (getProperty("qos").isValid()) {
			try
			{
				int value = Integer.parseInt(getProperty("qos").getValueAsString());
				if ((value >= 0) && (value <= 2)) {
					qos = value;
				}
			}
			catch (NumberFormatException e)
			{
				throw e; // shouldn't ever happen
			}
		}
	}

	public synchronized void stop()
	{
		try
		{
			if (this.mqttClient != null)
			{
				this.mqttClient.disconnect();
				this.mqttClient.close();
			}
		}
		catch (MqttException ex)
		{
			log.error("UNABLE_TO_CLOSE", ex);
		}
		setRunningState(RunningState.STOPPED);
	}

	@Override
	public boolean isClusterable()
	{
		return false;
	}
}
