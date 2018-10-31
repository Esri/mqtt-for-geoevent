package com.esri.geoevent.transport.mqtt;

import java.net.MalformedURLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.esri.ges.core.property.Property;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.transport.TransportBase;

/**
 * Abstracts the MQTT properties, connection, and disconnect.
 * 
 * @author eric5946
 */
public class MqttTransportUtil
{
	private static final BundleLogger log = BundleLoggerFactory.getLogger(MqttTransportUtil.class);

	private int port;
	private String host;
	private boolean ssl;
	private String topic;
	private int qos;
	private String username;
	private char[] password;
	private boolean retain;

	/**
	 * Parse out the MQTT properties and store them for connections.
	 * 
	 * @param transport
	 *            The BaseTrasport that is supplying the properties (input or
	 *            output)
	 * @throws Exception
	 */
	public void applyProperties(TransportBase transport) throws Exception
	{
		ssl = false;
		port = 1883;
		host = "iot.eclipse.org"; // default

		String hostValue = getStringPropertyValue(transport, "host");
		if (hostValue != null)
		{
			Matcher matcher = Pattern
					.compile("^(?:(tcp|ssl)://)?([-.a-z0-9]+)(?::([0-9]+))?$", Pattern.CASE_INSENSITIVE)
					.matcher(hostValue);
			if (matcher.matches())
			{
				ssl = "ssl".equalsIgnoreCase(matcher.group(1));
				host = matcher.group(2);
				port = matcher.start(3) > -1 ? Integer.parseInt(matcher.group(3)) : ssl ? 8883 : 1883;
			} else
			{
				throw new MalformedURLException("Invalid MQTT Host URL '" + hostValue + "'");
			}
		}

		topic = getStringPropertyValue(transport, "topic");
		username = getStringPropertyValue(transport, "username");

		// Get the password as a DecryptedValue an convert it to an Char array.
		password = null;
		if (getStringPropertyValue(transport, "password") != null)
		{
			String value = (String) transport.getProperty("password").getDecryptedValue();
			if (value != null)
			{
				password = value.toCharArray();
			}
		}

		// qos is an integer in the set [0, 1, or 2]
		qos = 0;
		if (transport.getProperty("qos").isValid())
		{
			try
			{
				int value = Integer.parseInt(transport.getProperty("qos").getValueAsString());
				if ((value >= 0) && (value <= 2))
				{
					qos = value;
				} else if (log.isWarnEnabled())
				{
					log.warn("Property value for QOS is not valid (" + value + "). Using default of 0 instead.");
				}
			} catch (NumberFormatException e)
			{
				throw e; // shouldn't ever happen but needed to be string for pick list
			}
		}

		// retain is a boolean value
		retain = false;
		if (transport.hasProperty("retain"))
		{
			Property value = transport.getProperty("retain");
			if (value != null)
			{
				Boolean boolValue = (Boolean) value.getValue();
				if (boolValue != null)
				{
					retain = boolValue.booleanValue();
				}
			}
		}

		if (log.isDebugEnabled())
		{
			log.debug("Applied Properties: SSL='" + ssl + "' PORT='" + port + "' HOST='" + host + "' TOPIC='" + topic
					+ "' USERNAME='" + username + "' PASSWORD is " + (password == null ? "VALID" : "IN-VALID")
					+ " QOS='" + qos + "' RETAIN='" + retain + "'");
		}
	}

	/**
	 * @return a new MQTT Client that is connected without a registered callback
	 *         (used for outbound connections).
	 * @throws MqttException
	 */
	public MqttClient createMqttClient() throws MqttException
	{
		return createMqttClient(null);
	}

	/**
	 * @param callback
	 *            An MQTT Callback to handle received messages from subscriptions.
	 *            Can be null for outbound connections.
	 * @return a new MQTT Client that is connected
	 * @throws MqttException
	 */
	public MqttClient createMqttClient(MqttCallback callback) throws MqttException
	{
		String url = (ssl ? "ssl://" : "tcp://") + host + ":" + Integer.toString(port);
		if (log.isDebugEnabled())
		{
			log.debug("Creating MQTT Broker client at URL '" + url + "'");
		}
		MqttClient mqttClient = new MqttClient(url, MqttClient.generateClientId(), new MemoryPersistence());
		if (callback != null)
		{
			if (log.isDebugEnabled())
			{
				log.debug("Setting MQTT callback to receive messages");
			}
			mqttClient.setCallback(callback);
		}

		MqttConnectOptions options = new MqttConnectOptions();

		// Connect with username and password if both are available.
		if (username != null && password != null && !username.isEmpty() && password.length > 0)
		{
			if (log.isDebugEnabled())
			{
				log.debug("Connecting to MQTT Broker using credentials. Username='" + username + "'");
			}
			options.setUserName(username);
			options.setPassword(password);
		}

		if (ssl)
		{
			// Support TLS only (1.0-1.2) as even SSL 3.0 has well known exploits
			if (log.isDebugEnabled())
			{
				log.debug("Connecting to MQTT Broker using SSL. NOTE: Only TLS 1.0 to 1.2 are supported.");
			}
			java.util.Properties sslProperties = new java.util.Properties();
			sslProperties.setProperty("com.ibm.ssl.protocol", "TLS");
			options.setSSLProperties(sslProperties);
		}

		options.setCleanSession(true);

		if (log.isTraceEnabled())
		{
			log.trace("Attempting to connect to MQTT Broker now...");
		}
		mqttClient.connect(options);
		if (log.isInfoEnabled())
		{
			log.info("Connection " + (mqttClient.isConnected() ? "SUCCEEDED" : "FAILED") + " to MQTT Broker.");
		}
		return mqttClient;
	}

	/**
	 * Checks to be sure the client is connected, then disconnects and closes the
	 * client.
	 * 
	 * @param mqttClient
	 *            The client to disconnect
	 */
	public void disconnectMqtt(MqttClient mqttClient)
	{
		if (log.isTraceEnabled())
		{
			log.trace("Disconnecting MQTT client...");
		}
		try
		{
			if (mqttClient != null)
			{
				if (mqttClient.isConnected())
				{
					mqttClient.disconnect();
					mqttClient.close();
				} else if (log.isTraceEnabled())
				{
					log.trace("MQTT client is already disconnected, can't disconnect.");
				}
			} else if (log.isTraceEnabled())
			{
				log.trace("MQTT client is already null, can't disconnect.");
			}
		} catch (MqttException e)
		{
			log.error("UNABLE_TO_CLOSE", e);
		} finally
		{
			mqttClient = null;
		}
	}

	/**
	 * Each topic must contain at least 1 character and the topic string permits
	 * empty spaces. The forward slash alone is a valid topic. The $-symbol topics
	 * are reserved for internal statistics of the MQTT broker ($ is not permitted).
	 * 
	 * Topics are case-sensitive. For example, "myhome/temperature" and
	 * "MyHome/Temperature" are two different topics.
	 * 
	 * @return True if the topic is valid. False if it cannot be used as a MQTT
	 *         topic.
	 */
	public boolean isTopicValid(String topic)
	{
		boolean result = false;
		if (topic != null)
		{
			// Can't be null
			if (!topic.isEmpty())
			{
				// Can't be empty string
				if (!topic.contains("$"))
				{
					// Can't contain the '$' char
					if (topic.length() > 1 || topic.equals("/"))
					{
						// Is longer than 1 character or equals '/'
						result = true;
					} else
					{
						log.error("GeoEvent TOPIC = '" + topic
								+ "'. ERROR, it must be more than one character long or equal to '/'.");
					}
				} else
				{
					log.error("GeoEvent TOPIC = '" + topic + "'. ERROR, cannot contain the '$' symbol.");
				}
			} else
			{
				log.error("GeoEvent TOPIC cannot be EMPTY.");
			}
		} else
		{
			log.error("GeoEvent TOPIC cannot be NULL.");
		}
		return result;
	}

	/**
	 * Checks if a property is valid, then makes sure it isn't null or empty.
	 * 
	 * @param transport
	 *            The transport with the property values
	 * @param propertyName
	 *            The name of the property to get
	 * @return Null if the property is not valdi, null, or empty string. The
	 *         property string value otherwise.
	 */
	public String getStringPropertyValue(TransportBase transport, String propertyName)
	{
		String result = null;
		if (transport.getProperty(propertyName).isValid())
		{
			String value = (String) transport.getProperty(propertyName).getValue();
			if (value != null)
			{
				value = value.trim();
				if (!value.isEmpty())
				{
					result = value.trim();
				} else if (log.isTraceEnabled())
				{
					log.trace("Property '" + propertyName + "' is EMPTY.");
				}
			} else if (log.isTraceEnabled())
			{
				log.trace("Property '" + propertyName + "' is NULL.");
			}
		} else if (log.isTraceEnabled())
		{
			log.trace("Property '" + propertyName + "' is NOT VALID.");
		}
		return result;
	}

	/**
	 * @return the topic
	 */
	public String getTopic()
	{
		return topic;
	}

	/**
	 * @return the qos
	 */
	public int getQos()
	{
		return qos;
	}

	/**
	 * @return the retain
	 */
	public boolean isRetain()
	{
		return retain;
	}
}
