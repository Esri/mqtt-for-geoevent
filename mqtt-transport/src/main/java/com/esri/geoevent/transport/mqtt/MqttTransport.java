package com.esri.geoevent.transport.mqtt;

import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.transport.Transport;

public interface MqttTransport extends Transport
{
  BundleLogger getLogger();

  default String translate(String message, Object... params)
  {
    return getLogger().translate(message, params);
  }

  default void trace(String message, Object... params)
  {
    getLogger().trace(message, params);
  }
}
