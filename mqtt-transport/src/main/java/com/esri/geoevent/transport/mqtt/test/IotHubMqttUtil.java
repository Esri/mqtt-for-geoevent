package com.esri.geoevent.transport.mqtt.test;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;

public class IotHubMqttUtil {

  public static MqttConnectOptions getMqttConnectOptions(String username, String password) throws Exception
  {
    MqttConnectOptions connOpts = new MqttConnectOptions();
    connOpts.setUserName(username);
    connOpts.setPassword(password.toCharArray());
    connOpts.setCleanSession(false);
    connOpts.setMqttVersion(4); // protocol=mqtt.MQTTv311
    connOpts.setKeepAliveInterval(30);
    connOpts.setConnectionTimeout(60);
    connOpts.setAutomaticReconnect(true);
    //    connOpts.setHttpsHostnameVerificationEnabled(false);

    SSLSocketFactory socketFactory = getSocketFactory();
    connOpts.setSocketFactory(socketFactory);

    //    java.util.Properties sslProperties = new java.util.Properties();
    // sslProperties.setProperty("com.ibm.ssl.protocol", "TLS");
    //    sslProperties.setProperty("jdk.tls.client.protocols", "TLSv1.2,TLSv1.3");
    //    sslProperties.setProperty("https.protocols", "TLSv1.2,TLSv1.3"); // TLSv1,TLSv1.1,TLSv1.2
    //    connOpts.setSSLProperties(sslProperties);

    return connOpts;
  }

  public static SSLSocketFactory getSocketFactory() throws Exception {
    Security.addProvider(new BouncyCastleProvider());

    // String caCertFile = "/Users/vlad2928/Projects/Esri/mqtt-for-geoevent/mqtt-transport/src/main/java/com/esri/geoevent/transport/mqtt/test/baltimore.cer";
    X509Certificate caCert = getX509Certificate();

    // CA certificate is used to authenticate server
    KeyStore caKs = KeyStore.getInstance(KeyStore.getDefaultType());
    caKs.load(null, null);
    caKs.setCertificateEntry("ca-certificate", caCert);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
    tmf.init(caKs);

    // Create a trust manager that does not validate certificate chains
    TrustManager[] trustAllCerts = new TrustManager[] {
        new X509ExtendedTrustManager() {
          @Override
          public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {}

          @Override
          public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {}

          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return null;
          }

          @Override
          public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {}

          @Override
          public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {}

          @Override
          public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {}

          @Override
          public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {}
        }
    };

    // finally, create SSL socket factory
    SSLContext context = SSLContext.getInstance("TLSv1.2");
    context.init(null, tmf.getTrustManagers(), null);
    return context.getSocketFactory();
  }

  public static X509Certificate getX509Certificate() throws Exception {
    X509Certificate caCert = null;
    String caCertFileName = "baltimore.cer";
    InputStream is = ClassLoader.getSystemResourceAsStream(caCertFileName);
    if (is != null)
    {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      BufferedInputStream bis = new BufferedInputStream(is);
      while (bis.available() > 0) {
        caCert = (X509Certificate) cf.generateCertificate(bis);
      }
    }
    return caCert;
  }

  public static String generateSasToken(String resourceUri, String key) throws Exception {
    // Token will expire in one hour
    long expiry = Instant.now().getEpochSecond() + 3600;

    String stringToSign = URLEncoder.encode(resourceUri, StandardCharsets.UTF_8) + "\n" + expiry;
    byte[] decodedKey = Base64.getDecoder().decode(key);

    Mac sha256HMAC = Mac.getInstance("HmacSHA256");
    SecretKeySpec secretKey = new SecretKeySpec(decodedKey, "HmacSHA256");
    sha256HMAC.init(secretKey);
    Base64.Encoder encoder = Base64.getEncoder();

    String signature = new String(
        encoder.encode(sha256HMAC.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8))),
        StandardCharsets.UTF_8
    );

    return "SharedAccessSignature sr=" +
        URLEncoder.encode(resourceUri, StandardCharsets.UTF_8) +
        "&sig=" + URLEncoder.encode(signature, StandardCharsets.UTF_8) +
        "&se=" + expiry;
  }
}
