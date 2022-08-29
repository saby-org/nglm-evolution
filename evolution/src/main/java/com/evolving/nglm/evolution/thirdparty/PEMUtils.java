package com.evolving.nglm.evolution.thirdparty;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Base64.Decoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.xml.bind.DatatypeConverter;

import com.evolving.nglm.core.ServerRuntimeException;

public class PEMUtils
{
  
  /*********************************************
  *
  * enum
  * 
  **********************************************/
  
  public enum PemStringType
  {
    Certificate,
    RSAPrivateKey
  }
  
  /*********************************************
  *
  * data
  * 
  **********************************************/
  
  public static final String SSLCONTEXT_PROTOCOL_TLS  = "TLSv1.2";
  public static final String KEY_FACTORY_ALGO_RSA  = "RSA";
  public static final String CERTIFICATE_FACTORY_ALGO_X509  = "X.509";
  public static final String KEYSTORE_ALGO_RSA  = "PKCS12";
  
  public static final String BEGIN_CERTIFICATE  = "-----BEGIN CERTIFICATE-----";
  public static final String END_CERTIFICATE  = "-----END CERTIFICATE-----";
  public static final String BEGIN_PRIVATE_KEY  = "-----BEGIN PRIVATE KEY-----";
  public static final String END_PRIVATE_KEY  = "-----END PRIVATE KEY-----";
  
  /*********************************************
  *
  * getSSLContext
  * 
  **********************************************/
  
  public static SSLContext getSSLContext(String pemLocation, String tempKeyPass) throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, KeyManagementException
  {
    SSLContext sslContext = SSLContext.getInstance(SSLCONTEXT_PROTOCOL_TLS); //using standard "TLSv1.2"
    KeyStore keyStore = null;
    try
      {
        keyStore = getKeyStoreFromPEM(pemLocation, tempKeyPass);
      } 
    catch (Exception e)
      {
        throw new ServerRuntimeException("could not initialize SSLContext", e);
      }
 
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); //using default "SunX509"
    kmf.init(keyStore, tempKeyPass.toCharArray());
    
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); //using default "SunX509"
    tmf.init(keyStore);
    
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    return sslContext;
  }
  
  //
  // getKeyStoreFromPEM
  //
  
  public static KeyStore getKeyStoreFromPEM(String pemLocation, String tempKeyPass) throws Exception
  {
    byte[] certAndKey = Files.readAllBytes(new File(pemLocation).toPath());

    //byte[] certBytes = getBytesFromPEM(certAndKey, PemStringType.Certificate);
    //byte[] keyBytes = getBytesFromPEM(certAndKey, PemStringType.RSAPrivateKey);
    // X509Certificate cert = generateCertificate(certBytes);
    // RSAPrivateKey key = generatePrivateKey(keyBytes);

    //
    // check
    //

    String pem = new String(certAndKey, StandardCharsets.UTF_8);
    Pattern parse = Pattern.compile("(?m)(?s)^---*BEGIN ([^-]+)---*$([^-]+)^---*END[^-]+-+$");
    Matcher m = parse.matcher(pem);
    CertificateFactory certFactory = CertificateFactory.getInstance(CERTIFICATE_FACTORY_ALGO_X509);
    Decoder decoder = Base64.getMimeDecoder();
    List<X509Certificate> certList = new ArrayList<>();
    RSAPrivateKey privateKey = null;

    int start = 0;
    while (m.find(start))
      {
        String type = m.group(1);
        String base64Data = m.group(2);
        byte[] data = decoder.decode(base64Data);
        start += m.group(0).length();
        type = type.toUpperCase();
        if (type.contains("CERTIFICATE"))
          {
            X509Certificate cert = (X509Certificate) certFactory.generateCertificate(new ByteArrayInputStream(data));
            certList.add(cert);
          } 
        else if (type.contains("PRIVATE KEY"))
          {
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(data, KEY_FACTORY_ALGO_RSA);
            KeyFactory factory = KeyFactory.getInstance(KEY_FACTORY_ALGO_RSA); // using standard "RSA"
            privateKey = (RSAPrivateKey) factory.generatePrivate(spec);
            if (!(privateKey instanceof RSAPrivateKey)) 
              {
                throw new IllegalArgumentException("Key file does not contain an X509 encoded private key");
              }
          } 
        else
          {
            throw new ServerRuntimeException("Failed to create SSLContext - unsupported type "+ type);
          }
      }
    
    KeyStore keystore = KeyStore.getInstance("JKS");
    keystore.load(null, null);

    int count = 0;
    for (Certificate cert : certList) {
      keystore.setCertificateEntry("cert" + count, cert);
      count++;
    }
    Certificate[] chain = certList.toArray(new Certificate[certList.size()]);
    char[] keyStorePassword = new char[0];
    keystore.setKeyEntry("key", privateKey, keyStorePassword, chain);

    //
    // check
    //
    
    /*KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType()); // using default "jks" -- standard "PKCS12"
    keystore.load(null);
    keystore.setCertificateEntry("cert-alias", cert);
    keystore.setKeyEntry("key-alias", key, tempKeyPass.toCharArray(), new Certificate[] { cert });
     */
    return keystore;
  }
  
  public static byte[] getBytesFromPEM(byte[] pemByte, PemStringType type)
  {
    String beginDelimiter, endDelimiter; 
    
    switch (type)
    {
      case Certificate:
        beginDelimiter = BEGIN_CERTIFICATE;
        endDelimiter = END_CERTIFICATE;
        break;
      case RSAPrivateKey:
        beginDelimiter = BEGIN_PRIVATE_KEY;
        endDelimiter = END_PRIVATE_KEY;
        break;
      default:
        throw new ServerRuntimeException("Failed to create SSLContext - unsupported type "+ type);
    }
    
    String data = new String(pemByte);
    String[] tokens = data.split(beginDelimiter);
    tokens = tokens[1].split(endDelimiter);
    return DatatypeConverter.parseBase64Binary(tokens[0]);
  }
  
  public static X509Certificate generateCertificate(byte[] certBytes) throws CertificateException 
  {
    CertificateFactory factory = CertificateFactory.getInstance(CERTIFICATE_FACTORY_ALGO_X509); //using standard "X.509"
    return (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(certBytes));
  }
  
  protected static RSAPrivateKey generatePrivateKey(byte[] keyBytes) throws InvalidKeySpecException, NoSuchAlgorithmException 
  {
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes, KEY_FACTORY_ALGO_RSA);
    KeyFactory factory = KeyFactory.getInstance(KEY_FACTORY_ALGO_RSA); //using standard "RSA"
    PrivateKey privateKey = factory.generatePrivate(spec);
    
    if (!(privateKey instanceof RSAPrivateKey)) 
      {
        throw new IllegalArgumentException("Key file does not contain an X509 encoded private key");
      }
    
    return (RSAPrivateKey) privateKey;
  }
  
}
