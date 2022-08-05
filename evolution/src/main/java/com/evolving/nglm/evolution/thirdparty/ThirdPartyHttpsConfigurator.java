package com.evolving.nglm.evolution.thirdparty;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import com.evolving.nglm.core.ServerRuntimeException;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;

public class ThirdPartyHttpsConfigurator extends HttpsConfigurator
{

  public ThirdPartyHttpsConfigurator(SSLContext context)
  {
    super(context);
  }
  
  // not used in thirdpartmanager
  public void configure(HttpsParameters params) {
      try {
          // Initialise the SSL context
          SSLContext c = SSLContext.getDefault();
          SSLEngine engine = c.createSSLEngine();
          params.setNeedClientAuth(false);
          params.setCipherSuites(engine.getEnabledCipherSuites());
          params.setProtocols(engine.getEnabledProtocols());

          // Get the default parameters
          SSLParameters defaultSSLParameters = c.getDefaultSSLParameters();
          params.setSSLParameters(defaultSSLParameters);
      } 
      catch (Exception ex) {
        throw new ServerRuntimeException("Failed to create HttpsConfigurator", ex);
      }
  }


}
