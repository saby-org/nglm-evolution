/****************************************************************************
*
*  FakeIN.java
*
****************************************************************************/

package com.evolving.nglm.evolution.fakein;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.Random;

public class FakeIN
{
  /****************************************
  *
  *  main
  *
  ****************************************/
  
  public static void main(String[] args) throws IOException
  {
    /****************************************
    *
    *  arguments
    *
    ****************************************/

    String port = args[0];
    boolean deterministic = (args.length > 1) ? Objects.equals(args[1], "deterministic") : false;

    /****************************************
    *
    *  server loop
    *
    ****************************************/

    ServerSocket listener = new ServerSocket(Integer.parseInt(port));
    try
      {
        while (true)
          {
            Socket socket = listener.accept();
            try
              {
                //
                //  retrieve request
                //
                
                BufferedReader plec = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String str = plec.readLine();

                //
                //  determine (fake) status
                //
                
                String status = deterministic ? "SUCCESS" : getRandomDeliveryStatus();
                System.out.println("FakeIN : received " + str + " (result status " + status + ")");

                //
                //  send response
                //
                
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(status);
              }
            finally
              {
                socket.close();
              }
          }
      }
    finally
      {
        listener.close();
      }
  }

  /****************************************
  *
  *  getRandomDeliveryStatus
  *
  ****************************************/
  
  private static String getRandomDeliveryStatus()
  {
    Random random = new Random();
    String result = "UNKNOWN";
    switch (random.nextInt(2))
      {
        case 0:
          result = "SUCCESS";
          break;

        case 1:
          result = "THIRD_PARTY_ERROR";
          break;
      }
    return result;
  }
}
