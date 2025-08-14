/*
 * Copyright (C) 2025 Marcio Endo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package demo.rinha;

import static org.testng.Assert.assertEquals;
import java.nio.channels.SocketChannel;
import org.testng.annotations.Test;

public class BackTest3Health {

  /*
  
  HTTP/1.1 200 OK
  Connection: close
  Content-Type: application/json; charset=utf-8
  Date: Mon, 11 Aug 2025 12:33:07 GMT
  Server: Kestrel

  {"failing":false,"minResponseTime":0}
  
   */

  @Test
  public void testCase00() {
    assertEquals(
        """
        HTTP/1.1 200 OK\r
        Connection: close\r
        Content-Type: application/json; charset=utf-8\r
        Date: Mon, 1 Aug 2025 12:33:07 GMT\r
        Server: Kestrel\r
        \r
        """.length(),
        138
    );

    assertEquals(
        """
        {"failing":true,"minResponseTime":0}\
        """.length(),
        36
    );
  }

  @Test(description = "happy path")
  public void testCase01() {
    final SocketChannel proc0;
    proc0 = Y.socketChannel(opts -> {
      opts.connect(true);

      opts.readData("""
      HTTP/1.1 200 OK\r
      Connection: close\r
      Content-Type: application/json; charset=utf-8\r
      Date: Mon, 11 Aug 2025 12:33:07 GMT\r
      Server: Kestrel\r
      \r
      {"failing":true,"minResponseTime":0}\
      """);
    });

    final SocketChannel proc1;
    proc1 = Y.socketChannel(opts -> {
      opts.connect(true);

      opts.readData("""
      HTTP/1.1 200 OK\r
      Connection: close\r
      Content-Type: application/json; charset=utf-8\r
      Date: Mon, 11 Aug 2025 12:33:07 GMT\r
      Server: Kestrel\r
      \r
      {"failing":false,"minResponseTime":0}\
      """);
    });

    final Back.Adapter adapter;
    adapter = Y.backAdapter(opts -> {
      opts.socketChannelHealth(proc0);
      opts.socketChannelHealth(proc1);
    });

    final Back back;
    back = Y.back(adapter);

    back._health();

    assertEquals(
        Y.socketChannelWriteAscii(proc0),
        """
        GET /payments/service-health HTTP/1.0\r
        \r
        """
    );

    assertEquals(
        Y.socketChannelWriteAscii(proc1),
        """
        GET /payments/service-health HTTP/1.0\r
        \r
        """
    );

    assertEquals(proc0.isOpen(), false);
    assertEquals(proc1.isOpen(), false);
  }

  /*
  
  back0  | health0=Health[failing=false, minResponseTime=0],health1=Health[failing=false, minResponseTime=0]
  back0  | health0=Health[failing=false, minResponseTime=100],health1=Health[failing=false, minResponseTime=0]
  back0  | health0=Health[failing=true, minResponseTime=100],health1=Health[failing=false, minResponseTime=0]
  back0  | health0=Health[failing=true, minResponseTime=2000],health1=Health[failing=true, minResponseTime=1000]
  back0  | health0=Health[failing=false, minResponseTime=20],health1=Health[failing=false, minResponseTime=20]
  back0  | health0=Health[failing=false, minResponseTime=0],health1=Health[failing=false, minResponseTime=5000]
  
   */

  @Test(description = "preferred computation")
  public void testCase02() {
    final Back.Adapter adapter;
    adapter = Y.backAdapter(_ -> {});

    final Back back;
    back = Y.back(adapter);

    assertEquals(back._health(false, 0, false, 0), 0);
    assertEquals(back._health(false, 100, false, 0), 1);
    assertEquals(back._health(true, 100, false, 0), 1);
    assertEquals(back._health(true, 2000, true, 1000), 1);
    assertEquals(back._health(false, 20, false, 20), 0);
    assertEquals(back._health(false, 0, false, 5000), 0);
  }

}
