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

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class FrontTest2Summary {

  private final long time0 = Y.fixedTimeMilis();
  private final long time1 = time0 + TimeUnit.SECONDS.toMillis(10);

  @Test

  public void testCase00() {
    assertEquals("2025-08-10T11:22:33.444Z".length(), 24);
  }

  @Test(description = "happy path")
  public void testCase01() {
    final SocketChannel client;
    client = Y.socketChannel(opts -> {
      opts.readData("""
      GET /payments-summary?from=2025-08-10T11:22:33.444Z&to=2025-08-10T11:22:43.444Z HTTP/1.1\r
      Host: localhost:9999\r
      User-Agent: Grafana k6/1.1.0\r
      Content-Type: application/json\r
      \r
      """);
    });

    final SocketChannel back0;
    back0 = Y.socketChannel(opts -> {
      opts.readData(Y.backMsgSummary(3, 4321, 6, 5432));
    });

    final SocketChannel back1;
    back1 = Y.socketChannel(opts -> {
      opts.readData(Y.backMsgSummary(2, 7654, 4, 9876));
    });

    final ServerSocketChannel channel;
    channel = Y.serverSocketChannel(opts -> {
      opts.socketChannel(client);
    });

    final Front.Adapter adapter;
    adapter = Y.frontAdapter(opts -> {
      opts.serverSocketChannel(channel);

      opts.socketChannel(back0);
      opts.socketChannel(back1);
    });

    final Front front;
    front = Y.front(adapter);

    assertEquals(front._exec(), "Front[backRound=0]");

    assertEquals(
        Y.socketChannelWrite(front._back0(back0, back1)),
        Y.frontMsgSummary(time0, time1)
    );

    assertEquals(
        Y.socketChannelWrite(front._back1(back0, back1)),
        Y.frontMsgSummary(time0, time1)
    );

    assertEquals(
        Y.socketChannelWriteAscii(client),
        """
        HTTP/1.1 200 OK\r
        Content-Type: application/json\r
        Content-Length: 113\r
        \r
        {"default":{"totalRequests":5,"totalAmount":119.750000},"fallback":{"totalRequests":10,"totalAmount":153.080000}}\
        """
    );

    assertEquals(back0.isOpen(), false);
    assertEquals(back1.isOpen(), false);
    assertEquals(client.isOpen(), false);
  }

  @Test(description = "no query params")
  public void testCase02() {
    final SocketChannel client;
    client = Y.socketChannel(opts -> {
      opts.readData("""
      GET /payments-summary HTTP/1.1\r
      Host: localhost:9999\r
      User-Agent: curl/8.5.0\r
      Accept: */*\r
      \r
      """);
    });

    final SocketChannel back0;
    back0 = Y.socketChannel(opts -> {
      opts.readData(Y.backMsgSummary(0, 0, 0, 0));
    });

    final SocketChannel back1;
    back1 = Y.socketChannel(opts -> {
      opts.readData(Y.backMsgSummary(0, 0, 0, 0));
    });

    final ServerSocketChannel channel;
    channel = Y.serverSocketChannel(opts -> {
      opts.socketChannel(client);
    });

    final Front.Adapter adapter;
    adapter = Y.frontAdapter(opts -> {
      opts.serverSocketChannel(channel);

      opts.socketChannel(back0);
      opts.socketChannel(back1);
    });

    final Front front;
    front = Y.front(adapter);

    assertEquals(front._exec(), "Front[backRound=0]");

    assertEquals(
        Y.socketChannelWrite(back0),
        Y.frontMsgSummary(0L, Long.MAX_VALUE)
    );

    assertEquals(
        Y.socketChannelWrite(back1),
        Y.frontMsgSummary(0L, Long.MAX_VALUE)
    );

    assertEquals(
        Y.socketChannelWriteAscii(client),
        """
        HTTP/1.1 200 OK\r
        Content-Type: application/json\r
        Content-Length: 108\r
        \r
        {"default":{"totalRequests":0,"totalAmount":0.000000},"fallback":{"totalRequests":0,"totalAmount":0.000000}}\
        """
    );

    assertEquals(back0.isOpen(), false);
    assertEquals(back1.isOpen(), false);
    assertEquals(client.isOpen(), false);
  }

}
