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
import org.testng.annotations.Test;

public class FrontTest0Purge {

  @Test(description = "happy path")
  public void testCase01() {
    final SocketChannel client;
    client = Y.socketChannel(opts -> {
      opts.readData("""
      POST /purge-payments HTTP/1.1\r
      Host: localhost:9999\r
      User-Agent: Grafana k6/1.1.0\r
      Content-Length: 0\r
      Content-Type: application/json\r
      \r
      """);
    });

    final SocketChannel back0;
    back0 = Y.socketChannel(opts -> {
      opts.connect(true);

      opts.readData(Shared.PURGE_200);
    });

    final SocketChannel back1;
    back1 = Y.socketChannel(opts -> {
      opts.connect(true);

      opts.readData(Shared.PURGE_200);
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

    assertEquals(Y.socketChannelWrite(back0), Y.frontMsgPurge());
    assertEquals(Y.socketChannelWrite(back1), Y.frontMsgPurge());
    assertEquals(
        Y.socketChannelWriteAscii(client),
        """
        HTTP/1.1 200 OK\r
        \r
        """
    );

    assertEquals(back0.isOpen(), false);
    assertEquals(back1.isOpen(), false);
    assertEquals(client.isOpen(), false);
  }

}
