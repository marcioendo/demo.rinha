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

public class BackTest1Payment {

  private final long fixedTime = Y.fixedTimeMilis();

  @Test
  public void testCase00() {
    assertEquals(
        """
        "requestedAt":"2025-08-10T11:22:33.444Z"}\
        """.length(),
        41
    );
  }

  @Test(description = "happy path")
  public void testCase01() {
    final SocketChannel front;
    front = Y.socketChannel(opts -> {
      opts.readData(
          Y.frontMsgPayment(fixedTime, 70, "{\"correlationId\":\"d1446168-6d53-4910-94f1-77d2acff17db\",\"amount\":19.9}")
      );
    });

    final SocketChannel processor;
    processor = Y.socketChannel(opts -> {
      opts.readData("""
      HTTP/1.1 200 OK\r
      \r
      """);
    });

    final ServerSocketChannel channel;
    channel = Y.serverSocketChannel(opts -> {
      opts.socketChannel(front);
    });

    final Back.Adapter adapter;
    adapter = Y.backAdapter(opts -> {
      opts.serverSocketChannel(channel);

      opts.socketChannel(processor);
    });

    final Back back;
    back = Y.back(adapter);

    assertEquals(back._payment(), "Trx[time=1754824953444, proc=0, amount=1990]");

    assertEquals(
        Y.socketChannelWriteAscii(processor),
        """
        POST /payments HTTP/1.0\r
        Content-Type: application/json\r
        Content-Length: 111\r
        \r
        {"correlationId":"d1446168-6d53-4910-94f1-77d2acff17db","amount":19.9,"requestedAt":"2025-08-10T11:22:33.444Z"}\
        """
    );

    assertEquals(
        Y.socketChannelWriteAscii(front),
        """
        HTTP/1.1 200 OK\r
        \r
        """
    );

    assertEquals(front.isOpen(), false);
    assertEquals(processor.isOpen(), false);
  }

}
