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

public class BackTest2Summary {

  private final long time0 = Y.fixedTimeMilis();
  private final long time1 = time0 + TimeUnit.SECONDS.toMillis(10);

  @Test(description = "happy path")
  public void testCase01() {
    final SocketChannel front;
    front = Y.socketChannel(opts -> {
      opts.readData(
          Y.frontMsgSummary(time0, time1)
      );
    });

    final ServerSocketChannel channel;
    channel = Y.serverSocketChannel(opts -> {
      opts.socketChannel(front);
    });

    final Back.Adapter adapter;
    adapter = Y.backAdapter(opts -> {
      opts.serverSocketChannel(channel);
    });

    final Back back;
    back = Y.back(adapter);

    back._trx(time0, 0, 1); // req0=1, amount0=1
    back._trx(time0 + 200, 1, 10); // req1=1, amount1=10
    back._trx(time0 + 400, 0, 100); // req0=2, amount0=101
    back._trx(time1, 0, 1000); // req0=3, amount0=1101
    back._trx(time1, 1, 10000); // req1=2, amount1=10010
    back._trx(time1 + 1, 0, 100000);
    back._trx(time1 + 1, 1, 1000000);

    assertEquals(back._exec(), "Back[trxsIndex=7]");

    assertEquals(
        Y.socketChannelWrite(front),
        Y.backMsgSummary(3, 1101, 2, 10010)
    );

    assertEquals(front.isOpen(), false);
  }

}
