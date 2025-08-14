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

public class BackTest0Purge {

  @Test(description = "happy path")
  public void testCase01() {
    final SocketChannel front;
    front = Y.socketChannel(opts -> {
      opts.readData(
          Y.frontMsgPurge()
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

    assertEquals(back._exec(), "Back[trxsIndex=0]");

    assertEquals(
        Y.socketChannelWrite(front),
        Y.toByteArray(Shared.PURGE_200)
    );

    assertEquals(front.isOpen(), false);
  }

}
