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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/// The Load Balancer.
public final class Front extends Shared {

  /// Testing Adapter
  static class Adapter {

    long currentTimeMillis() {
      return System.currentTimeMillis();
    }

    ServerSocketChannel serverSocketChannel() throws IOException {
      return ServerSocketChannel.open();
    }

    SocketChannel socketChannel() throws IOException {
      return SocketChannel.open(StandardProtocolFamily.UNIX);
    }

  }

  private static final VarHandle BACK_ROUND;

  static {
    try {
      final MethodHandles.Lookup lookup;
      lookup = MethodHandles.lookup();

      BACK_ROUND = lookup.findVarHandle(Front.class, "backRound", int.class);
    } catch (NoSuchFieldException e) {
      throw new AssertionError("Failed to create VarHandle", e);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Failed to create VarHandle", e);
    }
  }

  private static final int PORT = 9999;

  private final Adapter adapter;

  private final SocketAddress back0;

  private final SocketAddress back1;

  private volatile int backRound;

  private Front(
      Adapter adapter,
      SocketAddress back0,
      SocketAddress back1,
      ServerSocketChannel channel,
      ThreadFactory taskFactory) {
    super(channel, taskFactory);

    this.adapter = adapter;

    this.back0 = back0;

    this.back1 = back1;
  }

  public static void main(String... args) {
    final Adapter adapter;
    adapter = new Adapter();

    final Front front;
    front = boot(adapter, args);

    if (front != null) {
      front.execute();
    }
  }

  // ##################################################################
  // # BEGIN: Boot
  // ##################################################################

  static Front boot(Adapter adapter, String... args) {
    log("Front");

    final Consumer<AutoCloseable> shutdownHook;
    shutdownHook = shutdownHook();

    //
    // ServerSocketChannel
    //

    final ServerSocketChannel channel;

    try {
      channel = adapter.serverSocketChannel();

      final InetAddress address;
      address = myIpv4Address();

      final InetSocketAddress socketAddress;
      socketAddress = new InetSocketAddress(address, PORT);

      channel.bind(socketAddress);
    } catch (IOException e) {
      log("Failed to init ServerSocketChannel", e);

      return null;
    }

    shutdownHook.accept(channel);

    //
    // back sockets
    //

    final SocketAddress back0;
    back0 = UnixDomainSocketAddress.of(BACK0_SOCKET);

    final SocketAddress back1;
    back1 = UnixDomainSocketAddress.of(BACK1_SOCKET);

    //
    // task factory
    //

    final ThreadFactory taskFactory;
    taskFactory = Thread.ofVirtual().name("task-", 1).factory();

    return new Front(adapter, back0, back1, channel, taskFactory);
  }

  // ##################################################################
  // # END: Boot
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task
  // ##################################################################

  final SocketAddress back0() {
    return back0;
  }

  final SocketAddress back1() {
    return back1;
  }

  final long currentTimeMillis() {
    return adapter.currentTimeMillis();
  }

  final SocketAddress nextBackAddress() {
    final int round;
    round = (int) BACK_ROUND.getAndAdd(this, 1);

    final int backId;
    backId = round & 1;

    return backId == 0 ? back0 : back1;
  }

  @Override
  final SocketChannel socketChannel() throws IOException {
    return adapter.socketChannel();
  }

  @Override
  final Runnable task(ByteBuffer buffer, SocketChannel channel) {
    return new FrontTask(buffer, channel, this);
  }

  // ##################################################################
  // # END: Task
  // ##################################################################

  // ##################################################################
  // # BEGIN: Testing API
  // ##################################################################

  final String _debug() {
    return "Front[backRound=%d]".formatted(backRound);
  }

  final String _exec() {
    try {
      final Thread thread;
      thread = executeOne();

      if (thread != null) {
        thread.join();
      }

      return _debug();
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  // ##################################################################
  // # END: Testing API
  // ##################################################################

}
