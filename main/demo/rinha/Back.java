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
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/// The required servers behind the load balancer.
public final class Back extends Shared {

  private static final int BUFFER_SIZE = 192;

  /// Testing Adapter
  static class Adapter {

    ServerSocketChannel serverSocketChannel(Path path) throws IOException {
      final ServerSocketChannel channel;
      channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);

      Files.deleteIfExists(path);

      final UnixDomainSocketAddress socketAddress;
      socketAddress = UnixDomainSocketAddress.of(path);

      channel.bind(socketAddress);

      return channel;
    }

    SocketAddress socketAddress(Path socket) {
      final SocketAddress addr;
      addr = UnixDomainSocketAddress.of(socket);

      try {
        try (SocketChannel ch = socketChannel()) {
          ch.connect(addr);

          logf("Connect ok=%s%n", addr);
        }
      } catch (IOException e) {
        log("Failed to init back sockets", e);

        throw new UncheckedIOException(e);
      }

      return addr;
    }

    SocketChannel socketChannel() throws IOException {
      return SocketChannel.open(StandardProtocolFamily.UNIX);
    }

    void shutdownHook(Path path) {
      try {
        Files.deleteIfExists(path);
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }

  }

  private volatile boolean active = true;

  private final Adapter adapter;

  private final ServerSocketChannel channel;

  private final SocketAddress pay;

  private final ThreadFactory taskFactory;

  private Back(
      Adapter adapter,
      ServerSocketChannel channel,
      SocketAddress pay,
      ThreadFactory taskFactory) {
    this.adapter = adapter;

    this.channel = channel;

    this.pay = pay;

    this.taskFactory = taskFactory;
  }

  public static void main(String... args) {
    final Adapter adapter;
    adapter = new Adapter();

    final Back back;
    back = Back.boot(adapter, args);

    if (back != null) {
      back.server();
    }
  }

  // ##################################################################
  // # BEGIN: Boot
  // ##################################################################

  static Back boot(Adapter adapter, String... args) {
    log("Back");

    final Consumer<AutoCloseable> shutdownHook;
    shutdownHook = shutdownHook();

    //
    // Args
    //

    if (args.length != 1) {
      log("Syntax: back ID");

      return null;
    }

    int argsIndex;
    argsIndex = 0;

    // our ID
    final String id;
    id = args[argsIndex++];

    final Path socket;
    socket = switch (id) {
      case "0" -> BACK0_SOCKET;

      case "1" -> BACK1_SOCKET;

      default -> null;
    };

    if (socket == null) {
      log("Valid ID options are: 0 or 1");

      return null;
    }

    shutdownHook.accept(() -> adapter.shutdownHook(socket));

    //
    // ServerSocketChannel
    //

    final ServerSocketChannel channel;

    try {
      channel = adapter.serverSocketChannel(socket);
    } catch (IOException e) {
      log("Failed to init ServerSocketChannel", e);

      return null;
    }

    shutdownHook.accept(channel);

    //
    // Pay address
    //

    final SocketAddress pay;
    pay = adapter.socketAddress(PAY_SOCKET);

    //
    // Task Factory
    //

    final ThreadFactory taskFactory;
    taskFactory = Thread.ofVirtual().name("task-", 1).factory();

    //
    // Back
    //

    return new Back(adapter, channel, pay, taskFactory);
  }

  // ##################################################################
  // # END: Boot
  // ##################################################################

  // ##################################################################
  // # BEGIN: Server
  // ##################################################################

  private void server() {
    while (active) {
      final Runnable task;
      task = serverListen();

      final Thread thread;
      thread = taskFactory.newThread(task);

      thread.setUncaughtExceptionHandler(this::serverExceptionHandler);

      thread.start();
    }
  }

  private void serverExceptionHandler(Thread t, Throwable e) {
    e.printStackTrace(System.out);

    active = false;
  }

  private Runnable serverListen() {
    final SocketChannel front;

    try {
      front = channel.accept();
    } catch (IOException e) {
      log("Failed to accept client conn", e);

      return null;
    }

    final ByteBuffer buffer;
    buffer = ByteBuffer.allocate(BUFFER_SIZE);

    return new Task(buffer, front);
  }

  // ##################################################################
  // # END: Server
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task
  // ##################################################################

  private class Task implements Runnable {

    final ByteBuffer buffer;

    final SocketChannel front;

    Task(ByteBuffer buffer, SocketChannel front) {
      this.buffer = buffer;

      this.front = front;
    }

    @Override
    public final void run() {
      try {
        taskRoute(buffer, front);
      } catch (Exception e) {
        throw new TaskException(buffer, e);
      }
    }

  }

  // ##################################################################
  // # BEGIN: Task: Route
  // ##################################################################

  private void taskRoute(ByteBuffer buffer, SocketChannel front) throws Exception {
    final boolean payment;

    final int limit;

    try (front) {
      final int read;
      read = front.read(buffer);

      if (read <= 0) {
        return;
      }

      buffer.flip();

      limit = buffer.limit();

      final byte op;
      op = buffer.get();

      if (op == OP_PAYMENTS) {
        payment = true;

        final byte[] resp;
        resp = RESP_200;

        buffer.position(limit);

        buffer.limit(limit + resp.length);

        buffer.put(limit, resp);
      } else {
        payment = false;

        buffer.clear();

        buffer.put(RESP_404);

        buffer.flip();
      }

      front.write(buffer);
    }

    if (payment) {
      // forward request
      buffer.position(0);

      buffer.limit(limit);

      try (SocketChannel remote = adapter.socketChannel()) {
        remote.connect(pay);

        remote.write(buffer);
      }
    }
  }

  // ##################################################################
  // # END: Task
  // ##################################################################

  // ##################################################################
  // # BEGIN: Testing API
  // ##################################################################

  final String _debug() {
    return "Back[]";
  }

  final String _exec() {
    final Runnable task;
    task = serverListen();

    task.run();

    return _debug();
  }

  // ##################################################################
  // # END: Testing API
  // ##################################################################

}
