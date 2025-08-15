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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

sealed abstract class Shared permits Back, Front {

  static final Path BACK0_SOCKET = Path.of("/tmp/kag8kie5uDie3lei0toh-back0.sock");
  static final Path BACK1_SOCKET = Path.of("/tmp/kag8kie5uDie3lei0toh-back1.sock");

  static final int BUFFER_POOL = 4096;
  static final int BUFFER_SIZE = 256;

  static final byte OP_PURGE = 1;
  static final byte OP_PAYMENTS = 2;
  static final byte OP_SUMMARY = 3;
  static final byte OP_UNKNOWN = 4;

  static final int PURGE_200 = 0xBABA;

  static final byte[] RESP_200 = asciiBytes("""
  HTTP/1.1 200 OK\r
  \r
  """);

  static final byte[] RESP_404 = asciiBytes("""
  HTTP/1.1 404 Not Found\r
  \r
  """);

  static final byte[] RESP_500 = asciiBytes("""
  HTTP/1.1 500 Internal Server Error\r
  \r
  """);

  private volatile boolean active = true;

  private final Deque<ByteBuffer> bufferPool;

  private final ServerSocketChannel channel;

  final Lock lock = new ReentrantLock();

  private final ThreadFactory taskFactory;

  Shared(ServerSocketChannel channel, ThreadFactory taskFactory) {
    this.bufferPool = bufferPool();

    this.channel = channel;

    this.taskFactory = taskFactory;
  }

  // ##################################################################
  // # BEGIN: Buffer Pool
  // ##################################################################

  private Deque<ByteBuffer> bufferPool() {
    final ByteBuffer buffer;
    buffer = ByteBuffer.allocateDirect(BUFFER_POOL * BUFFER_SIZE);

    final Deque<ByteBuffer> bufferPool;
    bufferPool = new ArrayDeque<>(BUFFER_POOL);

    for (int idx = 0; idx < BUFFER_POOL; idx++) {
      final int offset;
      offset = idx * BUFFER_SIZE;

      final ByteBuffer slice;
      slice = buffer.slice(offset, BUFFER_SIZE);

      bufferPool.addLast(slice);
    }

    return bufferPool;
  }

  final void bufferPool(ByteBuffer buffer) {
    lock.lock();
    try {
      bufferPool.addLast(buffer);
    } finally {
      lock.unlock();
    }
  }

  // ##################################################################
  // # END: Buffer Pool
  // ##################################################################

  // ##################################################################
  // # BEGIN: Server
  // ##################################################################

  final void execute() {
    try {
      while (active) {
        executeOne();
      }
    } catch (Throwable e) {
      throw new UnsupportedOperationException("Implement me", e);
    }
  }

  // returns Thread to ease testing
  final Thread executeOne() throws IOException {
    final ByteBuffer buffer;

    lock.lock();
    try {
      buffer = bufferPool.removeFirst();
    } finally {
      lock.unlock();
    }

    buffer.clear();

    final SocketChannel client;
    client = channel.accept();

    final Runnable task;
    task = task(buffer, client);

    final Thread thread;
    thread = taskFactory.newThread(task);

    thread.start();

    return thread;
  }

  // ##################################################################
  // # END: Server
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task support
  // ##################################################################

  record Summary(int req0, int amount0, int req1, int amount1) {
    static final Summary ERROR = new Summary(0, 0, 0, 0);

    final Summary add(Summary s) {
      return new Summary(
          req0 + s.req0,
          amount0 + s.amount0,
          req1 + s.req1,
          amount1 + s.amount1
      );
    }

    final String json() {
      return """
        {"default":{"totalRequests":%d,"totalAmount":%f},"fallback":{"totalRequests":%d,"totalAmount":%f}}"""
          .formatted(req0, amount0 / 100d, req1, amount1 / 100d);
    }
  }

  @SuppressWarnings("serial")
  static final class TaskException extends RuntimeException {

    @SuppressWarnings("unused")
    private ByteBuffer buffer;

    TaskException(String message, ByteBuffer buffer) {
      super(message);

      this.buffer = buffer;
    }

    TaskException(Throwable cause) {
      super(cause);
    }

  }

  final StructuredTaskScope.ShutdownOnFailure newShutdownOnFailure(String name) {
    return new StructuredTaskScope.ShutdownOnFailure(name, taskFactory);
  }

  abstract SocketChannel socketChannel() throws IOException;

  abstract Runnable task(ByteBuffer buffer, SocketChannel client);

  // ##################################################################
  // # END: Task support
  // ##################################################################

  static byte[] asciiBytes(String s) {
    return s.getBytes(StandardCharsets.US_ASCII);
  }

  static long asciiLong(String s) {
    final byte[] ascii;
    ascii = s.getBytes(StandardCharsets.US_ASCII);

    if (ascii.length != 8) {
      throw new IllegalArgumentException("String must contain exactly 8 us-ascii characters");
    }

    return 0
        | Byte.toUnsignedLong(ascii[0]) << 56
        | Byte.toUnsignedLong(ascii[1]) << 48
        | Byte.toUnsignedLong(ascii[2]) << 40
        | Byte.toUnsignedLong(ascii[3]) << 32
        | Byte.toUnsignedLong(ascii[4]) << 24
        | Byte.toUnsignedLong(ascii[5]) << 16
        | Byte.toUnsignedLong(ascii[6]) << 8
        | Byte.toUnsignedLong(ascii[7]) << 0;
  }

  // ##################################################################
  // # BEGIN: Log
  // ##################################################################

  static String debug(ByteBuffer buf) {
    buf.mark();

    final byte[] bytes;
    bytes = new byte[buf.remaining()];

    buf.get(bytes);

    buf.reset();

    return new String(bytes, StandardCharsets.US_ASCII);
  }

  static void debug(ServerSocketChannel channel) {
    try {
      if (channel != null) {
        final SocketAddress la;
        la = channel.getLocalAddress();

        final boolean open;
        open = channel.isOpen();

        System.out.printf("ServerSocketChannel[localAddress=%s,open=%s]%n", la, open);
      }
    } catch (IOException e) {
      log(e);
    }
  }

  static void log(String message) {
    System.out.println(message);
  }

  static void log(String message, ByteBuffer buf) {
    System.out.println(message);

    log(debug(buf));
  }

  static void log(String message, Throwable t) {
    System.out.println(message);

    t.printStackTrace(System.out);
  }

  static void log(Throwable t) {
    t.printStackTrace(System.out);
  }

  static void logf(String format, Object... args) {
    System.out.printf(format, args);
  }

  // ##################################################################
  // # END: Log
  // ##################################################################

  static InetAddress myIpv4Address() throws IOException {
    final Enumeration<NetworkInterface> ifaces;
    ifaces = NetworkInterface.getNetworkInterfaces();

    while (ifaces.hasMoreElements()) {
      final NetworkInterface iface;
      iface = ifaces.nextElement();

      if (!iface.isUp()) {
        continue;
      }

      if (iface.isLoopback()) {
        continue;
      }

      final Enumeration<InetAddress> addresses;
      addresses = iface.getInetAddresses();

      while (addresses.hasMoreElements()) {
        final InetAddress address;
        address = addresses.nextElement();

        if (address instanceof Inet4Address) {
          return address;
        }
      }
    }

    throw new IOException("Could not find an IPv4 address to bind to");
  }

  // ##################################################################
  // # BEGIN: ShutdownHook
  // ##################################################################

  private static final class ShutdownHook implements Consumer<AutoCloseable>, Runnable {

    private final List<AutoCloseable> services = new ArrayList<>();

    @Override
    public final void accept(AutoCloseable service) {
      if (service != null) {
        services.add(service);
      }
    }

    @Override
    public final void run() {
      for (AutoCloseable c : services) {
        if (c == null) {
          continue;
        }

        try {
          c.close();
        } catch (Throwable t) {
          // not much we can do here
        }
      }
    }

  }

  static Consumer<AutoCloseable> shutdownHook() {
    final ShutdownHook hook;
    hook = new ShutdownHook();

    final Runtime runtime;
    runtime = Runtime.getRuntime();

    final Thread thread;
    thread = Thread.ofPlatform().unstarted(hook);

    runtime.addShutdownHook(thread);

    return hook;
  }

  // ##################################################################
  // # END: ShutdownHook
  // ##################################################################

}
