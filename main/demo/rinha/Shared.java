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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Consumer;

sealed abstract class Shared permits Back, Front {

  static final Path BACK0_SOCKET = Path.of("/tmp/kag8kie5uDie3lei0toh-back0.sock");
  static final Path BACK1_SOCKET = Path.of("/tmp/kag8kie5uDie3lei0toh-back1.sock");

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

  Shared() {}

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
  // # BEGIN: Buffer Pool
  // ##################################################################

  final Deque<ByteBuffer> bufferPool(int bufferSize, int poolSize) {
    final ByteBuffer buffer;
    buffer = ByteBuffer.allocateDirect(bufferSize * poolSize);

    final Deque<ByteBuffer> bufferPool;
    bufferPool = new ArrayDeque<>(poolSize);

    for (int idx = 0; idx < poolSize; idx++) {
      final int offset;
      offset = idx * bufferSize;

      final ByteBuffer slice;
      slice = buffer.slice(offset, bufferSize);

      bufferPool.addLast(slice);
    }

    return bufferPool;
  }

  // ##################################################################
  // # END: Buffer Pool
  // ##################################################################

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

  static String dump(ServerSocketChannel channel) {
    try {
      if (channel != null) {
        final SocketAddress la;
        la = channel.getLocalAddress();

        final boolean open;
        open = channel.isOpen();

        return "ServerSocketChannel[localAddress=%s,open=%s]".formatted(la, open);
      } else {
        return "null";
      }
    } catch (IOException e) {
      return e.getMessage();
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

  static void logf(String format, Object... args) {
    System.out.printf(format, args);
  }

  // ##################################################################
  // # END: Log
  // ##################################################################

  // ##################################################################
  // # BEGIN: ServerSocket
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
  // # END: ServerSocket
  // ##################################################################

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
