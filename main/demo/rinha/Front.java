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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Deque;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/// The Load Balancer.
public final class Front extends Shared {

  private static final int BUFFER_SIZE = 224;
  private static final int POOL_SIZE = 1024;

  /// Testing Adapter
  static class Adapter {

    ServerSocketChannel serverSocketChannel() throws IOException {
      return ServerSocketChannel.open();
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

  private volatile boolean active = true;

  private final Adapter adapter;

  private final SocketAddress back0;

  private final SocketAddress back1;

  private volatile int backRound;

  private final Deque<ByteBuffer> bufferPool = bufferPool(BUFFER_SIZE, POOL_SIZE);

  private final ServerSocketChannel channel;

  private final SocketAddress pay;

  private final ThreadFactory taskFactory;

  private Front(
      Adapter adapter,
      SocketAddress back0,
      SocketAddress back1,
      ServerSocketChannel channel,
      SocketAddress pay,
      ThreadFactory taskFactory) {
    this.adapter = adapter;
    this.back0 = back0;
    this.back1 = back1;
    this.channel = channel;
    this.pay = pay;
    this.taskFactory = taskFactory;
  }

  public static void main(String... args) {
    final Adapter adapter;
    adapter = new Adapter();

    final Front front;
    front = boot(adapter, args);

    if (front != null) {
      front.server();
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
    // socket addresses
    //

    final SocketAddress back0;
    back0 = adapter.socketAddress(BACK0_SOCKET);

    final SocketAddress back1;
    back1 = adapter.socketAddress(BACK1_SOCKET);

    final SocketAddress pay;
    pay = adapter.socketAddress(PAY_SOCKET);

    //
    // task factory
    //

    final ThreadFactory taskFactory;
    taskFactory = Thread.ofVirtual().name("task-", 1).factory();

    return new Front(adapter, back0, back1, channel, pay, taskFactory);
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

  final Runnable serverListen() {
    final SocketChannel client;

    try {
      client = channel.accept();
    } catch (IOException e) {
      log("Failed to accept remote connection", e);

      return null;
    }

    final ByteBuffer buffer;

    synchronized (bufferPool) {
      buffer = bufferPool.removeFirst();
    }

    buffer.clear();

    return new Task(buffer, client);
  }

  // ##################################################################
  // # END: Server
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task
  // ##################################################################

  private class Task implements Runnable {

    final ByteBuffer buffer;

    final SocketChannel client;

    Task(ByteBuffer buffer, SocketChannel client) {
      this.buffer = buffer;

      this.client = client;
    }

    @Override
    public final void run() {
      try (client) {
        taskRoute(buffer, client);
      } catch (Throwable e) {
        throw new TaskException(buffer, e);
      } finally {
        synchronized (bufferPool) {
          bufferPool.addLast(buffer);
        }
      }
    }

  }

  private SocketAddress taskBackAddress() {
    final int round;
    round = (int) BACK_ROUND.getAndAdd(this, 1);

    final int backId;
    backId = round & 1;

    return backId == 0 ? back0 : back1;
  }

  // ##################################################################
  // # BEGIN: Task: Route
  // ##################################################################

  private static final long ROUTE_POST_PAYMENTS = asciiLong("POST /pa");

  private static final long ROUTE_POST_PURGE = asciiLong("POST /pu");

  private static final long ROUTE_GET_SUMMARY = asciiLong("GET /pay");

  private void taskRoute(ByteBuffer buffer, SocketChannel client) throws Exception {
    // read the request
    final int clientRead;
    clientRead = client.read(buffer);

    if (clientRead <= 0) {
      // be optimistic:
      // let's assume the WHOLE request
      // will be read in a single read operation
      return;
    }

    buffer.flip();

    // route
    final long first;
    first = buffer.getLong();

    if (first == ROUTE_POST_PAYMENTS) {
      taskPayment(buffer);
    }

    else if (first == ROUTE_GET_SUMMARY) {
      taskSummary(buffer);
    }

    else if (first == ROUTE_POST_PURGE) {
      taskPurge(buffer);
    }

    else {
      taskUnknown(buffer);
    }

    // write response
    while (buffer.hasRemaining()) {
      client.write(buffer);
    }
  }

  // ##################################################################
  // # END: Task: Route
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Purge
  // ##################################################################

  class PurgeTask implements Callable<Boolean> {
    private final SocketAddress backAddress;

    private final ByteBuffer slice;

    PurgeTask(SocketAddress backAddress, ByteBuffer slice) {
      this.backAddress = backAddress;

      this.slice = slice;
    }

    @Override
    public final Boolean call() throws Exception {
      // assemble the request
      slice.put(OP_PURGE);

      slice.flip();

      try (SocketChannel back = adapter.socketChannel()) {
        // send the request
        back.connect(backAddress);

        while (slice.hasRemaining()) {
          back.write(slice);
        }

        // read the response
        slice.clear();

        // be optimistic:
        // let's assume the WHOLE request
        // will be read in a single read operation
        back.read(slice);
      }

      // process the response
      slice.flip();

      final int trx;
      trx = slice.getInt();

      return trx == TRX_200;
    }
  }

  private void taskPurge(ByteBuffer buffer) throws IOException {
    // assemble the request
    buffer.clear();

    buffer.put(OP_PURGE);

    buffer.flip();

    try (SocketChannel remote = adapter.socketChannel()) {
      // send the request
      remote.connect(pay);

      remote.write(buffer);

      // read the response
      buffer.clear();

      remote.read(buffer);
    }

    // send response as it is
    buffer.flip();
  }

  // ##################################################################
  // # END: Task: Purge
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Payment
  // ##################################################################

  private void taskPayment(ByteBuffer buffer) throws IOException {
    // we're assuming the request is exactly:
    //
    // POST /payments HTTP/1.1\r\n
    // Host: localhost:9999\r\n
    // User-Agent: Grafana k6/1.1.0\r\n
    // Content-Length: 70\r\n
    // Content-Type: application/json\r\n
    // \r\n
    //
    // which means request body starts at 131
    // we take out the 8 bytes we read at the route parsing
    int bufferIndex;
    bufferIndex = 131;

    bufferIndex -= 1; // opcode

    buffer.position(bufferIndex);

    buffer.mark();

    buffer.put(OP_PAYMENTS);

    buffer.reset();

    // choose backend
    final SocketAddress backAddress;
    backAddress = taskBackAddress();

    try (SocketChannel back = adapter.socketChannel()) {
      back.connect(backAddress);

      back.write(buffer);

      buffer.clear();

      back.read(buffer);
    }

    // forward the response as it is
    buffer.flip();
  }

  // ##################################################################
  // # END: Task: Payment
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Summary
  // ##################################################################

  private record Summary(int req0, int amount0, int req1, int amount1) {
    final String json() {
      return """
        {"default":{"totalRequests":%d,"totalAmount":%.2f},"fallback":{"totalRequests":%d,"totalAmount":%.2f}}"""
          .formatted(req0, amount0 / 100d, req1, amount1 / 100d);
    }
  }

  private void taskSummary(ByteBuffer buffer) throws IOException {
    // find '='
    int off;
    off = 0;

    for (int idx = buffer.position(), max = buffer.limit(); idx < max; idx++) {
      final byte b;
      b = buffer.get(idx);

      if (b == '=') {
        off = idx;

        break;
      }
    }

    // from time
    final long time0;

    // to time
    final long time1;

    if (off != 0) {
      // skip '='
      off += 1;

      time0 = summaryTime(buffer, off);

      // skip iso time
      off += 24;

      // skip '&to='
      off += 4;

      time1 = summaryTime(buffer, off);
    } else {
      time0 = 0L;

      time1 = Long.MAX_VALUE;
    }

    // assemble request
    buffer.clear();

    buffer.put(OP_SUMMARY);

    buffer.putLong(time0);

    buffer.putLong(time1);

    buffer.flip();

    try (SocketChannel remote = adapter.socketChannel()) {
      remote.connect(pay);

      remote.write(buffer);

      buffer.clear();

      remote.read(buffer);
    }

    buffer.flip();

    final Summary result;
    result = new Summary(
        buffer.getInt(),
        buffer.getInt(),
        buffer.getInt(),
        buffer.getInt()
    );

    final String json;
    json = result.json();

    final String resp = """
      HTTP/1.1 200 OK\r
      Content-Type: application/json\r
      Content-Length: %d\r
      \r
      %s""".formatted(json.length(), json);

    final byte[] bytes;
    bytes = resp.getBytes(StandardCharsets.US_ASCII);

    buffer.clear();

    buffer.put(bytes);

    buffer.flip();
  }

  private long summaryTime(ByteBuffer buffer, int off) {
    final byte[] bytes;
    bytes = new byte[24];

    buffer.get(off, bytes);

    final String s;
    s = new String(bytes, StandardCharsets.US_ASCII);

    final Instant instant;
    instant = Instant.parse(s);

    return instant.toEpochMilli();
  }

  // ##################################################################
  // # END: Task: Summary
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Unknown
  // ##################################################################

  private void taskUnknown(ByteBuffer buffer) throws IOException {
    buffer.clear();

    buffer.put(OP_UNKNOWN);

    buffer.flip();

    // choose backend
    final SocketAddress backAddress;
    backAddress = taskBackAddress();

    try (SocketChannel back = adapter.socketChannel()) {
      back.connect(backAddress);

      while (buffer.hasRemaining()) {
        back.write(buffer);
      }

      buffer.clear();

      back.read(buffer);
    }

    buffer.flip();
  }

  // ##################################################################
  // # END: Task: Unknown
  // ##################################################################

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
    final Runnable task;
    task = serverListen();

    task.run();

    return _debug();
  }

  final SocketAddress _socketAddress() throws IOException {
    return channel.getLocalAddress();
  }

  // ##################################################################
  // # END: Testing API
  // ##################################################################

}
