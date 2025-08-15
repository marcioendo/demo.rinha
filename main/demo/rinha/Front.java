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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Deque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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

  private final Deque<ByteBuffer> bufferPool = bufferPool();

  private final ServerSocketChannel channel;

  private final Lock lock = new ReentrantLock();

  private final ThreadFactory taskFactory;

  private Front(
      Adapter adapter,
      SocketAddress back0,
      SocketAddress back1,
      ServerSocketChannel channel,
      ThreadFactory taskFactory) {
    this.adapter = adapter;
    this.back0 = back0;
    this.back1 = back1;
    this.channel = channel;
    this.taskFactory = taskFactory;
  }

  public static void main(String... args) {
    final Adapter adapter = new Adapter();

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
  // # BEGIN: Main
  // ##################################################################

  private void execute() {
    while (true) { // we don't need to be interruptible
      executeOne();
    }
  }

  final Thread executeOne() {
    final SocketChannel client;

    try {
      client = channel.accept();
    } catch (IOException e) {
      log("Failed to accept remote connection", e);

      return null;
    }

    final ByteBuffer buffer;

    lock.lock();
    try {
      buffer = bufferPool.removeFirst();
    } finally {
      lock.unlock();
    }

    buffer.clear();

    final Task task;
    task = new Task(buffer, client);

    final Thread thread;
    thread = taskFactory.newThread(task);

    thread.start();

    return thread;
  }

  private SocketAddress nextBackAddress() {
    final int round;
    round = (int) BACK_ROUND.getAndAdd(this, 1);

    final int backId;
    backId = round & 1;

    return backId == 0 ? back0 : back1;
  }

  // ##################################################################
  // # END: Main
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

    private static final long ROUTE_POST_PAYMENTS = asciiLong("POST /pa");

    private static final long ROUTE_POST_PURGE = asciiLong("POST /pu");

    private static final long ROUTE_GET_SUMMARY = asciiLong("GET /pay");

    @Override
    public final void run() {
      try (client) {
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

        final ByteBuffer resp;

        if (first == ROUTE_POST_PAYMENTS) {
          resp = payment();
        }

        else if (first == ROUTE_GET_SUMMARY) {
          resp = summary();
        }

        else if (first == ROUTE_POST_PURGE) {
          resp = purge();
        }

        else {
          resp = unknown();
        }

        // write response
        while (resp.hasRemaining()) {
          client.write(resp);
        }
      } catch (IOException e) {
        log("Client I/O error", e);
      } finally {
        lock.lock();
        try {
          bufferPool.addLast(buffer);
        } finally {
          lock.unlock();
        }
      }
    }

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

        int bytesRead;
        bytesRead = 0;

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
          bytesRead = back.read(slice);
        } catch (IOException e) {
          log("Backend I/O error", e);
        }

        // process the response (if any)
        if (bytesRead == 4) {
          slice.flip();

          final int trx;
          trx = slice.getInt();

          return trx == PURGE_200;
        } else {
          return Boolean.FALSE;
        }
      }
    }

    private ByteBuffer purge() {
      final PurgeTask purge0;
      purge0 = new PurgeTask(back0, buffer.slice(0, 4));

      final PurgeTask purge1;
      purge1 = new PurgeTask(back1, buffer.slice(4, 8));

      boolean success = false;

      try (var scope = new StructuredTaskScope.ShutdownOnFailure("purge", taskFactory)) {
        final Subtask<Boolean> task0;
        task0 = scope.fork(purge0);

        final Subtask<Boolean> task1;
        task1 = scope.fork(purge1);

        scope.join();

        scope.throwIfFailed();

        final boolean res0;
        res0 = task0.get();

        final boolean res1;
        res1 = task1.get();

        success = res0 && res1;
      } catch (ExecutionException e) {
        log("Failed to execute purge", e);
      } catch (InterruptedException e) {
        log("Interrupted while running purge", e);
      }

      return success ? ByteBuffer.wrap(RESP_200) : ByteBuffer.wrap(RESP_500);
    }

    private ByteBuffer payment() {
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

      bufferIndex -= 8; // time

      bufferIndex -= 1; // opcode

      buffer.position(bufferIndex);

      buffer.mark();

      buffer.put(OP_PAYMENTS);

      buffer.putLong(adapter.currentTimeMillis());

      buffer.reset();

      // choose backend
      final SocketAddress backAddress;
      backAddress = nextBackAddress();

      int bytesRead;
      bytesRead = 0;

      try (SocketChannel back = adapter.socketChannel()) {
        // send the request
        back.connect(backAddress);

        while (buffer.hasRemaining()) {
          back.write(buffer);
        }

        // read the response
        buffer.clear();

        // be optimistic:
        // let's assume the WHOLE request
        // will be read in a single read operation
        bytesRead = back.read(buffer);
      } catch (IOException e) {
        log("Backend I/O error", e);
      }

      // process the response (if any)
      if (bytesRead > 0) {
        // forward the response as it is
        return buffer.flip();
      } else {
        return resp500();
      }
    }

    private ByteBuffer summary() {
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

        time0 = summaryTime(off);

        // skip iso time
        off += 24;

        // skip '&to='
        off += 4;

        time1 = summaryTime(off);
      } else {
        time0 = 0L;

        time1 = Long.MAX_VALUE;
      }

      final ByteBuffer buffer0;
      buffer0 = buffer.clear();

      final ByteBuffer buffer1;
      buffer1 = ByteBuffer.allocate(BUFFER_SIZE);

      final SummaryTask summary0;
      summary0 = new SummaryTask(back0, buffer0, time0, time1);

      final SummaryTask summary1;
      summary1 = new SummaryTask(back1, buffer1, time0, time1);

      Summary result;
      result = Summary.ERROR;

      try (var scope = new StructuredTaskScope.ShutdownOnFailure("summary", taskFactory)) {
        final Subtask<Summary> task0;
        task0 = scope.fork(summary0);

        final Subtask<Summary> task1;
        task1 = scope.fork(summary1);

        scope.join();

        scope.throwIfFailed();

        final Summary res0;
        res0 = task0.get();

        final Summary res1;
        res1 = task1.get();

        result = res0.add(res1);
      } catch (ExecutionException e) {
        log("Failed to execute summary", e);
      } catch (InterruptedException e) {
        log("Interrupted while running summary", e);
      }

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

      return buffer.flip();
    }

    private long summaryTime(int off) {
      final byte[] bytes;
      bytes = new byte[24];

      buffer.get(off, bytes);

      final String s;
      s = new String(bytes, StandardCharsets.US_ASCII);

      final Instant instant;
      instant = Instant.parse(s);

      return instant.toEpochMilli();
    }

    private record Summary(int req0, int amount0, int req1, int amount1) {
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

    private class SummaryTask implements Callable<Summary> {

      private final SocketAddress backAddress;

      private final ByteBuffer buffer;

      private final long time0;

      private final long time1;

      SummaryTask(SocketAddress backAddress, ByteBuffer buffer, long time0, long time1) {
        this.backAddress = backAddress;

        this.buffer = buffer;

        this.time0 = time0;

        this.time1 = time1;
      }

      @Override
      public final Summary call() throws Exception {
        // assemble request
        buffer.put(OP_SUMMARY);

        buffer.putLong(time0);

        buffer.putLong(time1);

        buffer.flip();

        int bytesRead;
        bytesRead = 0;

        try (SocketChannel back = adapter.socketChannel()) {
          back.connect(backAddress);

          // send request
          while (buffer.hasRemaining()) {
            back.write(buffer);
          }

          buffer.clear();

          // read response
          bytesRead = back.read(buffer);
        }

        if (bytesRead > 0) {
          buffer.flip();

          return new Summary(
              buffer.getInt(),
              buffer.getInt(),
              buffer.getInt(),
              buffer.getInt()
          );
        } else {
          return Summary.ERROR;
        }
      }

    }

    private ByteBuffer unknown() {
      buffer.put(OP_UNKNOWN);

      buffer.flip();

      // choose backend
      final SocketAddress backAddress;
      backAddress = nextBackAddress();

      try (SocketChannel back = adapter.socketChannel()) {
        back.connect(backAddress);

        while (buffer.hasRemaining()) {
          back.write(buffer);
        }

        buffer.clear();

        back.read(buffer);
      } catch (IOException e) {
        log("Backend I/O error", e);
      }

      return buffer.flip();
    }

    private ByteBuffer resp500() {
      return ByteBuffer.wrap(RESP_500);
    }

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
    final Thread thread;
    thread = executeOne();

    if (thread != null) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    return _debug();
  }

  final SocketAddress _socketAddress() throws IOException {
    return channel.getLocalAddress();
  }

  // ##################################################################
  // # END: Testing API
  // ##################################################################

}
