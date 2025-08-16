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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Deque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/// An instance of our server.
public final class Back extends Shared {

  private static final int BUFFER_SIZE = 192;
  private static final int POOL_SIZE = 3072;

  private static final int MAX_PERMITS = 10;
  private static final int MAX_TRXS = 8_500;

  /// Testing Adapter
  static class Adapter {

    SocketAddress proc0() throws IOException {
      return addr("payment-processor-default", 8080);
    }

    SocketAddress proc1() throws IOException {
      return addr("payment-processor-fallback", 8080);
    }

    private SocketAddress addr(String name, int port) throws IOException {
      final InetAddress[] all;
      all = InetAddress.getAllByName(name);

      for (InetAddress addr : all) {
        if (addr.isLoopbackAddress()) {
          continue;
        }

        if (addr instanceof Inet4Address) {
          return new InetSocketAddress(addr, port);
        }
      }

      throw new IOException("Failed to find IPv4 address of " + name);
    }

    ServerSocketChannel serverSocketChannel(Path path) throws IOException {
      final ServerSocketChannel channel;
      channel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);

      Files.deleteIfExists(path);

      final UnixDomainSocketAddress socketAddress;
      socketAddress = UnixDomainSocketAddress.of(path);

      channel.bind(socketAddress);

      return channel;
    }

    SocketChannel socketChannel() throws IOException {
      return SocketChannel.open();
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

  private final Deque<ByteBuffer> bufferPool = bufferPool(BUFFER_SIZE, POOL_SIZE);

  private final ServerSocketChannel channel;

  private final Lock lock = new ReentrantLock();

  private final SocketAddress proc0;

  @SuppressWarnings("unused")
  private final SocketAddress proc1;

  private int procAcquired;

  private final Condition procReady = lock.newCondition();

  private final Condition procReleased = lock.newCondition();

  private final ThreadFactory taskFactory;

  private final Trx[] trxs = new Trx[MAX_TRXS];

  private int trxsIndex;

  private int waiting;

  private Back(
      Adapter adapter,
      ServerSocketChannel channel,
      SocketAddress proc0,
      SocketAddress proc1,
      ThreadFactory taskFactory) {
    this.adapter = adapter;

    this.channel = channel;

    this.proc0 = proc0;

    this.proc1 = proc1;

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
    // Payment processors
    //

    final SocketAddress proc0, proc1;

    try {
      proc0 = adapter.proc0();

      proc1 = adapter.proc1();
    } catch (IOException e) {
      log("Failed to init payment processor addresses", e);

      return null;
    }

    //
    // Task Factory
    //

    final ThreadFactory taskFactory;
    taskFactory = Thread.ofVirtual().name("task-", 1).factory();

    //
    // Back
    //

    return new Back(adapter, channel, proc0, proc1, taskFactory);
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

    lock.lock();
    try {
      buffer = bufferPool.removeFirst();
    } finally {
      lock.unlock();
    }

    buffer.clear();

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
        task(this);
      } catch (IOException e) {
        throw new TaskException(buffer, e);
      }
    }

  }

  private void task(Task task) throws IOException {
    final ByteBuffer buffer;
    buffer = task.buffer;

    final SocketChannel front;
    front = task.front;

    try {
      task(buffer, front);
    } finally {
      lock.lock();
      try {
        bufferPool.addLast(buffer);
      } finally {
        lock.unlock();
      }
    }
  }

  private void task(ByteBuffer buffer, SocketChannel front) throws IOException {
    final int limit;

    try (front) {
      limit = taskRoute(buffer, front);
    }

    if (limit > 0) {
      // reset buffer
      buffer.position(1); // skip op

      buffer.limit(limit);

      // process payment
      taskPaymentProcess(buffer);
    }
  }

  // ##################################################################
  // # BEGIN: Task: Route
  // ##################################################################

  private int taskRoute(ByteBuffer buffer, SocketChannel front) throws IOException {
    // read the request
    final int frontRead;
    frontRead = front.read(buffer);

    if (frontRead <= 0) {
      // be optimistic:
      // let's assume the WHOLE request
      // will be read in a single read operation
      return -1;
    }

    buffer.flip();

    // helper return signal
    int signal;
    signal = 0;

    // task dispatch
    final byte op;
    op = buffer.get();

    switch (op) {
      case OP_PURGE -> taskPurge(buffer);

      case OP_PAYMENTS -> signal = taskPayment(buffer);

      case OP_SUMMARY -> taskSummary(buffer);

      default -> {
        buffer.clear();

        buffer.put(RESP_404);

        buffer.flip();
      }
    }

    // write response
    while (buffer.hasRemaining()) {
      front.write(buffer);
    }

    return signal;
  }

  // ##################################################################
  // # END: Task: Route
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Purge
  // ##################################################################

  private void taskPurge(ByteBuffer buffer) {
    trxsIndex = 0;

    buffer.clear();

    buffer.putInt(PURGE_200);

    buffer.flip();
  }

  // ##################################################################
  // # END: Task: Purge
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Payment
  // ##################################################################

  private int taskPayment(ByteBuffer buffer) {
    final int limit;
    limit = buffer.limit();

    final byte[] resp;
    resp = RESP_200;

    buffer.position(limit);

    buffer.limit(limit + resp.length);

    buffer.put(limit, resp);

    return limit;
  }

  // we'll use HTTP/1.0 so we can skip the Host header...
  private static final byte[] PAYMENT_REQ = asciiBytes("""
    POST /payments HTTP/1.0\r
    Content-Type: application/json\r
    Content-Length: \
    """);

  private static final long PAYMENT_RESP = asciiLong("HTTP/1.1");
  private static final long PAYMENT_RESP_200 = asciiLong(" 200 OK\r");
  private static final long PAYMENT_RESP_400 = asciiLong(" 400 Bad");
  private static final long PAYMENT_RESP_500 = asciiLong(" 500 Int");

  private void taskPaymentProcess(ByteBuffer buffer) throws IOException {
    final long time;
    time = buffer.getLong();

    final byte[] json;
    json = taskPaymentJson(buffer, time);

    final int amount;
    amount = buffer.getInt();

    while (true) {
      // always use the default processor
      final SocketAddress procAddres;
      procAddres = proc0;

      // before talking to the processor, obtain a permit
      // this is required to prevent ddosing the payment processor
      permitAcquire();

      try (SocketChannel processor = adapter.socketChannel()) {
        processor.connect(procAddres);

        // send request
        buffer.clear();

        buffer.put(PAYMENT_REQ);

        // write content-length value
        int value;
        value = json.length;

        int divisor;
        divisor = 1;

        while (value >= 10) {
          value /= 10;

          divisor *= 10;
        }

        value = json.length;

        while (divisor >= 1) {
          final int digit;
          digit = value / divisor;

          final int c;
          c = '0' + digit;

          buffer.put((byte) c);

          value %= divisor;

          divisor /= 10;
        }

        buffer.put(CRLFCRLF);

        buffer.put(json);

        buffer.flip();

        while (buffer.hasRemaining()) {
          processor.write(buffer);
        }

        // read response
        buffer.clear();

        // be optimistic:
        // let's assume the WHOLE request
        // will be read in a single read operation
        processor.read(buffer);
      } finally {
        // we've done talking to the processor, release the permit
        permitRelease();
      }

      // process the response
      buffer.flip();

      final long long0;
      long0 = buffer.getLong();

      if (long0 != PAYMENT_RESP) {
        throw new TaskException(buffer, "Unexpected payment processor response");
      }

      final long long1;
      long1 = buffer.getLong();

      if (long1 == PAYMENT_RESP_200) {
        paymentOk(time, amount);

        // our work is done
        return;
      }

      else if (long1 == PAYMENT_RESP_400 || long1 == PAYMENT_RESP_500) {
        paymentRetry();
      }

      else {
        throw new TaskException(buffer, "Unexpected payment processor response");
      }
    }
  }

  private static final int PAYMENT_REQ_TRAILER_LEN = "\"requestedAt\":\"2025-07-15T12:34:56.000Z\"}".length();
  private static final byte[] REQUESTED_AT = asciiBytes(",\"requestedAt\":\"");
  private static final byte[] CRLFCRLF = asciiBytes("\r\n\r\n");

  private byte[] taskPaymentJson(ByteBuffer buffer, long time) {
    final int reqJsonLen;
    reqJsonLen = buffer.remaining();

    final byte[] json;
    json = new byte[reqJsonLen + PAYMENT_REQ_TRAILER_LEN];

    // json: copy from original request
    int jsonIdx;
    jsonIdx = buffer.remaining();

    buffer.get(json, 0, jsonIdx);

    // json: remove trailing '}'
    jsonIdx -= 1;

    // json: parse amount
    final int amount;
    amount = parseAmount(json, jsonIdx);

    // store amount at the buffer
    buffer.clear();

    buffer.putInt(amount);

    buffer.flip();

    // json: requestedAt prop
    System.arraycopy(REQUESTED_AT, 0, json, jsonIdx, REQUESTED_AT.length);

    jsonIdx += REQUESTED_AT.length;

    // json: requestedAt value
    final Instant instant;
    instant = Instant.ofEpochMilli(time);

    final String iso;
    iso = instant.toString();

    for (int idx = 0, len = iso.length(); idx < len; idx++) {
      final char c;
      c = iso.charAt(idx);

      // iso date has only us-ascii characters
      json[jsonIdx++] = (byte) c;
    }

    // json: trailer
    json[jsonIdx++] = '\"';
    json[jsonIdx++] = '}';

    return json;
  }

  private int parseAmount(byte[] json, int jsonIdx) {
    int amount;
    amount = 0;

    int dot;
    dot = jsonIdx;

    int idx;
    idx = jsonIdx - 1;

    byte b;
    b = 0;

    int mul;
    mul = 1;

    while (idx >= 0) {
      b = json[idx--];

      if (b == ':') {
        break;
      }

      if (b == '.') {
        dot = idx + 1;

        continue;
      }

      if (b < '0' || b > '9') {
        logf("amount parse error: expected digit but got %x%n", b);

        return amount;
      }

      final int digit;
      digit = b - '0';

      final int val;
      val = digit * mul;

      amount += val;

      mul *= 10;
    }

    final int decimals;
    decimals = jsonIdx - dot - 1;

    switch (decimals) {
      case 0 -> amount *= 100;
      case 1 -> amount *= 10;
      case 2 -> {}
      default -> logf("amount parse error: more than 2 decimals %d%n", decimals);
    }

    return amount;
  }

  private void permitAcquire() {
    lock.lock();
    try {
      while (procAcquired == MAX_PERMITS) {
        procReleased.awaitUninterruptibly();
      }

      procAcquired += 1;
    } finally {
      lock.unlock();
    }
  }

  private void permitRelease() {
    lock.lock();
    try {
      procAcquired -= 1;

      procReleased.signal();
    } finally {
      lock.unlock();
    }
  }

  private void paymentOk(long time, int amount) {
    final Trx trx;
    trx = new Trx(time, 0, amount);

    lock.lock();
    try {
      final int idx;
      idx = trxsIndex++;

      trxs[idx] = trx;

      if (waiting > 0) {
        procReady.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  private void paymentRetry() {
    lock.lock();
    try {
      waiting += 1;

      procReady.awaitUninterruptibly();

      waiting -= 1;
    } finally {
      lock.unlock();
    }
  }

  // ##################################################################
  // # END: Task: Payment
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Summary
  // ##################################################################

  private void taskSummary(ByteBuffer buffer) {
    int req0 = 0, req1 = 0, amount0 = 0, amount1 = 0;

    final long time0;
    time0 = buffer.getLong();

    final long time1;
    time1 = buffer.getLong();

    lock.lock();
    try {
      for (int idx = 0, max = trxs.length; idx < max; idx++) {
        final Trx trx;
        trx = trxs[idx];

        if (trx == null) {
          continue;
        }

        if (trx.time < time0 || trx.time > time1) {
          continue;
        }

        if (trx.proc == 0) {
          req0 += 1;

          amount0 += trx.amount;
        } else {
          req1 += 1;

          amount1 += trx.amount;
        }
      }
    } finally {
      lock.unlock();
    }

    buffer.clear();

    buffer.putInt(req0);

    buffer.putInt(amount0);

    buffer.putInt(req1);

    buffer.putInt(amount1);

    buffer.flip();

  }

  // ##################################################################
  // # END: Task: Summary
  // ##################################################################

  // ##################################################################
  // # END: Task
  // ##################################################################

  // ##################################################################
  // # BEGIN: Trx
  // ##################################################################

  private record Trx(long time, int proc, int amount) {}

  // ##################################################################
  // # BEGIN: Trx
  // ##################################################################

  // ##################################################################
  // # BEGIN: Testing API
  // ##################################################################

  final String _debug() {
    return "Back[trxsIndex=%d]".formatted(trxsIndex);
  }

  final String _exec() {
    final Runnable task;
    task = serverListen();

    task.run();

    return _debug();
  }

  final String _payment() {
    final Runnable task;
    task = serverListen();

    final int before;
    before = trxsIndex;

    task.run();

    final Trx trx;
    trx = trxs[before];

    return trx.toString();
  }

  void _trx(long time, int proc, int amount) {
    trxs[trxsIndex++] = new Trx(time, proc, amount);
  }

  // ##################################################################
  // # END: Testing API
  // ##################################################################

}
