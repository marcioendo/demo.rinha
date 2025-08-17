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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/// Worker responsible for the actual payments.
public final class Pay extends Shared {

  private static final int BUFFER_SIZE = 192;

  private static final int MAX_PERMITS = 100;

  /// Testing Adapter
  static class Adapter {

    long currentTimeMillis() {
      return System.currentTimeMillis();
    }

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
        final InetSocketAddress maybe;
        maybe = new InetSocketAddress(addr, port);

        try (SocketChannel ch = socketChannel()) {
          ch.connect(maybe);

          logf("Connect ok=%s%n", maybe);

          return maybe;
        } catch (IOException e) {
          logf("Connect failed=%s%n", maybe);

          e.printStackTrace(System.out);

          continue;
        }
      }

      throw new IOException("Failed to find IP address of " + name);
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

  /// A payment processor
  private static final class Proc {

    static final VarHandle PERMIT;

    static {
      try {
        final MethodHandles.Lookup lookup;
        lookup = MethodHandles.lookup();

        PERMIT = lookup.findVarHandle(Proc.class, "permit", int.class);
      } catch (NoSuchFieldException e) {
        throw new AssertionError("Failed to create VarHandle", e);
      } catch (IllegalAccessException e) {
        throw new AssertionError("Failed to create VarHandle", e);
      }
    }

    final SocketAddress address;

    final int id;

    volatile int permit;

    volatile int permitMax = MAX_PERMITS;

    final Set<Thread> permitThreads = new HashSet<>();

    final Thread[] retryThreads = new Thread[MAX_TRXS / 2];

    int retryThreadsIndex;

    Proc(int id, SocketAddress address) {
      this.address = address;

      this.id = id;
    }

    final void acquire() {
      while (true) {
        final int value;
        value = permit;

        int newValue;
        newValue = value + 1;

        if (newValue <= permitMax) {
          if (PERMIT.compareAndSet(this, value, newValue)) {
            return;
          }
        } else {
          final Thread thread;
          thread = Thread.currentThread();

          synchronized (permitThreads) {
            permitThreads.add(thread);
          }

          LockSupport.park();

          synchronized (permitThreads) {
            permitThreads.remove(thread);
          }
        }
      }
    }

    final void release() {
      while (true) {
        final int value;
        value = permit;

        int newValue;
        newValue = value - 1;

        if (PERMIT.compareAndSet(this, value, newValue)) {
          releaseThreads();

          return;
        }
      }
    }

    private void releaseThreads() {
      if (permitThreads.isEmpty()) {
        return;
      }

      synchronized (permitThreads) {
        final Iterator<Thread> it;
        it = permitThreads.iterator();

        if (it.hasNext()) {
          final Thread t;
          t = it.next();

          LockSupport.unpark(t);
        }
      }
    }

    final void retry() {
      synchronized (retryThreads) {
        final int idx;
        idx = retryThreadsIndex++;

        retryThreads[idx] = Thread.currentThread();
      }

      LockSupport.park();
    }

    final void retrySignal() {
      int size;
      size = retryThreadsIndex;

      if (size > 0) {
        synchronized (retryThreads) {
          size = retryThreadsIndex;

          if (size > 0) {
            for (int idx = 0; idx < size; idx++) {
              final Thread t;
              t = retryThreads[idx];

              LockSupport.unpark(t);
            }

            retryThreadsIndex = 0;
          }
        }
      }
    }

  }

  private volatile boolean active = true;

  private final Adapter adapter;

  private final ByteBuffer[] buffers;

  private int buffersIndex;

  private final ServerSocketChannel channel;

  private final Proc proc0;

  private final Proc proc1;

  private final ThreadFactory taskFactory;

  private final Trx[] trxs = new Trx[MAX_TRXS];

  private int trxsIndex;

  private Pay(
      Adapter adapter,
      ByteBuffer[] buffers,
      ServerSocketChannel channel,
      Proc proc0,
      Proc proc1,
      ThreadFactory taskFactory) {
    this.adapter = adapter;

    this.buffers = buffers;

    this.channel = channel;

    this.proc0 = proc0;

    this.proc1 = proc1;

    this.taskFactory = taskFactory;
  }

  public static void main(String... args) {
    final Adapter adapter;
    adapter = new Adapter();

    final Pay back;
    back = Pay.boot(adapter, args);

    if (back != null) {
      back.server();
    }
  }

  // ##################################################################
  // # BEGIN: Boot
  // ##################################################################

  static Pay boot(Adapter adapter, String... args) {
    log("Pay");

    //
    // Buffers
    //

    final ByteBuffer[] buffers;
    buffers = new ByteBuffer[MAX_TRXS];

    final ByteBuffer direct;
    direct = ByteBuffer.allocateDirect(MAX_TRXS * BUFFER_SIZE);

    for (int idx = 0; idx < MAX_TRXS; idx++) {
      final int offset;
      offset = idx * BUFFER_SIZE;

      final ByteBuffer buffer;
      buffer = direct.slice(offset, BUFFER_SIZE);

      buffers[idx] = buffer;
    }

    final Consumer<AutoCloseable> shutdownHook;
    shutdownHook = shutdownHook();

    //
    // Socket path
    //

    final Path socket;
    socket = PAY_SOCKET;

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

    final Proc proc0, proc1;

    try {
      final SocketAddress addr0;
      addr0 = adapter.proc0();

      final SocketAddress addr1;
      addr1 = adapter.proc1();

      proc0 = new Proc(0, addr0);

      proc1 = new Proc(1, addr1);
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
    // Pay
    //

    return new Pay(adapter, buffers, channel, proc0, proc1, taskFactory);
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
    final SocketChannel remote;

    try {
      remote = channel.accept();
    } catch (IOException e) {
      log("Failed to accept client conn", e);

      return null;
    }

    final int idx;
    idx = buffersIndex++;

    final ByteBuffer buffer;
    buffer = buffers[idx];

    return new Task(buffer, remote);
  }

  // ##################################################################
  // # END: Server
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task
  // ##################################################################

  private class Task implements Runnable {

    final ByteBuffer buffer;

    final SocketChannel remote;

    Task(ByteBuffer buffer, SocketChannel remote) {
      this.buffer = buffer;

      this.remote = remote;
    }

    @Override
    public final void run() {
      try {
        task(buffer, remote);
      } catch (Exception e) {
        throw new TaskException(buffer, e);
      }
    }

  }

  private void task(ByteBuffer buffer, SocketChannel remote) throws Exception {
    final boolean payment;

    try (remote) {
      payment = taskRoute(buffer, remote);
    }

    if (payment) {
      buffer.position(1);

      pay(buffer);
    }
  }

  // ##################################################################
  // # BEGIN: Task: Route
  // ##################################################################

  private boolean taskRoute(ByteBuffer buffer, SocketChannel remote) throws IOException {
    final int read;
    read = remote.read(buffer);

    if (read <= 0) {
      return false;
    }

    buffer.flip();

    // task dispatch
    final byte op;
    op = buffer.get();

    switch (op) {
      case OP_PURGE -> taskPurge(buffer, remote);

      case OP_PAYMENTS -> { /*just read the request, there's no response to the Back service */ }

      case OP_SUMMARY -> taskSummary(buffer, remote);

      default -> taskUnknown(buffer, remote);
    }

    return op == OP_PAYMENTS;
  }

  // ##################################################################
  // # END: Task: Route
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Purge
  // ##################################################################

  private void taskPurge(ByteBuffer buffer, SocketChannel remote) throws IOException {
    trxsIndex = 0;

    buffer.clear();

    buffer.put(RESP_200);

    buffer.flip();

    remote.write(buffer);
  }

  // ##################################################################
  // # END: Task: Purge
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Summary
  // ##################################################################

  private void taskSummary(ByteBuffer buffer, SocketChannel remote) throws IOException {
    int req0 = 0, req1 = 0, amount0 = 0, amount1 = 0;

    final long time0;
    time0 = buffer.getLong();

    final long time1;
    time1 = buffer.getLong();

    proc0.permitMax = proc1.permitMax = 0;

    while (proc0.permit > 0 && proc1.permit > 0) {
      Thread.onSpinWait();
    }

    synchronized (trxs) {
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
    }

    proc0.permitMax = proc1.permitMax = MAX_PERMITS;

    buffer.clear();

    buffer.putInt(req0);

    buffer.putInt(amount0);

    buffer.putInt(req1);

    buffer.putInt(amount1);

    buffer.flip();

    remote.write(buffer);
  }

  // ##################################################################
  // # END: Task: Summary
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task: Unknown
  // ##################################################################

  private void taskUnknown(ByteBuffer buffer, SocketChannel remote) throws IOException {
    buffer.clear();

    buffer.put(RESP_404);

    buffer.flip();

    remote.write(buffer);
  }

  // ##################################################################
  // # END: Task: Unknown
  // ##################################################################

  // ##################################################################
  // # END: Task
  // ##################################################################

  // ##################################################################
  // # BEGIN: Pay
  // ##################################################################

  private record Trx(int amount, int proc, long time) {}

  // we'll use HTTP/1.0 so we can skip the Host header...
  private static final byte[] PAYMENT_REQ = asciiBytes("""
  POST /payments HTTP/1.0\r
  Content-Type: application/json\r
  Content-Length: \
  """);

  private static final byte[] CRLFCRLF = asciiBytes("\r\n\r\n");

  private static final long PAYMENT_RESP = asciiLong("HTTP/1.1");
  private static final long PAYMENT_RESP_200 = asciiLong(" 200 OK\r");
  private static final long PAYMENT_RESP_400 = asciiLong(" 400 Bad");
  private static final long PAYMENT_RESP_500 = asciiLong(" 500 Int");

  private void pay(ByteBuffer buffer) throws InterruptedException, IOException {
    final long time;
    time = adapter.currentTimeMillis();

    final byte[] json;
    json = taskPaymentJson(buffer, time);

    final int amount;
    amount = buffer.getInt();

    while (true) {
      //
      // assemble request
      //

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

      // always use the default processor
      final int currentProc;
      currentProc = 0;

      final Proc proc;
      proc = currentProc == 0 ? proc0 : proc1;

      // before talking to the processor, obtain a permit
      // required to:
      // 1) synchronize with summary
      // 2) prevent ddosing the payment processor
      proc.acquire();

      try (SocketChannel processor = adapter.socketChannel()) {
        processor.connect(proc.address);

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
        proc.release();
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
        proc.retrySignal();

        final Trx trx;
        trx = new Trx(amount, proc.id, time);

        synchronized (trxs) {
          final int idx;
          idx = trxsIndex++;

          trxs[idx] = trx;
        }

        // our work is done
        return;
      }

      else if (long1 == PAYMENT_RESP_400 || long1 == PAYMENT_RESP_500) {
        proc.retry();
      }

      else {
        throw new TaskException(buffer, "Unexpected payment processor response");
      }
    }
  }

  private static final int PAYMENT_REQ_TRAILER_LEN = "\"requestedAt\":\"2025-07-15T12:34:56.000Z\"}".length();
  private static final byte[] REQUESTED_AT = asciiBytes(",\"requestedAt\":\"");

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

  // ##################################################################
  // # END: Pay
  // ##################################################################

  final void dump() {
    System.out.println(_debug());
  }

  // ##################################################################
  // # BEGIN: Testing API
  // ##################################################################

  final String _debug() {
    return "Pay[trxsIndex=%d]".formatted(trxsIndex);
  }

  final String _exec() {
    final Runnable task;
    task = serverListen();

    task.run();

    return _debug();
  }

  final void _trx(long time, int proc, int amount) {
    final int id;
    id = trxsIndex++;

    final Trx trx;
    trx = new Trx(amount, proc, time);

    trxs[id] = trx;
  }

  final String _trxPayment(int idx) {
    while (trxs[idx] == null) {
      Thread.onSpinWait();
    }

    final Trx trx;
    trx = trxs[idx];

    return trx.toString();
  }

  // ##################################################################
  // # END: Testing API
  // ##################################################################

}
