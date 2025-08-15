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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/// An instance of our server.
public final class Back extends Shared {

  private static final int MAX_TRXS = 9_000;

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

    SocketChannel socketChannelHealth() throws IOException {
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

  private final Adapter adapter;

  private final ServerSocketChannel channel;

  private final Lock lock = new ReentrantLock();

  private final int maxPermits;

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
      int maxPermits,
      SocketAddress proc0,
      SocketAddress proc1,
      ThreadFactory taskFactory) {
    this.adapter = adapter;

    this.channel = channel;

    this.maxPermits = maxPermits;

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
      back.execute();
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

    if (args.length != 2) {
      log("Syntax: back ID MAX_PERMITS");

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

    // MAX_PERMITS
    final int maxPermits;

    try {
      final String arg;
      arg = args[argsIndex++];

      maxPermits = Integer.parseInt(arg);
    } catch (NumberFormatException e) {
      log("MAX_PERMITS must an integer value");

      return null;
    }

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

    logf("proc0=%s%n", proc0);

    logf("proc1=%s%n", proc1);

    //
    // Task Factory
    //

    final ThreadFactory taskFactory;
    taskFactory = Thread.ofVirtual().name("task-", 1).factory();

    //
    // Back
    //

    return new Back(adapter, channel, maxPermits, proc0, proc1, taskFactory);
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

  private Thread executeOne() {
    final SocketChannel front;

    try {
      front = channel.accept();
    } catch (IOException e) {
      log("Failed to accept client conn", e);

      return null;
    }

    final ByteBuffer buffer;
    buffer = ByteBuffer.allocate(BUFFER_SIZE);

    final Task task;
    task = new Task(buffer, front);

    final Thread thread;
    thread = taskFactory.newThread(task);

    thread.start();

    return thread;
  }

  // ##################################################################
  // # END: Main
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
      try (front) {
        // read the request
        final int frontRead;
        frontRead = front.read(buffer);

        if (frontRead <= 0) {
          // be optimistic:
          // let's assume the WHOLE request
          // will be read in a single read operation
          return;
        }

        buffer.flip();

        // task dispatch
        final byte op;
        op = buffer.get();

        final ByteBuffer resp;
        resp = switch (op) {
          case OP_PURGE -> purge();

          case OP_PAYMENTS -> payment();

          case OP_SUMMARY -> summary();

          case OP_UNKNOWN -> unknown();

          default -> unknown();
        };

        // write response
        while (resp.hasRemaining()) {
          front.write(resp);
        }
      } catch (IOException e) {
        log("Front I/O error", e);
      }
    }

    private ByteBuffer purge() {
      trxsIndex = 0;

      buffer.clear();

      buffer.putInt(PURGE_200);

      return buffer.flip();
    }

    private ByteBuffer payment() {
      // handle actual payment in a new thread
      final PaymentTask task;
      task = new PaymentTask(buffer);

      final Thread thread;
      thread = taskFactory.newThread(task);

      thread.start();

      // send response straight away
      return ByteBuffer.wrap(RESP_200);
    }

    private ByteBuffer summary() {
      int req0 = 0, req1 = 0, amount0 = 0, amount1 = 0;

      final long time0;
      time0 = buffer.getLong();

      final long time1;
      time1 = buffer.getLong();

      lock.lock();
      try {
        final int max;
        max = trxsIndex;

        for (int idx = 0; idx < max; idx++) {
          final Trx trx;
          trx = trxs[idx];

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

      return buffer.flip();
    }

    private ByteBuffer unknown() {
      return ByteBuffer.wrap(RESP_404);
    }

  }

  // ##################################################################
  // # END: Task
  // ##################################################################

  // ##################################################################
  // # BEGIN: Payment
  // ##################################################################

  // we'll use HTTP/1.0 so we can skip the Host header...
  private static final byte[] PAYMENT_REQ = asciiBytes("""
    POST /payments HTTP/1.0\r
    Content-Type: application/json\r
    Content-Length: \
    """);

  private static final int PAYMENT_REQ_TRAILER_LEN = "\"requestedAt\":\"2025-07-15T12:34:56.000Z\"}".length();
  private static final byte[] REQUESTED_AT = asciiBytes(",\"requestedAt\":\"");
  private static final byte[] CRLFCRLF = asciiBytes("\r\n\r\n");

  private static final long PAYMENT_RESP = asciiLong("HTTP/1.1");
  private static final long PAYMENT_RESP_200 = asciiLong(" 200 OK\r");
  private static final long PAYMENT_RESP_400 = asciiLong(" 400 Bad");
  private static final long PAYMENT_RESP_500 = asciiLong(" 500 Int");

  private class PaymentTask implements Runnable {

    private int amount;

    private final ByteBuffer buffer;

    private int rubicon;

    private long time;

    PaymentTask(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public final void run() {
      prepare();

      while (true) {
        final int bytesRead;
        bytesRead = exchange();

        // process the response (if any)
        if (bytesRead > 0) {
          buffer.position(rubicon);

          buffer.limit(rubicon + bytesRead);

          final long long0;
          long0 = buffer.getLong();

          if (long0 != PAYMENT_RESP) {
            log("Unexpected payment processor response");

            // fail...
            break;
          }

          final long long1;
          long1 = buffer.getLong();

          if (long1 == PAYMENT_RESP_200) {
            paymentOk();

            // our work is done
            return;
          }

          else if (long1 == PAYMENT_RESP_400 || long1 == PAYMENT_RESP_500) {
            paymentRetry();
          }

          else {
            buffer.rewind();

            log("Unexpected payment response", buffer);

            throw new UnsupportedOperationException("Implement me :: unexpected payment response");
          }
        } else {
          log("No data, cancelling task");

          return;
        }
      }

      log("No result!");
    }

    private void prepare() {
      //
      // trx time
      //

      time = buffer.getLong();

      //
      // json
      //

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

      // parse amount (is this really required??? or can we assume 19.9???)
      amount = parseAmount(json, jsonIdx);

      // json: requestedAt prop
      System.arraycopy(REQUESTED_AT, 0, json, jsonIdx, REQUESTED_AT.length);

      jsonIdx += REQUESTED_AT.length;

      final Instant instant;
      instant = Instant.ofEpochMilli(time);

      final byte[] instantBytes;
      instantBytes = asciiBytes(instant.toString());

      System.arraycopy(instantBytes, 0, json, jsonIdx, instantBytes.length);

      jsonIdx += instantBytes.length;

      // json: trailer
      json[jsonIdx++] = '\"';
      json[jsonIdx++] = '}';

      //
      // assemble payment processor request
      //

      buffer.clear();

      buffer.put(PAYMENT_REQ);

      // content-length
      final int contentLength;
      contentLength = json.length;

      final String contentLengthValue;
      contentLengthValue = Integer.toString(contentLength);

      final byte[] contentLengthBytes;
      contentLengthBytes = asciiBytes(contentLengthValue);

      buffer.put(contentLengthBytes);

      buffer.put(CRLFCRLF);

      buffer.put(json);

      // we'll keep the request saved for reuse if necessary
      // response will begin at the rubicon
      rubicon = buffer.position();
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

    private int exchange() {
      // before talking to the processor, obtain a permit
      // this is required to prevent ddosing the payment processor
      permitAcquire();

      // always use the default processor
      final SocketAddress procAddres;
      procAddres = proc0;

      int bytesRead;
      bytesRead = 0;

      try (SocketChannel processor = adapter.socketChannel()) {
        processor.connect(procAddres);

        // send request
        buffer.position(0);

        buffer.limit(rubicon);

        while (buffer.hasRemaining()) {
          processor.write(buffer);
        }

        // read response
        buffer.position(rubicon);

        buffer.limit(buffer.capacity());

        // be optimistic:
        // let's assume the WHOLE request
        // will be read in a single read operation
        bytesRead = processor.read(buffer);
      } catch (IOException e) {
        log("Processor I/O error, cancelling task", e);

        bytesRead = -1;
      } finally {
        // we've done talking to the processor, release the permit
        permitRelease();
      }

      return bytesRead;
    }

    private void permitAcquire() {
      lock.lock();
      try {
        while (procAcquired == maxPermits) {
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

    private void paymentOk() {
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

  }

  // ##################################################################
  // # END: Payment
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

  final String _payment() {
    final Thread thread;
    thread = executeOne();

    if (thread != null) {
      final int before;
      before = trxsIndex;

      try {
        thread.join();

        while (trxsIndex == before) {
          Thread.sleep(50);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      final Trx trx;
      trx = trxs[before];

      return trx.toString();
    }

    return "ERROR";
  }

  void _trx(long time, int proc, int amount) {
    trxs[trxsIndex++] = new Trx(time, proc, amount);
  }

  // ##################################################################
  // # END: Testing API
  // ##################################################################

}
