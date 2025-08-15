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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
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

  private final int maxPermits;

  private final SocketAddress proc0;

  private int procAcquired;

  private final Condition procReady = lock.newCondition();

  private final Condition procReleased = lock.newCondition();

  private final Trx[] trxs = new Trx[MAX_TRXS];

  private int trxsIndex;

  private int waiting;

  private Back(
      Adapter adapter,
      ServerSocketChannel channel,
      int maxPermits,
      SocketAddress proc0,
      ThreadFactory taskFactory) {
    super(channel, taskFactory);

    this.adapter = adapter;

    this.maxPermits = maxPermits;

    this.proc0 = proc0;
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

    final SocketAddress proc0;

    try {
      proc0 = adapter.proc0();
    } catch (IOException e) {
      log("Failed to init payment processor addresses", e);

      return null;
    }

    logf("proc0=%s%n", proc0);

    //
    // Task Factory
    //

    final ThreadFactory taskFactory;
    taskFactory = Thread.ofVirtual().name("task-", 1).factory();

    //
    // Back
    //

    return new Back(adapter, channel, maxPermits, proc0, taskFactory);
  }

  // ##################################################################
  // # END: Boot
  // ##################################################################

  // ##################################################################
  // # BEGIN: Task
  // ##################################################################

  final void paymentOk(long time, int amount) {
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

  final void paymentRetry() {
    lock.lock();
    try {
      waiting += 1;

      procReady.awaitUninterruptibly();

      waiting -= 1;
    } finally {
      lock.unlock();
    }
  }

  final void permitAcquire() {
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

  final void permitRelease() {
    lock.lock();
    try {
      procAcquired -= 1;

      procReleased.signal();
    } finally {
      lock.unlock();
    }
  }

  final SocketAddress procAddress() {
    return proc0;
  }

  final void purge() {
    trxsIndex = 0;
  }

  @Override
  final SocketChannel socketChannel() throws IOException {
    return adapter.socketChannel();
  }

  final Summary summary(long time0, long time1) {
    int req0 = 0, req1 = 0, amount0 = 0, amount1 = 0;

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

    return new Shared.Summary(req0, amount0, req1, amount1);
  }

  @Override
  final Runnable task(ByteBuffer buffer, SocketChannel channel) {
    return new BackTask(this, buffer, channel);
  }

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

  final String _payment() {
    try {
      final Thread thread;
      thread = executeOne();

      if (thread != null) {
        final int before;
        before = trxsIndex;

        thread.join();

        while (trxsIndex == before) {
          Thread.sleep(50);
        }

        final Trx trx;
        trx = trxs[before];

        return trx.toString();
      }

      return "ERROR";
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  void _trx(long time, int proc, int amount) {
    trxs[trxsIndex++] = new Trx(time, proc, amount);
  }

  // ##################################################################
  // # END: Testing API
  // ##################################################################

}
