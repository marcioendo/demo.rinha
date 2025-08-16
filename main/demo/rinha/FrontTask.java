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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope.Subtask;

final class FrontTask implements Runnable {

  private final ByteBuffer buffer;

  private final SocketChannel client;

  private final Front front;

  FrontTask(ByteBuffer buffer, SocketChannel client, Front front) {
    this.buffer = buffer;

    this.client = client;

    this.front = front;
  }

  private static final long ROUTE_POST_PAYMENTS = Shared.asciiLong("POST /pa");

  private static final long ROUTE_POST_PURGE = Shared.asciiLong("POST /pu");

  private static final long ROUTE_GET_SUMMARY = Shared.asciiLong("GET /pay");

  @Override
  public final void run() {
    try (client) {
      // read the request
      final int clientRead;
      clientRead = client.read(buffer);

      if (clientRead <= 0) {
        throw new Shared.TaskException(this, buffer, "No data read from client");
      }

      buffer.flip();

      // route
      final long first;
      first = buffer.getLong();

      if (first == ROUTE_POST_PAYMENTS) {
        payment();
      }

      else if (first == ROUTE_GET_SUMMARY) {
        summary();
      }

      else if (first == ROUTE_POST_PURGE) {
        purge();
      }

      else {
        buffer.rewind();

        Shared.log("unknown", buffer);

        unknown();
      }

      // write response
      while (buffer.hasRemaining()) {
        client.write(buffer);
      }
    } catch (Throwable e) {
      throw new Shared.TaskException(this, buffer, e);
    } finally {
      front.bufferPool(buffer);
    }
  }

  @Override
  public final String toString() {
    return "FrontTask[]";
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
      slice.put(Shared.OP_PURGE);

      slice.flip();

      int bytesRead;
      bytesRead = 0;

      try (SocketChannel back = front.socketChannel()) {
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
      }

      // process the response (if any)
      if (bytesRead == 4) {
        slice.flip();

        final int trx;
        trx = slice.getInt();

        return trx == Shared.PURGE_200;
      } else {
        return Boolean.FALSE;
      }
    }
  }

  private void purge() throws ExecutionException, InterruptedException {
    final PurgeTask purge0;
    purge0 = new PurgeTask(front.back0(), buffer.slice(0, 4));

    final PurgeTask purge1;
    purge1 = new PurgeTask(front.back1(), buffer.slice(4, 4));

    boolean success;
    success = false;

    try (var scope = front.newShutdownOnFailure("purge")) {
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
    }

    buffer.clear();

    buffer.put(success ? Shared.RESP_200 : Shared.RESP_500);

    buffer.flip();
  }

  private void payment() throws IOException {
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

    buffer.put(Shared.OP_PAYMENTS);

    buffer.putLong(front.currentTimeMillis());

    buffer.reset();

    // choose backend
    final SocketAddress backAddress;
    backAddress = front.nextBackAddress();

    int bytesRead;
    bytesRead = 0;

    try (SocketChannel back = front.socketChannel()) {
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
    }

    // process the response (if any)
    if (bytesRead > 0) {
      // forward the response as it is
      buffer.flip();
    } else {
      buffer.clear();

      buffer.put(Shared.RESP_500);

      buffer.flip();
    }
  }

  private void summary() throws ExecutionException, InterruptedException {
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

    buffer.clear();

    final int half;
    half = buffer.capacity() / 2;

    final ByteBuffer buffer0;
    buffer0 = buffer.slice(0, half);

    final ByteBuffer buffer1;
    buffer1 = buffer.slice(half, half);

    final SummaryTask summary0;
    summary0 = new SummaryTask(front.back0(), buffer0, time0, time1);

    final SummaryTask summary1;
    summary1 = new SummaryTask(front.back1(), buffer1, time0, time1);

    Shared.Summary result;
    result = Shared.Summary.ERROR;

    try (var scope = front.newShutdownOnFailure("summary")) {
      final Subtask<Shared.Summary> task0;
      task0 = scope.fork(summary0);

      final Subtask<Shared.Summary> task1;
      task1 = scope.fork(summary1);

      scope.join();

      scope.throwIfFailed();

      final Shared.Summary res0;
      res0 = task0.get();

      final Shared.Summary res1;
      res1 = task1.get();

      result = res0.add(res1);
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

    buffer.flip();
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

  private class SummaryTask implements Callable<Shared.Summary> {

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
    public final Shared.Summary call() throws Exception {
      // assemble request
      buffer.put(Shared.OP_SUMMARY);

      buffer.putLong(time0);

      buffer.putLong(time1);

      buffer.flip();

      int bytesRead;
      bytesRead = 0;

      try (SocketChannel back = front.socketChannel()) {
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

        return new Shared.Summary(
            buffer.getInt(),
            buffer.getInt(),
            buffer.getInt(),
            buffer.getInt()
        );
      } else {
        return Shared.Summary.ERROR;
      }
    }

  }

  private void unknown() throws IOException {
    buffer.put(Shared.OP_UNKNOWN);

    buffer.flip();

    // choose backend
    final SocketAddress backAddress;
    backAddress = front.nextBackAddress();

    try (SocketChannel back = front.socketChannel()) {
      back.connect(backAddress);

      while (buffer.hasRemaining()) {
        back.write(buffer);
      }

      buffer.clear();

      back.read(buffer);
    }

    buffer.flip();
  }

}
