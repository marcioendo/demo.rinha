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
import java.time.Instant;

final class BackTask implements Runnable {

  private final Back back;

  private final ByteBuffer buffer;

  private final SocketChannel front;

  BackTask(Back back, ByteBuffer buffer, SocketChannel front) {
    this.back = back;

    this.buffer = buffer;

    this.front = front;
  }

  @Override
  public final void run() {
    try {
      run0();
    } catch (Throwable e) {
      throw new Shared.TaskException(e);
    } finally {
      back.bufferPool(buffer);
    }
  }

  private void run0() throws IOException {
    byte op = 0;

    int paymentLimit = 0;

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
      op = buffer.get();

      switch (op) {
        case Shared.OP_PURGE -> {
          back.purge();

          buffer.clear();

          buffer.putInt(Shared.PURGE_200);

          buffer.flip();
        }

        case Shared.OP_PAYMENTS -> {
          paymentLimit = buffer.limit();

          // send response straight away
          final byte[] resp;
          resp = Shared.RESP_200;

          buffer.position(paymentLimit);

          buffer.limit(paymentLimit + resp.length);

          buffer.put(paymentLimit, resp);
        }

        case Shared.OP_SUMMARY -> {
          final long time0;
          time0 = buffer.getLong();

          final long time1;
          time1 = buffer.getLong();

          final Shared.Summary summary;
          summary = back.summary(time0, time1);

          buffer.clear();

          buffer.putInt(summary.req0());

          buffer.putInt(summary.amount0());

          buffer.putInt(summary.req1());

          buffer.putInt(summary.amount1());

          buffer.flip();
        }

        default -> {
          buffer.clear();

          buffer.put(Shared.RESP_404);

          buffer.flip();
        }
      }

      // write response
      while (buffer.hasRemaining()) {
        front.write(buffer);
      }
    }

    if (op == Shared.OP_PAYMENTS) {
      buffer.position(1); // skip op

      buffer.limit(paymentLimit);

      // process payment
      paymentTask();
    }
  }

  // we'll use HTTP/1.0 so we can skip the Host header...
  private static final byte[] PAYMENT_REQ = Shared.asciiBytes("""
    POST /payments HTTP/1.0\r
    Content-Type: application/json\r
    Content-Length: \
    """);

  private static final int PAYMENT_REQ_TRAILER_LEN = "\"requestedAt\":\"2025-07-15T12:34:56.000Z\"}".length();
  private static final byte[] REQUESTED_AT = Shared.asciiBytes(",\"requestedAt\":\"");
  private static final byte[] CRLFCRLF = Shared.asciiBytes("\r\n\r\n");

  private static final long PAYMENT_RESP = Shared.asciiLong("HTTP/1.1");
  private static final long PAYMENT_RESP_200 = Shared.asciiLong(" 200 OK\r");
  private static final long PAYMENT_RESP_400 = Shared.asciiLong(" 400 Bad");
  private static final long PAYMENT_RESP_500 = Shared.asciiLong(" 500 Int");

  private void paymentTask() throws IOException {
    //
    // trx time
    //

    final long time;
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
    final int amount;
    amount = parseAmount(json, jsonIdx);

    // json: requestedAt prop
    System.arraycopy(REQUESTED_AT, 0, json, jsonIdx, REQUESTED_AT.length);

    jsonIdx += REQUESTED_AT.length;

    final Instant instant;
    instant = Instant.ofEpochMilli(time);

    final byte[] instantBytes;
    instantBytes = Shared.asciiBytes(instant.toString());

    System.arraycopy(instantBytes, 0, json, jsonIdx, instantBytes.length);

    jsonIdx += instantBytes.length;

    // json: trailer
    json[jsonIdx++] = '\"';
    json[jsonIdx++] = '}';

    //
    // Content-Length
    //

    final int contentLength;
    contentLength = json.length;

    final String contentLengthValue;
    contentLengthValue = Integer.toString(contentLength);

    final byte[] contentLengthBytes;
    contentLengthBytes = Shared.asciiBytes(contentLengthValue);

    //
    // assemble payment processor request
    //

    while (true) {
      // before talking to the processor, obtain a permit
      // this is required to prevent ddosing the payment processor
      back.permitAcquire();

      // always use the default processor
      final SocketAddress procAddres;
      procAddres = back.procAddress();

      int bytesRead;
      bytesRead = 0;

      try (SocketChannel processor = back.socketChannel()) {
        processor.connect(procAddres);

        // send request
        buffer.clear();

        buffer.put(PAYMENT_REQ);

        buffer.put(contentLengthBytes);

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
        bytesRead = processor.read(buffer);
      } finally {
        // we've done talking to the processor, release the permit
        back.permitRelease();
      }

      // process the response (if any)
      if (bytesRead > 0) {
        buffer.flip();

        final long long0;
        long0 = buffer.getLong();

        if (long0 != PAYMENT_RESP) {
          throw new Shared.TaskException("Unexpected payment processor response", buffer);
        }

        final long long1;
        long1 = buffer.getLong();

        if (long1 == PAYMENT_RESP_200) {
          back.paymentOk(time, amount);

          // our work is done
          return;
        }

        else if (long1 == PAYMENT_RESP_400 || long1 == PAYMENT_RESP_500) {
          back.paymentRetry();
        }

        else {
          throw new Shared.TaskException("Unexpected payment processor response", buffer);
        }
      } else {
        throw new Shared.TaskException("No data from payment processor", buffer);
      }
    }
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
        Shared.logf("amount parse error: expected digit but got %x%n", b);

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
      default -> Shared.logf("amount parse error: more than 2 decimals %d%n", decimals);
    }

    return amount;
  }

}
