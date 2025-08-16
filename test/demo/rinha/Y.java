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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

final class Y {

  static {}

  private static final Instant FIXED_TIME = Instant.parse("2025-08-10T11:22:33.444Z");

  private static final long FIXED_TIME_MILLIS = FIXED_TIME.toEpochMilli();

  public static long fixedTimeMilis() {
    return FIXED_TIME_MILLIS;
  }

  public static byte[] toByteArray(int value) {
    final ByteBuffer buffer;
    buffer = ByteBuffer.allocate(4);

    buffer.putInt(value);

    return buffer.array();
  }

  // ##################################################################
  // # BEGIN: Back
  // ##################################################################

  public static Back back(Back.Adapter adapter) {
    return back(adapter, "0", "50");
  }

  private static Back back(Back.Adapter adapter, String... args) {
    return Back.boot(adapter, args);
  }

  public static byte[] backMsgSummary(int req0, int amount0, int req1, int amount1) {
    final ByteBuffer buffer;
    buffer = ByteBuffer.allocate(16);

    buffer.putInt(req0);
    buffer.putInt(amount0);

    buffer.putInt(req1);
    buffer.putInt(amount1);

    return buffer.array();
  }

  sealed interface BackAdapterOptions {

    void serverSocketChannel(ServerSocketChannel value);

    void socketChannel(SocketChannel value);

    void socketChannelHealth(SocketChannel value);

  }

  private static final class ThisBackAdapter extends Back.Adapter implements BackAdapterOptions {

    private SocketAddress proc0;

    private SocketAddress proc1;

    private ServerSocketChannel serverSocketChannel;

    private final List<SocketChannel> socketChannels = new ArrayList<>();

    private int socketChannelsIndex;

    private final List<SocketChannel> socketChannelsHealth = new ArrayList<>();

    private int socketChannelsHealthIndex;

    @Override
    public final void serverSocketChannel(ServerSocketChannel value) {
      if (serverSocketChannel != null) {
        throw new IllegalStateException("ServerSocketChannel already defined");
      }

      serverSocketChannel = Objects.requireNonNull(value, "value == null");
    }

    @Override
    public final void socketChannel(SocketChannel value) {
      socketChannels.add(
          Objects.requireNonNull(value, "value == null")
      );
    }

    @Override
    public final void socketChannelHealth(SocketChannel value) {
      socketChannelsHealth.add(
          Objects.requireNonNull(value, "value == null")
      );
    }

    @Override
    final SocketAddress proc0() throws IOException {
      return proc0;
    }

    @Override
    final SocketAddress proc1() throws IOException {
      return proc1;
    }

    @Override
    final ServerSocketChannel serverSocketChannel(Path path) throws IOException {
      return serverSocketChannel;
    }

    @Override
    final SocketChannel socketChannel() throws IOException {
      if (socketChannelsIndex < socketChannels.size()) {
        return socketChannels.get(socketChannelsIndex++);
      } else {
        return null;
      }
    }

    @Override
    final SocketChannel socketChannelHealth() throws IOException {
      if (socketChannelsHealthIndex < socketChannelsHealth.size()) {
        return socketChannelsHealth.get(socketChannelsHealthIndex++);
      } else {
        return null;
      }
    }

    @Override
    final void shutdownHook(Path path) {
      // noop
    }

  }

  public static Back.Adapter backAdapter(Consumer<? super BackAdapterOptions> opts) {
    final ThisBackAdapter builder;
    builder = new ThisBackAdapter();

    opts.accept(builder);

    return builder;
  }

  // ##################################################################
  // # END: Back
  // ##################################################################

  // ##################################################################
  // # BEGIN: ByteBuffer
  // ##################################################################

  public static ByteBuffer byteBuffer(String s) {
    final byte[] ascii;
    ascii = s.getBytes(StandardCharsets.US_ASCII);

    return ByteBuffer.wrap(ascii);
  }

  // ##################################################################
  // # END: ByteBuffer
  // ##################################################################

  // ##################################################################
  // # BEGIN: Front
  // ##################################################################

  public static Front front(Front.Adapter adapter) {
    return Front.boot(adapter);
  }

  sealed interface FrontAdapterOptions {

    void currentTimeMillis(long value);

    void serverSocketChannel(ServerSocketChannel value);

    void socketChannel(SocketChannel value);

  }

  private static final class ThisFrontAdapter extends Front.Adapter implements FrontAdapterOptions {

    private long currentTimeMillis;

    private ServerSocketChannel serverSocketChannel;

    private final List<SocketChannel> socketChannels = new ArrayList<>();

    private int socketChannelsIndex;

    @Override
    public final void currentTimeMillis(long value) {
      currentTimeMillis = value;
    }

    @Override
    public final void serverSocketChannel(ServerSocketChannel value) {
      if (serverSocketChannel != null) {
        throw new IllegalStateException("ServerSocketChannel already defined");
      }

      serverSocketChannel = Objects.requireNonNull(value, "value == null");
    }

    @Override
    public final void socketChannel(SocketChannel value) {
      socketChannels.add(
          Objects.requireNonNull(value, "value == null")
      );
    }

    @Override
    final long currentTimeMillis() {
      return currentTimeMillis;
    }

    @Override
    final ServerSocketChannel serverSocketChannel() throws IOException {
      if (serverSocketChannel == null) {
        throw new IOException("No ServerSocketChannel defined");
      }

      return serverSocketChannel;
    }

    @Override
    final SocketChannel socketChannel() throws IOException {
      if (socketChannelsIndex < socketChannels.size()) {
        return socketChannels.get(socketChannelsIndex++);
      } else {
        return null;
      }
    }

  }

  public static Front.Adapter frontAdapter(Consumer<? super FrontAdapterOptions> opts) {
    final ThisFrontAdapter builder;
    builder = new ThisFrontAdapter();

    opts.accept(builder);

    return builder;
  }

  public static byte[] frontMsgPayment(long time, int strlen, String strval) {
    final ByteBuffer buffer;
    buffer = ByteBuffer.allocate(512);

    buffer.put(Shared.OP_PAYMENTS);

    buffer.putLong(time);

    buffer.put(strval.getBytes(StandardCharsets.US_ASCII));

    buffer.flip();

    final byte[] bytes;
    bytes = new byte[buffer.remaining()];

    buffer.get(bytes);

    return bytes;
  }

  public static byte[] frontMsgPurge() {
    return new byte[] {Shared.OP_PURGE};
  }

  public static byte[] frontMsgSummary(long time0, long time1) {
    final ByteBuffer buffer;
    buffer = ByteBuffer.allocate(512);

    buffer.put(Shared.OP_SUMMARY);

    buffer.putLong(time0);

    buffer.putLong(time1);

    buffer.flip();

    final byte[] bytes;
    bytes = new byte[buffer.remaining()];

    buffer.get(bytes);

    return bytes;
  }

  // ##################################################################
  // # END: Front
  // ##################################################################

  // ##################################################################
  // # BEGIN: ServerSocketChannel
  // ##################################################################

  public sealed interface ServerSocketChannelOptions {

    void socketChannel(SocketChannel value);

  }

  private static final class ThisServerSocketChannel extends YServerSocketChannel implements ServerSocketChannelOptions {

    private final List<Object> sockets = new ArrayList<>();

    private int socketsIndex;

    @Override
    public final void socketChannel(SocketChannel value) {
      sockets.add(
          Objects.requireNonNull(value, "value == null")
      );
    }

    @Override
    public final SocketChannel accept() throws IOException {
      if (socketsIndex < sockets.size()) {
        final Object o;
        o = sockets.get(socketsIndex++);

        if (o == null) {
          return null;
        }

        else if (o instanceof SocketChannel channel) {
          return channel;
        }

        else {
          throw new IllegalArgumentException("Unknown type=" + o.getClass());
        }
      } else {
        return null;
      }
    }

    @Override
    public final ServerSocketChannel bind(SocketAddress local, int backlog) throws IOException {
      return this;
    }

    @Override
    protected final void implConfigureBlocking(boolean block) throws IOException {
      // noop
    }

  }

  public static ServerSocketChannel serverSocketChannel(Consumer<? super ServerSocketChannelOptions> opts) {
    final ThisServerSocketChannel builder;
    builder = new ThisServerSocketChannel();

    opts.accept(builder);

    return builder;
  }

  // ##################################################################
  // # END: ServerSocketChannel
  // ##################################################################

  // ##################################################################
  // # BEGIN: SocketChannel
  // ##################################################################

  public sealed interface SocketChannelOptions {

    void connect(boolean value);

    void readData(byte[] value);

    void readData(int value);

    void readData(String value);

  }

  private static final class Data {

    private final byte[] bytes;

    private int bytesIndex;

    Data(byte[] bytes) {
      this.bytes = bytes.clone();
    }

    final boolean exhausted() {
      return bytesIndex == bytes.length;
    }

    final int read(ByteBuffer dst) {
      final int remaining;
      remaining = dst.remaining();

      if (remaining == 0) {
        return 0;
      }

      final int available;
      available = bytes.length - bytesIndex;

      final int actual;
      actual = Math.min(remaining, available);

      dst.put(bytes, bytesIndex, actual);

      bytesIndex += actual;

      return actual;
    }

  }

  private static final class ThisSocketChannel extends YSocketChannel implements SocketChannelOptions {

    private Object connect = Boolean.TRUE;

    private final List<Data> readData = new ArrayList<>();

    private int readDataIndex;

    private SocketAddress remoteAddress;

    private final ByteArrayOutputStream write = new ByteArrayOutputStream();

    private int writeSpeed = -1;

    @Override
    public final void connect(boolean value) {
      connect = value;
    }

    @Override
    public final void readData(byte[] value) {
      Objects.requireNonNull(value, "value == null");

      final Data d;
      d = new Data(value);

      readData.add(d);
    }

    @Override
    public final void readData(int value) {
      final ByteBuffer buffer;
      buffer = ByteBuffer.allocate(4);

      buffer.putInt(value);

      readData(buffer.array());
    }

    @Override
    public final void readData(String value) {
      final byte[] bytes;
      bytes = value.getBytes(StandardCharsets.US_ASCII);

      readData(bytes);
    }

    @SuppressWarnings("unused")
    public final void writeSpeed(int value) {
      if (value < 0) {
        throw new IllegalArgumentException();
      }

      writeSpeed = value;
    }

    //

    @Override
    public final boolean connect(SocketAddress remote) throws IOException {
      if (connect instanceof Boolean b) {
        final boolean result;
        result = b.booleanValue();

        remoteAddress = remote;

        return result;
      } else {
        throw new UnsupportedOperationException("Implement me :: type=" + connect.getClass());
      }
    }

    @Override
    public final SocketAddress getRemoteAddress() throws IOException {
      return remoteAddress;
    }

    @Override
    public final int read(ByteBuffer dst) throws IOException {
      if (readDataIndex < readData.size()) {
        final Data current;
        current = readData.get(readDataIndex);

        final int result;
        result = current.read(dst);

        if (current.exhausted()) {
          readDataIndex += 1;
        }

        return result;
      } else {
        return -1;
      }
    }

    @Override
    public final int write(ByteBuffer src) throws IOException {
      if (writeSpeed == -1) {
        final byte[] bytes;
        bytes = new byte[src.remaining()];

        src.get(bytes);

        write.write(bytes);

        return bytes.length;
      } else {
        throw new UnsupportedOperationException("Implement me");
      }
    }

    @Override
    protected void implCloseSelectableChannel() throws IOException {
      // noop
    }

    @Override
    protected final void implConfigureBlocking(boolean block) throws IOException {
      // noop
    }

  }

  public static SocketChannel socketChannel(Consumer<? super SocketChannelOptions> opts) {
    final ThisSocketChannel builder;
    builder = new ThisSocketChannel();

    opts.accept(builder);

    return builder;
  }

  public static byte[] socketChannelWrite(SocketChannel channel) {
    if (channel instanceof ThisSocketChannel impl) {
      return impl.write.toByteArray();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public static String socketChannelWriteAscii(SocketChannel channel) {
    return new String(
        socketChannelWrite(channel),
        StandardCharsets.US_ASCII
    );
  }

  // ##################################################################
  // # END: SocketChannel
  // ##################################################################

  // ##################################################################
  // # BEGIN: ShutdownHook
  // ##################################################################

  private static final class ShutdownHook extends Thread {

    static final ShutdownHook INSTANCE = init();

    private final List<AutoCloseable> services = new ArrayList<>();

    private static ShutdownHook init() {
      final ShutdownHook hook;
      hook = new ShutdownHook();

      final Runtime runtime;
      runtime = Runtime.getRuntime();

      runtime.addShutdownHook(hook);

      return hook;
    }

    public final void register(AutoCloseable closeable) {
      Objects.requireNonNull(closeable, "closeable == null");

      services.add(closeable);
    }

    @Override
    public final void run() {
      for (AutoCloseable c : services) {
        try {
          c.close();
        } catch (Throwable e) {
          // not much we can do here
        }
      }
    }

  }

  public static void shutdownHook(AutoCloseable closeable) {
    ShutdownHook.INSTANCE.register(closeable);
  }

  // ##################################################################
  // # END: ShutdownHook
  // ##################################################################

}
