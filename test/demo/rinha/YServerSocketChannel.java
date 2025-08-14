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
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

class YServerSocketChannel extends ServerSocketChannel {

  YServerSocketChannel() {
    super(null);
  }

  @Override
  public <T> T getOption(SocketOption<T> name) throws IOException { throw new UnsupportedOperationException("Implement me"); }

  @Override
  public Set<SocketOption<?>> supportedOptions() { throw new UnsupportedOperationException("Implement me"); }

  @Override
  public ServerSocketChannel bind(SocketAddress local, int backlog) throws IOException { throw new UnsupportedOperationException("Implement me"); }

  @Override
  public <T> ServerSocketChannel setOption(SocketOption<T> name, T value) throws IOException { throw new UnsupportedOperationException("Implement me"); }

  @Override
  public ServerSocket socket() { throw new UnsupportedOperationException("Implement me"); }

  @Override
  public SocketChannel accept() throws IOException { throw new UnsupportedOperationException("Implement me"); }

  @Override
  public SocketAddress getLocalAddress() throws IOException { throw new UnsupportedOperationException("Implement me"); }

  @Override
  protected void implCloseSelectableChannel() throws IOException { throw new UnsupportedOperationException("Implement me"); }

  @Override
  protected void implConfigureBlocking(boolean block) throws IOException { throw new UnsupportedOperationException("Implement me"); }

}
