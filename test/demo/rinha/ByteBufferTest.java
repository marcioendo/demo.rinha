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

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.testng.annotations.Test;

public class ByteBufferTest {

  @Test
  public void testCase01() {
    ByteBuffer buffer;
    buffer = ByteBuffer.allocate(32);

    buffer.put((byte) 'G');
    buffer.put((byte) 'E');
    buffer.put((byte) 'T');
    buffer.put((byte) ' ');

    buffer.flip();

    int method;
    method = buffer.getInt();

    assertEquals(method >>> 24 & 0xFF, 'G');
    assertEquals(method >>> 16 & 0xFF, 'E');
    assertEquals(method >>> 8 & 0xFF, 'T');
    assertEquals(method >>> 0 & 0xFF, ' ');
  }

}
