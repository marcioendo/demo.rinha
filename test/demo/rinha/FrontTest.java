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

import org.testng.annotations.Test;

public class FrontTest {

  @Test
  public void testCase00() {
    assertEquals(
        """
        POST /payments HTTP/1.1\r
        Host: localhost:9999\r
        User-Agent: Grafana k6/1.1.0\r
        Content-Length: 70\r
        Content-Type: application/json\r
        \r
        """.length(),

        131
    );
  }

  /*
  
  POST /purge-payments HTTP/1.1
  Host: localhost:9999
  User-Agent: Grafana k6/1.1.0
  Content-Length: 0
  Content-Type: application/json
  
  
  GET /payments-summary?from=2025-08-05T22:02:28.297Z&to=2025-08-05T22:02:41.797Z HTTP/1.1
  Host: localhost:9999
  User-Agent: Grafana k6/1.1.0
  Content-Type: application/json
  
  ERRO[0001] Não foi possível obter resposta de '/payments-summary?from=2025-08-06T11:04:46.238Z&to=2025-08-06T11:04:59.738Z' para o backend (HTTP 400)  source=console
  ERRO[0011] Não foi possível obter resposta de '/payments-summary?from=2025-08-06T11:04:56.244Z&to=2025-08-06T11:05:09.744Z' para o backend (HTTP 400)  source=console
  ERRO[0021] Não foi possível obter resposta de '/payments-summary?from=2025-08-06T11:05:06.246Z&to=2025-08-06T11:05:19.746Z' para o backend (HTTP 400)  source=console
  ERRO[0031] Não foi possível obter resposta de '/payments-summary?from=2025-08-06T11:05:16.247Z&to=2025-08-06T11:05:29.747Z' para o backend (HTTP 400)  source=console
  ERRO[0041] Não foi possível obter resposta de '/payments-summary?from=2025-08-06T11:05:26.249Z&to=2025-08-06T11:05:39.749Z' para o backend (HTTP 400)  source=console
  ERRO[0051] Não foi possível obter resposta de '/payments-summary?from=2025-08-06T11:05:36.251Z&to=2025-08-06T11:05:49.751Z' para o backend (HTTP 400)  source=console
  ERRO[0062] Não foi possível obter resposta de '/payments-summary?from=2025-08-06T11:04:52.238Z&to=2025-08-06T11:06:02.238Z' para o backend (HTTP 400)  source=console
  
   */

}
