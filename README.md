# Rinha de Backend

A pure Java solution for the [Rinha de Backend 2025](https://github.com/zanfranceschi/rinha-de-backend-2025) challenge.
It showcases:

- No frameworks.
- No external libraries (except for testing).
- No external data stores.
- No external message brokers.
- No external load balancers.
- Tests.

## Quick Start

To build and run the solution locally, you'll need:

- GNU Make 4.4.1 (or later)
- GraalVM 24+
- Payment processors are running ([guide](https://github.com/zanfranceschi/rinha-de-backend-2025/blob/main/rinha-test/MINIGUIA.md))

Ensure the following environment variables are set:

- `JAVA_HOME` points to the GraalVM 24+ distribution or to a JDK 24+ distribution.
- `GRAALVM_HOME` points to the GraalVM 24+ distribution.

Clone the repository to a local directory:

```shell
git clone git@github.com:marcioendo/demo.rinha.git
cd demo.rinha
```

Build and start the services by running:

```shell
make compose
```

On the console, wait for messages similar to:

```
back1  | Back
back1  | proc0=payment-processor-default/172.18.0.3:8080
back1  | proc1=payment-processor-fallback/172.18.0.2:8080
back0  | Back
back0  | proc0=payment-processor-default/172.18.0.3:8080
back0  | proc1=payment-processor-fallback/172.18.0.2:8080
front  | Front
```

Run the challenge tests by following the instructions:

- https://github.com/zanfranceschi/rinha-de-backend-2025/blob/main/rinha-test/README.md

## License

Copyright (C) 2025 Marcio Endo.

Licensed under the Apache License, Version 2.0.
