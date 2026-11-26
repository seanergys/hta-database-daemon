<!--
SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>

SPDX-License-Identifier: MIT
-->

# HTA database daemon

A SeanerBUS client that records metrics into an [HTA](https://github.com/metricq/hta)
time series database and answers history requests for them.

The daemon

- subscribes to every metric listed in `metrics.list` and stores the values it receives,
- registers itself at the naming service as `{"service": "time_series_db"}`, so that other
  clients can find it, and
- answers `HistoryReqV1` requests with raw or aggregated values from the database.

## Building

### Requirements

- a C++17 compiler, CMake 3.11 or newer, and `pkg-config`
- Cap'n Proto (`capnproto` and its development files)
- Git, to fetch the submodules

The SeanerBUS C++ client (`lib/cpp`) and HTA (`lib/hta`) are Git submodules, and `lib/cpp` brings
its own submodules (fmt and the message schemas). Fetch them all, then build:

```console
git clone --recurse-submodules git@gitlab.seanergys.fz-juelich.de:software/hta-database-daemon.git
cd hta-database-daemon
cmake -B build
cmake --build build -j
```

In an existing clone, `git submodule update --init --recursive` fetches the submodules.

The result is `build/hta-database-daemon`. To install it into `<prefix>/bin`:

```console
cmake --install build --prefix <prefix>
```

### With Spack

[Seanerstack](https://gitlab.seanergys.fz-juelich.de/software/seanerstack) has a recipe for it:

```console
spack install seanerbus-hta@main
```

## Running

The daemon needs a running SeanerBUS broker, which it expects at `seanerbroker://localhost:5398`.
Start one first, for example with the `seanerbroker` of the SeanerBUS:

```console
seanerbroker 127.0.0.1:5398
```

The daemon takes no command line arguments. It reads and writes in its working directory:

| Path           | Purpose                                                                  |
| -------------- | ------------------------------------------------------------------------ |
| `metrics.list` | The metrics to record: one entity UUID per line. At least one is needed. |
| `data/`        | The database. HTA creates it on the first start.                         |

Create `metrics.list` and start the daemon from the directory that should hold the data:

```console
mkdir -p /var/lib/hta-database && cd /var/lib/hta-database
echo 15a1a4c6-e83b-4699-bbdb-448b26edf1cc > metrics.list
hta-database-daemon
```

Without a `metrics.list` that lists at least one metric, it prints a message and exits with 1.

To find the database from another client, resolve `{"service": "time_series_db"}` at the naming
service and send it a `HistoryReqV1`.

## Limitations

The broker address, the log level (`TRACE`) and the storage settings are fixed in `src/database.cpp`
for now.

## License

MIT, see [LICENSES/MIT.txt](LICENSES/MIT.txt).
