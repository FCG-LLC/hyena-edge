Hyena
-----

[![pipeline status](https://gitlab.cs.int/hyena/hyena/badges/master/pipeline.svg)](https://gitlab.cs.int/hyena/hyena/commits/master)

## Introduction

Hyena aims to be a fast storage engine for timestamp-based data.
It supports parallel scans and appends execution with limited mutation support for log data source.

## Command line options

```
-d, --data-directory <data_dir>    Directory to store data in. [default: data]
-h, --hostname <hostname>          Address to bind nanomsg socket [default: *]
-i, --ipc-path <ipc_path>          Path of nanomsg ipc socket [default: /tmp/hyena.ipc]
-p, --port <port>                  Port number to bind nanomsg socket [default: 4433]
-t, --transport <transport>        Nanomsg transport [default: ipc]  [possible values: tcp, ipc, ws]
```

## Architecture

The main design choices of Hyena:

- have an append-only data model, without direct mutation support
- use `mmap` to access block data, for both faster and easier manipulation
- use compression on the filesystem level to better integrate with `mmap`
- optimize for fast storage (NVMe, SSD) and multiple CPUs
- make use of all RAM that is available (`mmap`, kernel fs caching)

Hyena implements storage on top of `mmap` to map raw block files, without any compression,
so a filesystem with compression support is required for any real world deployment scenario.

Hyena uses nanomsg for communication with the clients.
Every client opens its dedicated nanomsg socket by first connecting to the control socket, `/tmp/hyena.ipc` by default.
After connecting a new client peer socket is created, `/tmp/hyena-session-X.ipc` by default, and used for communication from now on.

## Data model

Hyena conceptually stores all data in a single "hyper table" that combines all sources of data.
Internally the data is partitioned on several levels and not all data gets written everywhere.

The only requirement is that every dense column should be always present in all of the rows.

```
|  ts 	 	 |  source 	| ip                                      	| colA 	| colB 	| colC 	|
|------------|------	|-----------------------------------------	|------	|------	|------	|
| 1500029064 | 1       	| 64ff:9b00::c0a8:102					  	|  12   |   7  	|      	|
| 1500029064 | 1       	| 64ff:9b00::a0e:507					 	|  14  	|   1  	|      	|
| 1500029064 | 2       	| 64ff:9b00::ea59:a684 						|      	|   15  |      	|
| 1500029064 | 3       	| 64ff:9b00::c0a8:104 						|      	|		|   4  	|
| 1500029064 | 2       	| 64ff:9b00::5447:383e 						|      	|   15  |      	|
| 1500029064 | 3       	| 64ff:9b00::d16e:e867 						|      	|		|   10 	|
| 1500029064 | 1       	| 64ff:9b00::a0e:507					 	|  12   |   40 	|      	|
```

### Partitioning

Hyena does partitioning over `source` value, known as `partition group` (vertical)
and also over data volume, `partition` (horizontal).

Every distinct `partition group` has its own separate directory within the filesystem
and can also be concurrently written to.

Each new `partition` has blocks of a fixed size, so as soon as any column fills up its block within
a `partition` a new one is created.

```
|  ts 	|  source 	| ip                                      	| colA 	| colB 	| colC 	|
|-----	|---------	|-----------------------------------------	|------	|------	|------	|
|     	| 1       	| afcf:46c0:a4a5:4d2a:d04b:a085:7b25:961  	|      	|      	|      	|
|     	| 1       	| a6d8:7518:141e:97d7:ef00:d334:caf8:18cd 	|      	|      	|      	|
|     	| 2       	| 59a8:73db:d606:9dbe:e08b:3c6d:29b6:d3e9 	|      	|      	|      	|
```

## Storage

### Directory layout

Hyena stores its data files using the following scheme:

```
.    <- data directory root
├── 3    <- source partition group (packet headers, tls, netflows etc.)
│   ├── 2018-1    <- year-quarter
│   │   ├── 1    <- week number
│   │   │   ├── 12    <- day
│   │   │   │   ├── 02a37d99-e90a-476f-a764-82bcb8e444ea    <- partition id (for now UUID v4, i.e. random)
│   │   │   │   │   ├── block_0.data    <- block file for a column of index 0
│   │   │   │   │   ├── block_2.data
│   │   │   │   │   ├── block_3.data
│   │   │   │   │   ├── block_4.data
│   │   │   │   │   ├── block_4.index    <- index data for a sparse column
│   │   │   │   │   └── meta.data    <- partition meta data
│   └── pgmeta.data    <- partition group (source) meta data
└── catalog.data    <- main catalog file
```

### Block types

Hyena implements two types of storage:

- volatile, memory-backed (used mostly internally)
- persistent, mmap-backed (default)

Block formats:

- dense (not null)
- sparse (nullable)

Block types:

- signed integer
- unsigned integer

### Writes

Writes are performed by splitting data between different sources (⬒ and ⬔), cutting to the partition size
and storing the data in a separate `blocks` for every column.
Columns that do not contain any data within a partition do not have their files present.
As the block files use fs hole punching, the actual on disk size occupied grows by every 4kB non-zero data block.
This, coupled with `mmap` causes lower memory usage for sparse columns.

```
                                                                            .-----------------------------------------------------------.
                                                                            |           718c0eb8-688a-11e7-b174-448a5b6ef270            |
                                                                            |-----------------------------------------------------------|
                                                                            |.-------..--------..-------..--------..--------..--------. |
                                                                            ||  ts   || source ||  ip   ||  colA  ||  colB  ||  colC  | |
                                                                            ||-------||--------||-------||--------||--------||--------| |
                                                                            || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
                                                                            || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
                                                                            || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
                                                                            || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
                                                                .---------->|| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬚⬚⬚⬚⬚⬚ | |
                                                                |.--------->|| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬚⬚⬚⬚⬚⬚ | |
                                                                ||          |'-------''--------''-------''--------''--------''--------' |
                                                                ||          '-----------------------------------------------------------'
                                                                ||          .-----------------------------------------------------------.
                                                                ||          |           df04890c-6899-11e7-aec2-448a5b6ef270            |
                                                                ||          |-----------------------------------------------------------|
                                                                ||          |.-------..--------..-------..--------..--------..--------. |
                                                                ||          ||  ts   || source ||  ip   ||  colA  ||  colB  ||  colC  | |
.-------..--------..-------..--------..--------..--------.      ||          ||-------||--------||-------||--------||--------||--------| |
|  ts   || source ||  ip   ||  colA  ||  colB  ||  colC  |      ||.-------->|| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒  || ⬒⬒⬒⬒⬒  || ⬚⬚⬚⬚⬚  | |
|-------||--------||-------||--------||--------||--------|      |||         || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  | |
| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬚⬚⬚⬚⬚⬚ |------'||         || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  | |
| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬚⬚⬚⬚⬚⬚ |-------'|         || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  | |
| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬚⬚⬚⬚⬚⬚ |--------'         || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  | |
| ⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬔⬔⬔⬔⬔⬔ |--------.         || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  || ⬚⬚⬚⬚⬚  | |
| ⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬔⬔⬔⬔⬔⬔ |-------.|         |'-------''--------''-------''--------''--------''--------' |
'-------''--------''-------''--------''--------''--------'       ||         '-----------------------------------------------------------'
                                                                 ||         .-----------------------------------------------------------.
                                                                 ||         |           e7eee026-6899-11e7-9b09-448a5b6ef270            |
                                                                 ||         |-----------------------------------------------------------|
                                                                 ||         |.-------..--------..-------..--------..--------..--------. |
                                                                 ||         ||  ts   || source ||  ip   ||  colA  ||  colB  ||  colC  | |
                                                                 ||         ||-------||--------||-------||--------||--------||--------| |
                                                                 |'-------->|| ⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬔⬔⬔⬔⬔⬔ | |
                                                                 '--------->|| ⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬔⬔⬔⬔⬔⬔ | |
                                                                            || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
                                                                            || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
                                                                            || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
                                                                            || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
                                                                            |'-------''--------''-------''--------''--------''--------' |
                                                                            '-----------------------------------------------------------'
```

### Scans

Scans are performed in parallel, by simultaneously accessing different `partition groups`/`partitions`/`blocks`
and then materializing to an in-memory temporary data structure.
So in order to conserve memory it is best to split scan operations onto several smaller ones.

```
    .-----------------------------------------------------------.
    |           718c0eb8-688a-11e7-b174-448a5b6ef270            |
    |-----------------------------------------------------------|
    |.-------..--------..-------..--------..--------..--------. |
    ||  ts   || source ||  ip   ||  colA  ||  colB  ||  colC  | |
    ||-------||--------||-------||--------||--------||--------| |
    || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
    || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ◾◾◾◾◾◾ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |--------------.
    || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |              |
    || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ◾◾◾◾◾◾ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |-----------.  |
    || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |           |  |
    || ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ || ◾◾◾◾◾◾ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |--------.  |  |
    |'-------''--------''-------''--------''--------''--------' |        |  |  |
    '-----------------------------------------------------------'        |  |  |
    .-----------------------------------------------------------.        |  |  |
    |           f27b5394-6899-11e7-a1f3-448a5b6ef270            |        |  |  |
    |-----------------------------------------------------------|        |  |  |  .-------..--------..-------.
    |                                                           |        |  |  |  |  ts   || source ||  ip   |
    '-----------------------------------------------------------'        |  |  |  |-------||--------||-------|
    .-----------------------------------------------------------.        |  |  '->| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ |
    |           f6d4cb14-6899-11e7-bd31-448a5b6ef270            |        |  '---->| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ |
    |-----------------------------------------------------------|        '------->| ⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒⬒ || ⬒⬒⬒⬒⬒ |
    |                                                           |        .------->| ⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔ |
    '-----------------------------------------------------------'        |  .---->| ⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔ |
    .-----------------------------------------------------------.        |  |     '-------''--------''-------'
    |           fa9ec63c-6899-11e7-80f3-448a5b6ef270            |        |  |
    |-----------------------------------------------------------|        |  |
    |                                                           |        |  |
    '-----------------------------------------------------------'        |  |
    .-----------------------------------------------------------.        |  |
    |           fe2ab81a-6899-11e7-a53a-448a5b6ef270            |        |  |
    |-----------------------------------------------------------|        |  |
    |.-------..--------..-------..--------..--------..--------. |        |  |
    ||  ts   || source ||  ip   ||  colA  ||  colB  ||  colC  | |        |  |
    ||-------||--------||-------||--------||--------||--------| |        |  |
    || ⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ◾◾◾◾◾◾ | |--------'  |
    || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |           |
    || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |           |
    || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |           |
    || ⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔⬔ || ⬔⬔⬔⬔⬔ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ◾◾◾◾◾◾ | |-----------'
    || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ || ⬚⬚⬚⬚⬚⬚ | |
    |'-------''--------''-------''--------''--------''--------' |
    '-----------------------------------------------------------'
```

## Requirements

- a filesystem with support for `FALLOC_FL_PUNCH_HOLE` fallocate flag (hole punching) [soft req]
- a filesystem with built-in transparent compression support (`btrfs`, `zfs`) [soft req]
- a tuned `vm.max_map_count` kernel setting
- a tuned `ulimit -n`
- a fairly modern x64 CPU with support for `AVX2`/`AVX-512` [soft req]

Soft requirements can be lifted, but will most probably result in a worse performance.

### Limit values tuning

Both `vm.max_map_count` sysctl and `ulimit -n` values depend on the assumed data volumes.
Total map number required by Hyena depends on the total block file count, where every present block file uses a single slot,
with sparse columns requiring additional slot for index data.
A single 1 MiB partition typically holds up to `131072` rows of data, which gives us a rough estimate of required map slots:

```
[slots/s] = [records/s] / [131072] * ([column_count] + [sparse column count])
```

The same general rule applies to open files limit.

### BTRFS compression

It's preferable to use `zstd` compression algorithm, but that is only available in recent (4.14+) kernels.
In its absence the less efficient `lzo` can also be used.
Using `zlib` is generally discouraged due to its bad performance.

To enable lzo compression on Hyena data folder:

```sh
$> btrfs property set hyena compression lzo

```

Verify that it has been properly set:

```sh
$> btrfs property get hyena compression
compression=lzo

```

## Deployment checklist

- [ ] vm.max_map_count is set to an appropriate value
- [ ] data volume uses one of supported filesystems
- [ ] compression is enabled for the storage directory
- [ ] all clients must have access to the `hyena.ipc` main socket as well as to any peer socket opened by hyena
- [ ] maximum open file descriptors limit must be set to an appropriate value

## Known bugs

- terminating hyena during catalog/partition group metadata flush can result in broken metadata files and a panic during restart with `Catalog metadata already exists` error message. Currently the only way of fixing it is full data drop. In order to prevent this error always stop all data producers first.
- in case of insufficient disk space hyena will crash with `signal: 7, SIGBUS: access to undefined memory`

## Troubleshooting

### thread '<unnamed>' panicked at 'called `Result::unwrap()` on an `Err` value: Os { code: 12, kind: Other, message: "Cannot allocate memory" }

#### cause
> not enough memory maps available to the hyena process

#### fix
> increase `vm.max_map_count`

### Other
> see the list of [known bugs](#known-bugs)

## Useful links

- [Side effects when increasing vm.max_map_count](https://www.suse.com/support/kb/doc/?id=7000830)
