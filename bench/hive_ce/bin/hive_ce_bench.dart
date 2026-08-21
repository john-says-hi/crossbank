// Hive CE on the same named workloads as benches/kv.rs, raw Uint8List values.
//
// Prints one JSON document (schema: bench/results/README.md) on stdout.
// Run: dart run bench/hive_ce  (from the crossbank root)
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:hive_ce/hive.dart';

const settingsN = 200;
const settingsBytes = 1024;
const bulkN = 2000;
const bulkBytes = 256;
const txnN = 100;
const bigBytes = 8 * 1024 * 1024;
const iterations = 20;

Uint8List payload(int n, int seed) {
  final out = Uint8List(n);
  for (var i = 0; i < n; i++) {
    out[i] = (seed + i) & 0xff;
  }
  return out;
}

String key(int i) => 'k${i.toString().padLeft(6, '0')}';

class Sample {
  Sample(this.workload, this.n, this.bytes, this.micros);
  final String workload;
  final int n;
  final int bytes;
  final List<double> micros;

  Map<String, Object> toJson() {
    final sorted = [...micros]..sort();
    double pct(double p) => sorted[((sorted.length - 1) * p).round()];
    final p50 = pct(0.5);
    return {
      'workload': workload,
      'backend': 'hive_ce_file',
      'n': n,
      'bytes': bytes,
      'p50_ms': p50 / 1000,
      'p99_ms': pct(0.99) / 1000,
      'ops_per_s': p50 == 0 ? 0 : n / (p50 / 1e6),
    };
  }
}

Future<Sample> timed(
  String workload,
  int n,
  int bytes,
  Future<void> Function() setup,
  Future<void> Function() body,
  Future<void> Function() teardown,
) async {
  final micros = <double>[];
  // Warm-up.
  await setup();
  await body();
  await teardown();
  for (var i = 0; i < iterations; i++) {
    await setup();
    final sw = Stopwatch()..start();
    await body();
    sw.stop();
    micros.add(sw.elapsedMicroseconds.toDouble());
    await teardown();
  }
  return Sample(workload, n, bytes, micros);
}

Future<void> main() async {
  final dir = await Directory.systemTemp.createTemp('hive_ce_bench');
  Hive.init(dir.path);
  final samples = <Sample>[];
  var boxId = 0;
  String fresh() => 'b${boxId++}';

  // settings_eager: 200 keys x 1 KiB, eager Box, 90/10 get/put, one op per
  // iteration like the Criterion loop; we time 1000 ops and report per op.
  {
    late Box<Uint8List> box;
    var i = 0;
    samples.add(await timed('settings_eager', 1000, settingsBytes, () async {
      box = await Hive.openBox<Uint8List>(fresh());
      for (var k = 0; k < settingsN; k++) {
        await box.put(key(k), payload(settingsBytes, k));
      }
    }, () async {
      for (var op = 0; op < 1000; op++) {
        if (i % 10 == 0) {
          await box.put(key(i % settingsN), payload(settingsBytes, i));
        } else {
          box.get(key(i % settingsN));
        }
        i++;
      }
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  // bulk_lazy_put: 2000 x 256 B into a LazyBox, one put each.
  {
    late LazyBox<Uint8List> box;
    samples.add(await timed('bulk_lazy_put', bulkN, bulkN * bulkBytes,
        () async {
      box = await Hive.openLazyBox<Uint8List>(fresh());
    }, () async {
      for (var k = 0; k < bulkN; k++) {
        await box.put(key(k), payload(bulkBytes, k));
      }
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  // bulk_lazy_get: 1000 random-ish gets over that set.
  {
    late LazyBox<Uint8List> box;
    samples.add(await timed('bulk_lazy_get', 1000, bulkBytes, () async {
      box = await Hive.openLazyBox<Uint8List>(fresh());
      for (var k = 0; k < bulkN; k++) {
        await box.put(key(k), payload(bulkBytes, k));
      }
    }, () async {
      for (var op = 0; op < 1000; op++) {
        await box.get(key((op * 7919) % bulkN));
      }
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  // txn_batch: 100 puts in one putAll (Hive's closest thing to a transaction).
  {
    late LazyBox<Uint8List> box;
    var gen = 0;
    samples.add(await timed('txn_batch', txnN, txnN * 64, () async {
      box = await Hive.openLazyBox<Uint8List>(fresh());
    }, () async {
      gen++;
      await box.putAll({
        for (var k = 0; k < txnN; k++) '$gen:$k': payload(64, k),
      });
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  // reopen: write one 1 KiB value, close, reopen, read.
  {
    late String name;
    samples.add(await timed('reopen', 1, 1024, () async {
      name = fresh();
      final box = await Hive.openLazyBox<Uint8List>(name);
      await box.put('k', payload(1024, 1));
      await box.close();
    }, () async {
      final box = await Hive.openLazyBox<Uint8List>(name);
      await box.get('k');
      await box.close();
    }, () async {
      await Hive.deleteBoxFromDisk(name);
    }));
  }

  // big_value: one 8 MiB value put + get (crossbank chunk_sweep shape).
  {
    late LazyBox<Uint8List> box;
    final big = payload(bigBytes, 3);
    samples.add(await timed('big_value_put_get', 1, bigBytes, () async {
      box = await Hive.openLazyBox<Uint8List>(fresh());
    }, () async {
      await box.put('k', big);
      await box.get('k');
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  await Hive.close();
  await dir.delete(recursive: true);

  stdout.writeln(const JsonEncoder.withIndent('  ').convert({
    'tool': 'bench/hive_ce',
    'date_utc': DateTime.now().toUtc().toIso8601String(),
    'dart': Platform.version,
    'iterations': iterations,
    'samples': samples.map((s) => s.toJson()).toList(),
  }));
}
