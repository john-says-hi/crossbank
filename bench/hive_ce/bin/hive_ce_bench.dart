// Hive CE on the same named workloads as benches/kv.rs, raw Uint8List values.
//
// Native/file backend. The web (IndexedDB) twin is `web/main.dart`; both share
// `lib/workloads.dart` so the rows are comparable.
//
// Prints one JSON document (schema: bench/results/README.md) on stdout.
// Run: ci/bench.sh --hive   (or `dart run bin/hive_ce_bench.dart` from here)
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:hive_ce/hive.dart';
import 'package:hive_ce_bench/workloads.dart';

const backend = 'hive_ce_file';

Future<void> main() async {
  final dir = await Directory.systemTemp.createTemp('hive_ce_bench');
  Hive.init(dir.path);
  final samples = <Sample>[];
  var boxId = 0;
  String fresh() => 'b${boxId++}';

  final sw = Stopwatch()..start();
  double now() => sw.elapsedMicroseconds.toDouble();

  // settings_eager: 200 keys x 1 KiB, eager Box, 90/10 get/put, one op per
  // iteration like the Criterion loop; we time 1000 ops and report per op.
  {
    late Box<Uint8List> box;
    var i = 0;
    samples.add(await timed(
        'settings_eager', backend, settingsOps, settingsBytes, now, () async {
      box = await Hive.openBox<Uint8List>(fresh());
      for (var k = 0; k < settingsN; k++) {
        await box.put(key(k), payload(settingsBytes, k));
      }
    }, () async {
      for (var op = 0; op < settingsOps; op++) {
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
    samples.add(await timed(
        'bulk_lazy_put', backend, bulkN, bulkN * bulkBytes, now, () async {
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
    samples.add(await timed(
        'bulk_lazy_get', backend, bulkGetOps, bulkBytes, now, () async {
      box = await Hive.openLazyBox<Uint8List>(fresh());
      for (var k = 0; k < bulkN; k++) {
        await box.put(key(k), payload(bulkBytes, k));
      }
    }, () async {
      for (var op = 0; op < bulkGetOps; op++) {
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
    samples.add(
        await timed('txn_batch', backend, txnN, txnN * 64, now, () async {
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
    samples.add(await timed('reopen', backend, 1, 1024, now, () async {
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
    samples.add(
        await timed('big_value_put_get', backend, 1, bigBytes, now, () async {
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
