// Hive CE on IndexedDB, in a real browser, on the same named workloads and
// byte payloads as the native tool (`bin/hive_ce_bench.dart`).
//
// This is the Hive half of the one comparison that decides "can crossbank
// replace Hive on the web": Hive CE IndexedDB vs crossbank IndexedDB, same
// browser, same bytes.
//
// Build:  dart compile js -O2 web/main.dart -o web/main.dart.js
// Drive:  ci/web-bench/run.mjs   (or ci/bench.sh --hive --web)
//
// It prints ONE compact JSON document to the console prefixed with
// `BENCH_JSON ` and also parks it on `globalThis.__benchResult` with
// `globalThis.__benchDone = true`, so the driver has a reliable poll as well
// as a console line.
import 'dart:convert';
import 'dart:js_interop';
import 'dart:typed_data';

import 'package:hive_ce/hive.dart';
import 'package:hive_ce_bench/workloads.dart';

const backend = 'hive_ce_web';

@JS('performance.now')
external double _perfNow();

@JS('location.search')
external JSString get _locationSearch;

@JS('__benchResult')
external set _benchResult(JSAny? value);

@JS('__benchDone')
external set _benchDone(JSAny? value);

@JS('__benchError')
external set _benchError(JSAny? value);

@JS('__benchProgress')
external set _benchProgress(JSAny? value);

/// microseconds
double now() => _perfNow() * 1000;

int _iterations() {
  final q = Uri.splitQueryString(
    _locationSearch.toDart.replaceFirst(RegExp(r'^\?'), ''),
  );
  return int.tryParse(q['iters'] ?? '') ?? iterations;
}

/// `timed()` in workloads.dart is fixed at [iterations]; the web lane may need
/// a smaller count because every IndexedDB write is a real transaction. This
/// is the same maths with the count injected.
Future<Sample> timedN(
  String workload,
  int n,
  int bytes,
  int count,
  Future<void> Function() setup,
  Future<void> Function() body,
  Future<void> Function() teardown,
) async {
  _benchProgress = workload.toJS;
  print('running $workload');
  final micros = <double>[];
  await setup();
  await body();
  await teardown();
  for (var i = 0; i < count; i++) {
    await setup();
    final start = now();
    await body();
    micros.add(now() - start);
    await teardown();
  }
  return Sample(workload, backend, n, bytes, micros);
}

Future<void> main() async {
  final count = _iterations();
  try {
    await _run(count);
  } catch (e, st) {
    _benchError = '$e\n$st'.toJS;
    _benchDone = true.toJS;
    print('BENCH_ERROR $e');
    rethrow;
  }
}

Future<void> _run(int count) async {
  Hive.init(null);
  final samples = <Sample>[];
  var boxId = 0;
  // Fresh IndexedDB database per iteration; a stale one from a previous run in
  // the same profile would make the first iteration lie.
  String fresh() => 'wb${DateTime.now().microsecondsSinceEpoch}_${boxId++}';

  // ---- Large shapes: identical to bin/hive_ce_bench.dart. ----

  {
    late Box<Uint8List> box;
    var i = 0;
    samples.add(await timedN('settings_eager', settingsOps, settingsBytes,
        count, () async {
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

  {
    late LazyBox<Uint8List> box;
    samples.add(await timedN('bulk_lazy_put', bulkN, bulkN * bulkBytes, count,
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

  {
    late LazyBox<Uint8List> box;
    samples.add(await timedN('bulk_lazy_get', bulkGetOps, bulkBytes, count,
        () async {
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

  {
    late LazyBox<Uint8List> box;
    var gen = 0;
    samples.add(await timedN('txn_batch', txnN, txnN * 64, count, () async {
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

  {
    late String name;
    samples.add(await timedN('reopen', 1, 1024, count, () async {
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

  {
    late LazyBox<Uint8List> box;
    final big = payload(bigBytes, 3);
    samples.add(await timedN('big_value_put_get', 1, bigBytes, count, () async {
      box = await Hive.openLazyBox<Uint8List>(fresh());
    }, () async {
      await box.put('k', big);
      await box.get('k');
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  // ---- Small shapes: byte-identical to tests/bench_web.rs, so there is at
  // least one apples-to-apples web pair today. ----

  {
    late Box<Uint8List> box;
    samples.add(await timedN('settings_eager_web_small', smallSettingsOps,
        settingsBytes, count, () async {
      box = await Hive.openBox<Uint8List>(fresh());
      for (var k = 0; k < smallSettingsN; k++) {
        await box.put(smallKey(k), payload(settingsBytes, k));
      }
    }, () async {
      for (var op = 0; op < smallSettingsOps; op++) {
        box.get(smallKey(1));
      }
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  {
    late LazyBox<Uint8List> box;
    samples.add(await timedN('bulk_lazy_put_web_small', smallBulkN,
        smallBulkN * bulkBytes, count, () async {
      box = await Hive.openLazyBox<Uint8List>(fresh());
    }, () async {
      for (var k = 0; k < smallBulkN; k++) {
        await box.put(smallKey(k), payload(bulkBytes, k));
      }
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  {
    late LazyBox<Uint8List> box;
    samples.add(await timedN(
        'bulk_lazy_get_web_small', smallBulkN, bulkBytes, count, () async {
      box = await Hive.openLazyBox<Uint8List>(fresh());
      for (var k = 0; k < smallBulkN; k++) {
        await box.put(smallKey(k), payload(bulkBytes, k));
      }
    }, () async {
      for (var op = 0; op < smallBulkN; op++) {
        await box.get(smallKey(1));
      }
    }, () async {
      await box.deleteFromDisk();
    }));
  }

  await Hive.close();

  final doc = {
    'tool': 'bench/hive_ce/web',
    'date_utc': DateTime.now().toUtc().toIso8601String(),
    'dart': 'dart2js',
    'iterations': count,
    'samples': samples.map((s) => s.toJson()).toList(),
  };
  final compact = jsonEncode(doc);
  _benchResult = compact.toJS;
  _benchDone = true.toJS;
  print('BENCH_JSON $compact');
}
