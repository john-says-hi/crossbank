// Shared workload shapes for the Hive CE benches.
//
// This file is imported by BOTH the native tool (`bin/hive_ce_bench.dart`,
// file backend) and the web tool (`web/main.dart`, IndexedDB backend) so the
// two emit rows that are directly comparable: same names, same N, same byte
// payloads, same iteration count, same median/p99 maths.
//
// It must stay free of `dart:io` and `dart:html` so it compiles on both.
import 'dart:typed_data';

/// Native/large shapes — mirror `benches/kv.rs`.
const settingsN = 200;
const settingsBytes = 1024;
const settingsOps = 1000;
const bulkN = 2000;
const bulkBytes = 256;
const bulkGetOps = 1000;
const txnN = 100;
const bigBytes = 8 * 1024 * 1024;
const iterations = 20;

/// Small shapes — mirror `tests/bench_web.rs` exactly, so there is at least
/// one apples-to-apples web pair today. See PLAN.md: unifying `bench_web.rs`
/// onto the large shapes is the remaining Phase 5 item.
const smallSettingsN = 50;
const smallSettingsOps = 200;
const smallBulkN = 200;

Uint8List payload(int n, int seed) {
  final out = Uint8List(n);
  for (var i = 0; i < n; i++) {
    out[i] = (seed + i) & 0xff;
  }
  return out;
}

String key(int i) => 'k${i.toString().padLeft(6, '0')}';

/// `tests/bench_web.rs` uses 4-digit keys; match its byte-for-byte key width
/// for the `_web_small` rows.
String smallKey(int i) => 'k${i.toString().padLeft(4, '0')}';

class Sample {
  Sample(this.workload, this.backend, this.n, this.bytes, this.micros);
  final String workload;
  final String backend;
  final int n;
  final int bytes;
  final List<double> micros;

  Map<String, Object> toJson() {
    final sorted = [...micros]..sort();
    double pct(double p) => sorted[((sorted.length - 1) * p).round()];
    final p50 = pct(0.5);
    return {
      'workload': workload,
      'backend': backend,
      'n': n,
      'bytes': bytes,
      'p50_ms': p50 / 1000,
      'p99_ms': pct(0.99) / 1000,
      'ops_per_s': p50 == 0 ? 0 : n / (p50 / 1e6),
    };
  }
}

/// Elapsed microseconds. On the VM this is `Stopwatch`; on the web the caller
/// injects `performance.now()` so the numbers are not quantised to whole
/// milliseconds.
typedef Clock = double Function();

/// Run [body] [iterations] times (plus one un-timed warm-up), timing only the
/// body, and return the median/p99 sample.
Future<Sample> timed(
  String workload,
  String backend,
  int n,
  int bytes,
  Clock nowMicros,
  Future<void> Function() setup,
  Future<void> Function() body,
  Future<void> Function() teardown,
) async {
  final micros = <double>[];
  await setup();
  await body();
  await teardown();
  for (var i = 0; i < iterations; i++) {
    await setup();
    final start = nowMicros();
    await body();
    micros.add(nowMicros() - start);
    await teardown();
  }
  return Sample(workload, backend, n, bytes, micros);
}
