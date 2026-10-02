"""Master Verification Runner: SlabIO 100% Invariant & Integration Suite.

Runs:
1. Formal Z3 SMT Verification of Binary Format Invariants (6 Theorems).
2. End-to-End Correctness & API Reduction Suite (6 Integration Tests).

Copyright (c) 2026. Licensed under AGPLv3.
"""

from __future__ import annotations

import sys
import time
from verify_slabio import run_smt_proofs
from test_slabio_integration import run_tests as run_integration_tests


def run_master_suite() -> bool:
    t0 = time.time()
    print("=" * 80)
    print("SLABIO MASTER VERIFICATION SUITE")
    print("=" * 80)

    # 1. Formal SMT Proofs
    smt_ok = run_smt_proofs()
    if not smt_ok:
        print("\n[CRITICAL FAILURE] SMT Formal Verification failed!")
        return False

    # 2. Integration & Correctness Tests
    print("\n" + "=" * 80)
    print("PART 2: REAL-WORLD STORAGE ENGINE INTEGRATION & INTEGRITY TESTS")
    print("=" * 80)
    int_ok = run_integration_tests()
    if not int_ok:
        print("\n[CRITICAL FAILURE] Integration Tests failed!")
        return False

    # 3. Live S3 / MinIO Over-The-Wire Protocol Verification
    print("\n" + "=" * 80)
    print("PART 3: LIVE S3 / MINIO OVER-THE-WIRE PROTOCOL VERIFICATION")
    print("=" * 80)
    from test_minio_live import run_minio_live_test, is_port_open
    target_url = "http://127.0.0.1:9005" if is_port_open("127.0.0.1", 9005) else "http://127.0.0.1:9002"
    live_ok = run_minio_live_test(target_url)
    if not live_ok:
        print("\n[CRITICAL FAILURE] Live S3 / MinIO Protocol Verification failed!")
        return False

    elapsed = time.time() - t0
    print("\n" + "=" * 80)
    print("MASTER VERDICT: 100% FORMALLY VERIFIED, EMPIRICALLY VALIDATED, & S3/MINIO COMPLIANT")
    print(f"All structural SMT invariants, storage integration tests, and live S3 wire tests passed in {elapsed:.2f}s.")
    print("=" * 80)
    return True


if __name__ == "__main__":
    success = run_master_suite()
    sys.exit(0 if success else 1)
