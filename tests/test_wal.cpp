// test_wal.cpp — exercises the durability guarantees the whole
// consensus layer depends on: entries survive a simulated crash
// (destroy and reconstruct the Wal object, which is what happens on
// process restart), truncate-on-conflict works, and snapshot+compact
// drops exactly the entries it should while keeping the rest replayable.
//
// No test framework dependency — plain asserts with a PASS/FAIL
// summary, run via `make test`.

#include "querykv/wal.hpp"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>

using namespace qkv;

static int g_failures = 0;

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "  FAIL: " << #cond << " (line " << __LINE__ << ")\n"; \
            g_failures++; \
        } \
    } while (0)

static std::string tmp_dir(const std::string& suffix) {
    std::string dir = "/tmp/qkv_test_" + suffix;
    int rc = system(("rm -rf " + dir).c_str());
    (void)rc;
    return dir;
}

static LogEntry make_entry(uint64_t term, uint64_t index, CmdType cmd,
                            const std::string& k, const std::string& v) {
    LogEntry e; e.term = term; e.index = index; e.cmd = cmd; e.key = k; e.value = v;
    return e;
}

void test_append_and_replay() {
    std::cout << "test_append_and_replay\n";
    std::string dir = tmp_dir("append");
    {
        Wal wal(dir);
        wal.append(make_entry(1, 1, CmdType::PUT, "a", "1"));
        wal.append(make_entry(1, 2, CmdType::PUT, "b", "2"));
        wal.append(make_entry(2, 3, CmdType::DEL, "a", ""));
        CHECK(wal.entries().size() == 3);
        CHECK(wal.last_index() == 3);
        CHECK(wal.last_term() == 2);
    }
    // Simulate a crash + restart: reconstruct from the same directory.
    {
        Wal wal2(dir);
        CHECK(wal2.entries().size() == 3);
        CHECK(wal2.entries()[0].key == "a");
        CHECK(wal2.entries()[1].key == "b");
        CHECK(wal2.entries()[2].cmd == CmdType::DEL);
        CHECK(wal2.last_index() == 3);
    }
}

void test_term_vote_persistence() {
    std::cout << "test_term_vote_persistence\n";
    std::string dir = tmp_dir("meta");
    {
        Wal wal(dir);
        CHECK(wal.current_term() == 0);
        CHECK(wal.voted_for() == -1);
        wal.save_term_and_vote(5, 2);
    }
    {
        Wal wal2(dir);
        CHECK(wal2.current_term() == 5);
        CHECK(wal2.voted_for() == 2);
    }
}

void test_truncate_from() {
    std::cout << "test_truncate_from\n";
    std::string dir = tmp_dir("truncate");
    Wal wal(dir);
    wal.append(make_entry(1, 1, CmdType::PUT, "a", "1"));
    wal.append(make_entry(1, 2, CmdType::PUT, "b", "2"));
    wal.append(make_entry(1, 3, CmdType::PUT, "c", "3"));
    CHECK(wal.entries().size() == 3);

    // A new leader's AppendEntries conflicts starting at index 2 —
    // everything from index 2 onward must be dropped.
    wal.truncate_from(2);
    CHECK(wal.entries().size() == 1);
    CHECK(wal.last_index() == 1);

    wal.append(make_entry(2, 2, CmdType::PUT, "b2", "20"));
    CHECK(wal.entries().size() == 2);
    CHECK(wal.entries()[1].term == 2);

    // Truncation must also survive a restart (it rewrites wal.log).
    Wal wal2(dir);
    CHECK(wal2.entries().size() == 2);
    CHECK(wal2.entries()[1].key == "b2");
}

void test_snapshot_compaction() {
    std::cout << "test_snapshot_compaction\n";
    std::string dir = tmp_dir("snapshot");
    Wal wal(dir);
    for (uint64_t i = 1; i <= 10; i++) {
        wal.append(make_entry(1, i, CmdType::PUT, "k" + std::to_string(i), "v"));
    }
    CHECK(wal.entries().size() == 10);
    CHECK(!wal.has_snapshot());

    std::vector<uint8_t> fake_kv_blob = {1, 2, 3, 4};
    wal.write_snapshot(7, 1, fake_kv_blob);

    CHECK(wal.has_snapshot());
    CHECK(wal.snapshot_meta().last_included_index == 7);
    CHECK(wal.entries().size() == 3); // indices 8, 9, 10 remain
    CHECK(wal.first_index() == 8);
    CHECK(wal.last_index() == 10);
    CHECK(wal.read_snapshot_blob() == fake_kv_blob);

    // Compaction must survive a restart too.
    Wal wal2(dir);
    CHECK(wal2.snapshot_meta().last_included_index == 7);
    CHECK(wal2.entries().size() == 3);
    CHECK(wal2.entries()[0].index == 8);
    CHECK(wal2.read_snapshot_blob() == fake_kv_blob);
}

void test_partial_write_recovery() {
    std::cout << "test_partial_write_recovery (simulated torn write)\n";
    std::string dir = tmp_dir("torn");
    {
        Wal wal(dir);
        wal.append(make_entry(1, 1, CmdType::PUT, "a", "1"));
        wal.append(make_entry(1, 2, CmdType::PUT, "b", "2"));
    }
    // Simulate a crash mid-append by truncating the last few bytes of
    // wal.log off the end of the file (a torn write from a real crash
    // looks the same: a complete prefix of records, one incomplete tail).
    std::string path = dir + "/wal.log";
    FILE* f = fopen(path.c_str(), "r+b");
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fclose(f);
    int rc = truncate(path.c_str(), size - 3); // chop 3 bytes off the trailing record
    (void)rc;

    Wal wal2(dir);
    // The first (complete) entry must still be there; the torn second
    // entry must be dropped rather than crashing or corrupting replay.
    CHECK(wal2.entries().size() == 1);
    CHECK(wal2.entries()[0].key == "a");
}

int main() {
    test_append_and_replay();
    test_term_vote_persistence();
    test_truncate_from();
    test_snapshot_compaction();
    test_partial_write_recovery();

    if (g_failures == 0) {
        std::cout << "ALL WAL TESTS PASSED\n";
        return 0;
    }
    std::cout << g_failures << " CHECK(S) FAILED\n";
    return 1;
}
