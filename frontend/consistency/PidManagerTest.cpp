//
// Created by YuvalFreund on 15.03.24.
//
// -------------------------------------------------------------------------------------
// Consistency Check for ScaleStore
// 1. partition keys and build the tree multi threaded
// 2. partition keys and update keys by 1 for every access
// 3. shuffle keys and iterate once randomly over all keys and add thread id
// 4. run consistency checks
// -------------------------------------------------------------------------------------
#include "PerfEvent.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/storage/datastructures/BTree.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/ScrambledZipfGenerator.hpp"
#include "scalestore/utils/Time.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

// -------------------------------------------------------------------------------------
DEFINE_uint32(YCSB_read_ratio, 100, "");
DEFINE_bool(YCSB_all_workloads, false , "Execute all workloads i.e. 50 95 100 ReadRatio on same tree");
DEFINE_double(YCSB_trigger_leave_percentage, 1.0, "");
DEFINE_uint64(YCSB_tuple_count, 1, " Tuple count in");
DEFINE_double(YCSB_zipf_factor, 0.0, "Default value according to spec");
DEFINE_double(YCSB_run_for_seconds, 10.0, "");
DEFINE_bool(YCSB_partitioned, false, "");
DEFINE_bool(YCSB_warm_up, false, "");
DEFINE_bool(YCSB_record_latency, false, "");
DEFINE_bool(YCSB_all_zipf, false, "");
DEFINE_bool(YCSB_local_zipf, false, "");
DEFINE_bool(YCSB_flush_pages, false, "");
DEFINE_uint32(YCSB_shuffle_ratio, 20, "");
// -------------------------------------------------------------------------------------
static constexpr uint64_t BTREE_ID = 0;
static constexpr uint64_t BARRIER_ID = 1;
// -------------------------------------------------------------------------------------
struct Partition {
    uint64_t begin;
    uint64_t end;
};
struct Consistency_workloadInfo : public scalestore::profiling::WorkloadInfo {
    std::string experiment;
    uint64_t elements;

    Consistency_workloadInfo(std::string experiment, uint64_t elements) : experiment(experiment), elements(elements) {}

    virtual std::vector<std::string> getRow() { return {experiment, std::to_string(elements)}; }

    virtual std::vector<std::string> getHeader() { return {"workload", "elements"}; }

    virtual void csv(std::ofstream& file) override {
        file << experiment << " , ";
        file << elements << " , ";
    }

    virtual void csvHeader(std::ofstream& file) override {
        file << "Workload"
             << " , ";
        file << "Elements"
             << " , ";
    }
};
class Barrier {
private:
    const std::size_t threadCount;
    alignas(64) std::atomic<std::size_t> cntr;
    alignas(64) std::atomic<uint8_t> round;

public:
    explicit Barrier(std::size_t threadCount) : threadCount(threadCount), cntr(threadCount), round(0) {}

    template <typename F>
    bool wait(F finalizer) {
        auto prevRound = round.load();  // Must happen before fetch_sub
        auto prev = cntr.fetch_sub(1);
        if (prev == 1) {
            // last thread arrived
            cntr = threadCount;
            auto r = finalizer();
            round++;
            return r;
        } else {
            while (round == prevRound) {
                // wait until barrier is ready for re-use
                asm("pause");
                asm("pause");
                asm("pause");
            }
            return false;
        }
    }
    inline bool wait() {
        return wait([]() { return true; });
    }
};

// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[]) {
    using K = uint64_t;
    using V = uint64_t;
    gflags::SetUsageMessage("Catalog Test");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // -------------------------------------------------------------------------------------
    ScaleStore scalestore;
    auto& catalog = scalestore.getCatalog();

    uint64_t numberTuples = FLAGS_YCSB_tuple_count / FLAGS_nodes;
    uint64_t shuffleRatio = 0;
    if(FLAGS_YCSB_shuffle_ratio){
        shuffleRatio = FLAGS_YCSB_shuffle_ratio;
    }

    // -------------------------------------------------------------------------------------
    // LAMBDAS to reduce boilerplate code
    Barrier local_barrier(FLAGS_worker);
    auto execute_on_tree = [&](std::function<void(uint64_t t_i, storage::BTree<K, V> & tree)> callback) {
        for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
            scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
                // -------------------------------------------------------------------------------------
                storage::BTree<K, V> tree(catalog.getCatalogEntry(BTREE_ID).pid);
                // -------------------------------------------------------------------------------------
                storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
                barrier.wait();
                // -------------------------------------------------------------------------------------
                callback(t_i, tree);
                local_barrier.wait();
            });
        }
        scalestore.getWorkerPool().joinAll();
    };

    // -------------------------------------------------------------------------------------
    // create Btree (0), Barrier(1)
    // -------------------------------------------------------------------------------------
    if (scalestore.getNodeID() == 0) {
        scalestore.getWorkerPool().scheduleJobSync(0, [&]() {
            scalestore.createBTree<K, V>();
            scalestore.createBarrier(FLAGS_worker * FLAGS_nodes);
        });
    }

    // -------------------------------------------------------------------------------------
    // CONSISTENCY CHECKS
    // -------------------------------------------------------------------------------------
    // 1. build tree mutlithreaded
    {
        Consistency_workloadInfo builtInfo{"Build B-Tree", numberTuples};
        scalestore.startProfiler(builtInfo);
        // -------------------------------------------------------------------------------------
        execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
            uint64_t start = (scalestore.getNodeID() * FLAGS_worker) + t_i;
            const uint64_t inc = FLAGS_worker * FLAGS_nodes;
            for (; start < numberTuples; start = start + inc) {
                tree.insert(start, 1);
                V value = 99;
                auto found = tree.lookup(start, value);
                ensure(found);
                ensure(value == 1);
                threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
        });
        scalestore.stopProfiler();
    }

    {
        // -------------------------------------------------------------------------------------
        // consistency check for build
        Consistency_workloadInfo builtInfo{"Build Verify", numberTuples};
        scalestore.startProfiler(builtInfo);
        std::atomic<uint64_t> global_result{0};
        execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
            uint64_t start = t_i;  // scans all pages
            const uint64_t inc = FLAGS_worker;
            uint64_t local_result = 0;
            for (; start < numberTuples; start = start + inc) {
                V value = 99;
                auto found = tree.lookup(start, value);
                local_result += value;
                ensure(found);
                ensure(value == 1);
                threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
            global_result += local_result;
        });
        scalestore.stopProfiler();
        ensure(global_result == numberTuples);
    }
    // -------------------------------------------------------------------------------------
    // 2. MT increase own keys by 1 for multiple rounds
    {
        Consistency_workloadInfo builtInfo{"Trigger and increment B-Tree", numberTuples};
        scalestore.startProfiler(builtInfo);
        std::atomic<bool> finishedShuffling =false;
        // -------------------------------------------------------------------------------------
        execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
            rdma::MessageHandler& mh = scalestore.getMessageHandler();
            PageIdManager& pageIdManager = scalestore.getPageIdManager();
            uint64_t leavingNodeId = 0;
            threads::Worker* workerPtr = scalestore.getWorkerPool().getWorkerByTid(t_i);
            if(t_i == 0 && scalestore.getNodeID() == leavingNodeId){
                std::cout<<"begin trigger" <<std::endl;
                pageIdManager.gossipNodeIsLeaving(workerPtr);
                std::cout<<"done trigger" <<std::endl;
                pageIdManager.isBeforeShuffle = false;
            }

            uint64_t start = (scalestore.getNodeID() * FLAGS_worker) + t_i;
            const uint64_t inc = FLAGS_worker * FLAGS_nodes;
            for (; start < numberTuples; start = start + inc) {
                if(scalestore.getNodeID() == leavingNodeId && pageIdManager.isBeforeShuffle == false && utils::RandomGenerator::getRandU64(0, 100) < shuffleRatio ){//&& (t_i == 0 ||t_i==1 ) ) { // worker will go and shuffle
                    finishedShuffling = mh.shuffleFrameAndIsLastShuffle(workerPtr);
                }
                auto updated = tree.lookupAndUpdate(start, [](V& value) { value++; });
                ensure(updated);
                threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
        });
        scalestore.stopProfiler();
    }
    {
        Consistency_workloadInfo builtInfo{"Verify increment", numberTuples};
        scalestore.startProfiler(builtInfo);
        // -------------------------------------------------------------------------------------
        // consistency check for updates
        execute_on_tree([&](uint64_t t_i, storage::BTree<K, V>& tree) {
            uint64_t start = t_i;
            const uint64_t inc = FLAGS_worker;
            for (; start < numberTuples; start = start + inc) {
                V value = 99;
                auto found = tree.lookup(start, value);
                ensure(found);
                ensure(value == 2);
                threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
            }
        });
        scalestore.stopProfiler();
    }
    std::cout<< "Finished page id manager test"<< std::endl;
    // -------------------------------------------------------------------------------------
    return 0;
}
