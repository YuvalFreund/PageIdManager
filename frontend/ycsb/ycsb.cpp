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
#include <chrono>

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
DEFINE_uint32(YCSB_shuffle_ratio, 100, "");
// -------------------------------------------------------------------------------------
using u64 = uint64_t;
using u8 = uint8_t;
// -------------------------------------------------------------------------------------
static constexpr uint64_t BTREE_ID = 0;
static constexpr uint64_t BARRIER_ID = 1;
// -------------------------------------------------------------------------------------
template <u64 size>
struct BytesPayload {
   u8 value[size];
   BytesPayload() = default;
   bool operator==(BytesPayload& other) { return (std::memcmp(value, other.value, sizeof(value)) == 0); }
   bool operator!=(BytesPayload& other) { return !(operator==(other)); }
   // BytesPayload(const BytesPayload& other) { std::memcpy(value, other.value, sizeof(value)); }
   // BytesPayload& operator=(const BytesPayload& other)
   // {
      // std::memcpy(value, other.value, sizeof(value));
      // return *this;
   // }
};
// -------------------------------------------------------------------------------------
struct Partition {
   uint64_t begin;
   uint64_t end;
};
// -------------------------------------------------------------------------------------
struct YCSB_workloadInfo : public scalestore::profiling::WorkloadInfo {
   std::string experiment;
   uint64_t elements;
   uint64_t readRatio;
   double zipfFactor;
   std::string zipfOffset;
   uint64_t timestamp = 0;

   YCSB_workloadInfo(std::string experiment, uint64_t elements, uint64_t readRatio, double zipfFactor, std::string zipfOffset)
      : experiment(experiment), elements(elements), readRatio(readRatio), zipfFactor(zipfFactor), zipfOffset(zipfOffset)
   {
   }

   
   virtual std::vector<std::string> getRow(){
      return {
          experiment, std::to_string(elements),    std::to_string(readRatio), std::to_string(zipfFactor),
          zipfOffset, std::to_string(timestamp++),
      };
   }

   virtual std::vector<std::string> getHeader(){
      return {"workload","elements","read ratio", "zipfFactor", "zipfOffset", "timestamp"};
   }
   

   virtual void csv(std::ofstream& file) override
   {
      file << experiment << " , ";
      file << elements << " , ";
      file << readRatio << " , ";
      file << zipfFactor << " , ";
      file << zipfOffset << " , ";
      file << timestamp << " , ";
   }
   virtual void csvHeader(std::ofstream& file) override
   {
      file << "Workload"
           << " , ";
      file << "Elements"
           << " , ";
      file << "ReadRatio"
           << " , ";
      file << "ZipfFactor"
           << " , ";
      file << "ZipfOffset"
           << " , ";
      file << "Timestamp"
           << " , ";
   }
};
// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char* argv[])
{
   using K = uint64_t;
   using V = BytesPayload<128>;

   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   // prepare workload
   std::vector<std::string> workload_type; // warm up or benchmark
   std::vector<uint32_t> workloads;
   std::vector<double> zipfs;
   if(FLAGS_YCSB_all_workloads){
      workloads.push_back(5); 
      workloads.push_back(50); 
      workloads.push_back(95);
      workloads.push_back(100);
   }else{
      workloads.push_back(FLAGS_YCSB_read_ratio);
   }

   
   if(FLAGS_YCSB_warm_up){
      workload_type.push_back("YCSB_warm_up");
      workload_type.push_back("YCSB_txn");
   }else{
      workload_type.push_back("YCSB_txn");
   }

   if(FLAGS_YCSB_all_zipf){
      // zipfs.insert(zipfs.end(), {0.0,1.0,1.25,1.5,1.75,2.0});
      // zipfs.insert(zipfs.end(), {1.05,1.1,1.15,1.20,1.3,1.35,1.4,1.45});
      zipfs.insert(zipfs.end(), {0,0.25,0.5,0.75,1.0,1.25,1.5,1.75,2.0});
   }else{
      zipfs.push_back(FLAGS_YCSB_zipf_factor);
   }
   // -------------------------------------------------------------------------------------
   ScaleStore scalestore;
   auto& catalog = scalestore.getCatalog();
   // -------------------------------------------------------------------------------------
   auto partition = [&](uint64_t id, uint64_t participants, uint64_t N) -> Partition {
      const uint64_t blockSize = N / participants;
      auto begin = id * blockSize;
      auto end = begin + blockSize;
      if (id == participants - 1)
         end = N;
      return {.begin = begin, .end = end};
   };

   auto barrier_wait = [&]() {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait();
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
   u64 YCSB_tuple_count = FLAGS_YCSB_tuple_count;
   // -------------------------------------------------------------------------------------
    uint64_t shuffleRatio = 100;
    if(FLAGS_YCSB_shuffle_ratio){
        shuffleRatio = FLAGS_YCSB_shuffle_ratio;
    }

    uint64_t nodeLeavingTrigger = 100000;


    auto nodePartition = partition(scalestore.getNodeID(), FLAGS_nodes, YCSB_tuple_count);
   // -------------------------------------------------------------------------------------
   // Build YCSB Table / Tree
   // -------------------------------------------------------------------------------------
   YCSB_workloadInfo builtInfo{"Build", YCSB_tuple_count, FLAGS_YCSB_read_ratio, FLAGS_YCSB_zipf_factor, (FLAGS_YCSB_local_zipf?"local_zipf":"global_zipf")};
   scalestore.startProfiler(builtInfo);
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
      scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
         // -------------------------------------------------------------------------------------
         // partition
         auto nodeKeys = nodePartition.end - nodePartition.begin;
         auto threadPartition = partition(t_i, FLAGS_worker, nodeKeys);
         auto begin = nodePartition.begin + threadPartition.begin;
         auto end = nodePartition.begin + threadPartition.end;
         storage::BTree<K, V> tree(catalog.getCatalogEntry(BTREE_ID).pid);
         // -------------------------------------------------------------------------------------
         storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
         barrier.wait();
         // -------------------------------------------------------------------------------------
         V value;
         for (K k_i = begin; k_i < end; k_i++) {
            utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&value), sizeof(V));
            tree.insert(k_i, value);
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
         // -------------------------------------------------------------------------------------
         barrier.wait();
      });
   }
   scalestore.getWorkerPool().joinAll();
   scalestore.stopProfiler();

   if(FLAGS_YCSB_flush_pages){
      std::cout << "Flushing all pages"
                << "\n";
      scalestore.getBuffermanager().writeAllPages();
      
      std::cout << "Done"
                << "\n";
   }
         
   for (auto ZIPF : zipfs) {
      // -------------------------------------------------------------------------------------
      // YCSB Transaction
      // -------------------------------------------------------------------------------------
      std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
      if (FLAGS_YCSB_partitioned)
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(nodePartition.begin, nodePartition.end - 1,
                                                                       ZIPF);
      else
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, YCSB_tuple_count, ZIPF);

      // -------------------------------------------------------------------------------------
      // zipf creation can take some time due to floating point loop therefore wait with barrier
      barrier_wait();
      // -------------------------------------------------------------------------------------
      for (auto READ_RATIO : workloads) {
         for (auto TYPE : workload_type) {
            barrier_wait();
            std::atomic<bool> keep_running = true;
            std::atomic<bool> finishedShuffling =false;
            std::atomic<u64> running_threads_counter = 0;
            std::atomic<uint64_t> threadsFinishedShuffle = 0;
            uint64_t zipf_offset = 0;
            if (FLAGS_YCSB_local_zipf) zipf_offset = (YCSB_tuple_count / FLAGS_nodes) * scalestore.getNodeID();
            uint64_t leavingNodeId = 0;
            YCSB_workloadInfo experimentInfo{TYPE, YCSB_tuple_count, READ_RATIO, ZIPF, (FLAGS_YCSB_local_zipf?"local_zipf":"global_zipf")};
            scalestore.startProfiler(experimentInfo);
            rdma::MessageHandler& mh = scalestore.getMessageHandler();
            std::chrono::steady_clock::time_point beginOfShuffling;
             for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i) {
                scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]() {
                   threads::Worker* workerPtr = scalestore.getWorkerPool().getWorkerByTid(t_i);
                   running_threads_counter++;
                   storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
                   storage::BTree<K, V> tree(catalog.getCatalogEntry(BTREE_ID).pid);
                   barrier.wait();
                   uint64_t checkToStartShuffle = 0;
                   PageIdManager& pageIdManager = scalestore.getPageIdManager();
                   while (keep_running) {
                       if(scalestore.getNodeID() == leavingNodeId && t_i == 0) {
                           checkToStartShuffle++;
                           if(checkToStartShuffle == nodeLeavingTrigger){
                               std::cout<<"begin trigger" <<std::endl;
                               beginOfShuffling = std::chrono::steady_clock::now();
                               pageIdManager.broadcastNodeIsLeaving(workerPtr);
                               std::cout<<"done trigger" <<std::endl;
                               pageIdManager.shuffleState = SHUFFLE_STATE::DURING_SHUFFLE;
                           }
                       }

                       if (scalestore.getNodeID() == leavingNodeId && pageIdManager.shuffleState == SHUFFLE_STATE::DURING_SHUFFLE && utils::RandomGenerator::getRandU64(0, 100) < shuffleRatio){
                           bool finished = mh.shuffleFrameAndIsLastShuffle(workerPtr,t_i,FLAGS_worker);
                           if (finished){
                               if(t_i != 0) {
                                   threadsFinishedShuffle++;
                                   break;
                               }else{
                                   if(threadsFinishedShuffle == FLAGS_worker - 1){
                                       std::chrono::steady_clock::time_point finishShuffling = std::chrono::steady_clock::now();
                                       std::cout<<"Done shuffling! shuffle percentage :" << shuffleRatio<< " shuffle time: "<< std::chrono::duration_cast<std::chrono::microseconds>(finishShuffling - beginOfShuffling).count()  <<std::endl;
                                       std::cout<<"Pages added: " << pageIdManager.pagesAdded << " pages shuffled:  "<< pageIdManager.pagesShuffled << " diff: " << pageIdManager.pagesAdded  - pageIdManager.pagesShuffled<<std::endl;

                                       mh.printShuffleLatency(FLAGS_worker,0); //todo yuvi- clean
                                       pageIdManager.broadcastNodeFinishedShuffling(workerPtr);
                                       scalestore.getPageProvider().forceEvictionAfterShuffle();
                                       break;
                                   }
                               }
                           }
                       } else {
                           K key = zipf_random->rand(zipf_offset);
                           ensure(key < YCSB_tuple_count);
                           V result;
                           if (READ_RATIO == 100 || utils::RandomGenerator::getRandU64(0, 100) < READ_RATIO) {
                               auto start = utils::getTimePoint();
                               auto success = tree.lookup_opt(key, result);
                               ensure(success);
                               auto end = utils::getTimePoint();
                               threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                           } else {
                               V payload;
                               utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(V));
                               auto start = utils::getTimePoint();
                               tree.insert(key, payload);
                               auto end = utils::getTimePoint();
                               threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                           }
                           threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                       }
                   }
                   running_threads_counter--;
                });
            }
            // -------------------------------------------------------------------------------------
            // Join Threads
            // -------------------------------------------------------------------------------------
            sleep(FLAGS_YCSB_run_for_seconds);
            keep_running = false;
            while (running_threads_counter) {
               _mm_pause();
            }
            scalestore.getWorkerPool().joinAll();
            // -------------------------------------------------------------------------------------
            scalestore.stopProfiler();

         }
      }
   }
   std::cout << "Starting hash table report " << "\n";
   scalestore.getBuffermanager().reportHashTableStats();
   return 0;
}


