#include "ScaleStore.hpp"
// -------------------------------------------------------------------------------------
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
#include <fcntl.h>
// -------------------------------------------------------------------------------------
namespace scalestore {
ScaleStore::ScaleStore(){
   // -------------------------------------------------------------------------------------
   // find node id
   if(FLAGS_nodes != 1){
      for(; nodeId < FLAGS_nodes; nodeId++){
         if( FLAGS_ownIp == NODES[FLAGS_nodes][nodeId])
            break;
      }
   }else{
      nodeId = 0; // fix to allow single node use on all nodes 
   }
   
   ensure(nodeId < FLAGS_nodes);
   // ------------------------------------------------------------------------------------
   // open SSD
   int flags = O_RDWR | O_DIRECT;   
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   posix_check(ssd_fd > -1);
   if (FLAGS_falloc > 0) {
      const u64 gib_size = 1024ull * 1024ull * 1024ull;
      auto dummy_data = (uint8_t*)aligned_alloc(512, gib_size);
      for (u64 i = 0; i < FLAGS_falloc; i++) {
         const int ret = pwrite(ssd_fd, dummy_data, gib_size, gib_size * i);
         posix_check(ret == gib_size);
      }
      free(dummy_data);
      fsync(ssd_fd);
   }
   ensure(fcntl(ssd_fd, F_GETFL) != -1);
   std::cout<<"node id: "<<nodeId <<std::endl;
   std::cout<<"Largest Message "<<scalestore::rdma::LARGEST_MESSAGE<<std::endl;

    // -------------------------------------------------------------------------------------
   // order of construction is important
    std::vector<uint64_t> nodesInCluster =  getNodeIdsVec(FLAGS_nodes);
    pageIdManager = std::make_unique<PageIdManager>(nodeId,nodesInCluster);
    cm = std::make_unique<rdma::CM<rdma::InitMessage>>();
    bm = std::make_unique<storage::Buffermanager>(*cm, nodeId, ssd_fd,*pageIdManager);
    storage::BM::global = bm.get();
    mh = std::make_unique<rdma::MessageHandler>(*cm, *bm, nodeId,*pageIdManager);
    workerPool = std::make_unique<threads::WorkerPool>(*cm, nodeId);
    pp = std::make_unique<storage::PageProvider>(*cm, *bm, mh->mbPartitions, ssd_fd,*pageIdManager);
    rGuard =std::make_unique<RemoteGuard>(mh->connectedClients);
    bmCounters = std::make_unique<profiling::BMCounters>(*bm);
    rdmaCounters = std::make_unique<profiling::RDMACounters>();
    catalog = std::make_unique<storage::Catalog>();
    // init catalog
    workerPool->scheduleJobSync(
      0, [&]() { catalog->init(nodeId); });
}


ScaleStore::~ScaleStore(){
   stopProfiler();
   workerPool.reset(); // important clients need to disconnect first
}
}  // scalestore
