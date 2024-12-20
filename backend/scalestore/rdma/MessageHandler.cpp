#include "MessageHandler.hpp"
#include "Defs.hpp"
#include "scalestore/storage/buffermanager/Page.hpp"
#include "scalestore/threads/CoreManager.hpp"
#include "scalestore/threads/ThreadContext.hpp"
// -------------------------------------------------------------------------------------
#include <numeric>

namespace scalestore {
namespace rdma {
MessageHandler::MessageHandler(rdma::CM<InitMessage>& cm, storage::Buffermanager& bm, NodeID nodeId, PageIdManager& pageIdManager)
    : cm(cm), bm(bm), nodeId(nodeId), mbPartitions(FLAGS_messageHandlerThreads),pageIdManager(pageIdManager) {
   // partition mailboxes
   size_t n = (FLAGS_worker) * (FLAGS_nodes - 1);
   if (n > 0) {
      ensure(FLAGS_messageHandlerThreads <= n);  // avoid over subscribing message handler threads
      const uint64_t blockSize = n / FLAGS_messageHandlerThreads;
      ensure(blockSize > 0);
      for (uint64_t t_i = 0; t_i < FLAGS_messageHandlerThreads; t_i++) {
         auto begin = t_i * blockSize;
         auto end = begin + blockSize;
         if (t_i == FLAGS_messageHandlerThreads - 1) end = n;

         // parititon mailboxes
         uint8_t* partition = (uint8_t*)cm.getGlobalBuffer().allocate(end - begin, CACHE_LINE);  // CL aligned
         ensure(((uintptr_t)partition) % CACHE_LINE == 0);
         // cannot use emplace because of mutex
         mbPartitions[t_i].mailboxes = partition;
         mbPartitions[t_i].numberMailboxes = end - begin;
         mbPartitions[t_i].beginId = begin;
         mbPartitions[t_i].inflightCRs.resize(end - begin);
      }
      startThread();
   };
}
// -------------------------------------------------------------------------------------
void MessageHandler::init() {
   InitMessage* initServer = (InitMessage*)cm.getGlobalBuffer().allocate(sizeof(InitMessage));
   // -------------------------------------------------------------------------------------
   size_t numConnections = (FLAGS_worker) * (FLAGS_nodes - 1);
   connectedClients = numConnections;
   while (cm.getNumberIncomingConnections() != (numConnections))
      ;  // block until client is connected
   // -------------------------------------------------------------------------------------
   std::cout << "Number connections " << numConnections << std::endl;
   // wait until all workers are connected
   std::vector<RdmaContext*> rdmaCtxs;  // get cm ids of incomming

   while (true) {
      std::vector<RdmaContext*> tmp_rdmaCtxs(cm.getIncomingConnections());  // get cm ids of incomming
      uint64_t workers = 0;
      for (auto* rContext : tmp_rdmaCtxs) {
         if (rContext->type != Type::WORKER) continue;
         workers++;
      }
      if (workers == numConnections) {
         rdmaCtxs = tmp_rdmaCtxs;
         break;
      }
   }
   // -------------------------------------------------------------------------------------
   // shuffle worker connections
   // -------------------------------------------------------------------------------------
   auto rng = std::default_random_engine{};
   std::shuffle(std::begin(rdmaCtxs), std::end(rdmaCtxs), rng);

   uint64_t counter = 0;
   uint64_t partitionId = 0;
   uint64_t partitionOffset = 0;

   for (auto* rContext : rdmaCtxs) {
      // -------------------------------------------------------------------------------------
      if (rContext->type != Type::WORKER) {
         continue;  // skip no worker connection
      }
      
      // partially initiallize connection connectxt
      ConnectionContext cctx;
      cctx.request = (Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctx.response = (Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctx.rctx = rContext;
      cctx.activeInvalidationBatch = new InvalidationBatch();
      cctx.passiveInvalidationBatch = new InvalidationBatch();
      // -------------------------------------------------------------------------------------
      // find correct mailbox in partitions
      if ((counter >= (mbPartitions[partitionId].beginId + mbPartitions[partitionId].numberMailboxes))) {
         partitionId++;
         partitionOffset = 0;
      }
      auto& mbPartition = mbPartitions[partitionId];
      ensure(mbPartition.beginId + partitionOffset == counter);
      // -------------------------------------------------------------------------------------
      // fill init message
      initServer->mbOffset = (uintptr_t)&mbPartition.mailboxes[partitionOffset];
      initServer->plOffset = (uintptr_t)cctx.request;
      initServer->bmId = nodeId;
      initServer->type = rdma::MESSAGE_TYPE::Init;
      // -------------------------------------------------------------------------------------
      cm.exchangeInitialMesssage(*(cctx.rctx), initServer);
      // -------------------------------------------------------------------------------------
      // finish initialization of cctx
      cctx.plOffset = (reinterpret_cast<InitMessage*>((cctx.rctx->applicationData)))->plOffset;
      cctx.bmId = (reinterpret_cast<InitMessage*>((cctx.rctx->applicationData)))->bmId;
      // -------------------------------------------------------------------------------------
      cctx.remoteMbOffsets.resize(FLAGS_nodes);
      cctx.remotePlOffsets.resize(FLAGS_nodes);
      // -------------------------------------------------------------------------------------
      cctxs.push_back(cctx);
      // -------------------------------------------------------------------------------------
      // check if ctx is needed as endpoint
      // increment running counter
      counter++;
      partitionOffset++;
   }

   ensure(counter == numConnections);
   // -------------------------------------------------------------------------------------
};
// -------------------------------------------------------------------------------------
MessageHandler::~MessageHandler() {
   stopThread();
}
// -------------------------------------------------------------------------------------
void MessageHandler::startThread() {
   for (uint64_t t_i = 0; t_i < FLAGS_messageHandlerThreads; t_i++) {
      std::thread t([&, t_i]() {
         // -------------------------------------------------------------------------------------
         std::unique_ptr<threads::ThreadContext> threadContext = std::make_unique<threads::ThreadContext>();
         threads::ThreadContext::tlsPtr = threadContext.get();  // init tl ptr
         // ------------------------------------------------------------------------------------- 
         threadCount++;
         // protect init only ont thread should do it;
         if (t_i == 0) {
            init();
            finishedInit = true;
         } else {
            while (!finishedInit)
               ;  // block until initialized
         }
         MailboxPartition& mbPartition = mbPartitions[t_i];
         uint8_t* mailboxes = mbPartition.mailboxes;
         const uint64_t beginId = mbPartition.beginId;
         uint64_t startPosition = 0;  // randomize messages
         uint64_t mailboxIdx = 0;
         profiling::WorkerCounters counters;  // create counters
         storage::AsyncReadBuffer async_read_buffer(bm.ssd_fd, PAGE_SIZE, 256);
         // for delegation purpose, i.e., communication to remote message handler
         std::vector<MHEndpoint> mhEndpoints(FLAGS_nodes);
         for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
            if (n_i == nodeId) continue;
            auto& ip = NODES[FLAGS_nodes][n_i];
            mhEndpoints[n_i].rctx = &(cm.initiateConnection(ip, rdma::Type::MESSAGE_HANDLER, 99, nodeId));
         }
         // handle

         std::vector<uint64_t> latencies(mbPartition.numberMailboxes);

         while (threadsRunning || connectedClients.load()) {
             for (uint64_t m_i = 0; m_i < mbPartition.numberMailboxes; m_i++, mailboxIdx++) {
                 // -------------------------------------------------------------------------------------
                 if (mailboxIdx >= mbPartition.numberMailboxes) mailboxIdx = 0;

                 if (mailboxes[mailboxIdx] == 0) continue;
                 // -------------------------------------------------------------------------------------
                 mailboxes[mailboxIdx] = 0;  // reset mailbox before response is sent
                 // -------------------------------------------------------------------------------------
                 // handle message
                 uint64_t clientId = mailboxIdx + beginId;  // correct for partiton
                 auto &ctx = cctxs[clientId];
                 // reset infligh copy requests if needed
                 if (mbPartition.inflightCRs[m_i].inflight) {  // only we modify this entry, however there could be readers
                     std::unique_lock <std::mutex> ulquard(mbPartition.inflightCRMutex);
                     mbPartition.inflightCRs[m_i].inflight = false;
                     mbPartition.inflightCRs[m_i].pid = EMPTY_PID;
                     mbPartition.inflightCRs[m_i].pVersion = 0;
                 }

                 switch (ctx.request->type) {
                     case MESSAGE_TYPE::Finish: {
                         connectedClients--;
                         break;
                     }
                     case MESSAGE_TYPE::DR: {
                         auto &request = *reinterpret_cast<DelegationRequest *>(ctx.request);
                         ctx.remoteMbOffsets[request.bmId] = request.mbOffset;
                         ctx.remotePlOffsets[request.bmId] = request.mbPayload;
                         auto &response = *MessageFabric::createMessage<DelegationResponse>(ctx.response);
                         writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                         break;
                     }
                     case MESSAGE_TYPE::PRX: {
                         auto &request = *reinterpret_cast<PossessionRequest *>(ctx.request);
                         handlePossessionRequest<POSSESSION::EXCLUSIVE>(mbPartition, request, ctx, clientId, mailboxIdx,
                                                                        counters,
                                                                        async_read_buffer,
                                                                        threads::ThreadContext::my().page_handle);
                         break;
                     }
                     case MESSAGE_TYPE::PRS: {
                         auto &request = *reinterpret_cast<PossessionRequest *>(ctx.request);
                         handlePossessionRequest<POSSESSION::SHARED>(mbPartition, request, ctx, clientId, mailboxIdx,
                                                                     counters,
                                                                     async_read_buffer,
                                                                     threads::ThreadContext::my().page_handle);
                         break;
                     }
                     case MESSAGE_TYPE::PMR: {
                         auto &request = *reinterpret_cast<PossessionMoveRequest *>(ctx.request);
                         // we are not owner therefore we transfer the page or notify if possession removed
                         auto guard = bm.findFrame<CONTENTION_METHOD::NON_BLOCKING>(PID(request.pid), Invalidation(),
                                                                                    ctx.bmId);
                         // -------------------------------------------------------------------------------------
                         if (guard.state == STATE::RETRY) {
                             ensure(guard.latchState != LATCH_STATE::EXCLUSIVE);
                             if (!request.needPage)  // otherwise we need to let the request complete
                                 guard.frame->mhWaiting = true;
                             mailboxes[mailboxIdx] = 1;
                             counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                             continue;
                         }
                         // -------------------------------------------------------------------------------------
                         // this is to fix the page eviction problem
                         if(pageIdManager.shuffleState != SHUFFLE_STATE::AFTER_SHUFFLE){
                             ensure(guard.state != STATE::NOT_FOUND);
                         }
                         ensure(guard.state != STATE::UNINITIALIZED);
                         ensure(guard.frame);
                         ensure(request.pid == guard.frame->pid);
                         ensure(guard.frame->page != nullptr);
                         ensure(guard.frame->latch.isLatched());
                         ensure(guard.latchState == LATCH_STATE::EXCLUSIVE);
                         // -------------------------------------------------------------------------------------
                         auto &response = *MessageFabric::createMessage<PossessionMoveResponse>(ctx.response,
                                                                                                RESULT::WithPage);
                         // Respond
                         // -------------------------------------------------------------------------------------
                         if (request.needPage) {
                             writePageAndMsg(clientId, guard.frame->page, request.pageOffset, response,
                                             threads::ThreadContext::my().page_handle);
                             counters.incr(profiling::WorkerCounters::rdma_pages_tx);
                         } else {
                             response.resultType = RESULT::NoPage;
                             writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                         }
                         // -------------------------------------------------------------------------------------
                         // Invalidate Page
                         // -------------------------------------------------------------------------------------
                         bm.removeFrame(*guard.frame, [&](BufferFrame &frame) {
                             ctx.activeInvalidationBatch->add(frame.page);
                         });
                         // -------------------------------------------------------------------------------------
                         break;
                     }
                     case MESSAGE_TYPE::PCR: {
                         auto &request = *reinterpret_cast<PossessionCopyRequest *>(ctx.request);
                         auto guard = bm.findFrame<CONTENTION_METHOD::NON_BLOCKING>(PID(request.pid), Copy(), ctx.bmId);
                         auto &response = *MessageFabric::createMessage<rdma::PossessionCopyResponse>(ctx.response,
                                                                                                      RESULT::WithPage);
                         // -------------------------------------------------------------------------------------
                         // entry already invalidated by someone
                         if ((guard.state == STATE::UNINITIALIZED || guard.state == STATE::NOT_FOUND)) {
                             response.resultType = RESULT::CopyFailedWithInvalidation;
                             writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                             ctx.retries = 0;
                             counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                             continue;
                         }
                         // -------------------------------------------------------------------------------------
                         // found entry but could not latch check max restart
                         if (guard.state == STATE::RETRY) {
                             if (guard.frame->pVersion == request.pVersion && ctx.retries <
                                                                              FLAGS_messageHandlerMaxRetries) { // is this abort needed? to check if mh_waiting?
                                 ctx.retries++;
                                 mailboxes[mailboxIdx] = 1;
                                 counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                                 continue;
                             }
                             response.resultType = RESULT::CopyFailedWithRestart;
                             writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                             ctx.retries = 0;
                             counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                             continue;
                         }
                         // -------------------------------------------------------------------------------------
                         // potential deadlock, restart and release latches
                         if (guard.frame->mhWaiting && guard.frame->state != BF_STATE::HOT) {
                             ensure((guard.frame->state == BF_STATE::IO_RDMA) | (guard.frame->state == BF_STATE::FREE));
                             response.resultType = RESULT::CopyFailedWithRestart;
                             writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                             guard.frame->latch.unlatchShared();
                             ctx.retries = 0;
                             counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                             continue;
                         }
                         // -------------------------------------------------------------------------------------
                         ensure(guard.frame->possession == POSSESSION::SHARED);
                         ensure(request.pid == guard.frame->pid);
                         ensure(guard.frame->pVersion == request.pVersion);
                         ensure(guard.frame != nullptr);
                         ensure(guard.frame->page != nullptr);
                         // -------------------------------------------------------------------------------------
                         // write back pages
                         writePageAndMsg(clientId, guard.frame->page, request.pageOffset, response,
                                         threads::ThreadContext::my().page_handle);
                         counters.incr(profiling::WorkerCounters::rdma_pages_tx);
                         guard.frame->latch.unlatchShared();
                         ctx.retries = 0;

                         break;
                     }
                     case MESSAGE_TYPE::PUR: {
                         auto &request = *reinterpret_cast<PossessionUpdateRequest *>(ctx.request);
                         auto &response = *MessageFabric::createMessage<rdma::PossessionUpdateResponse>(ctx.response,
                                                                                                        RESULT::UpdateSucceed);
                         //first check if we are even the directory - otherwise it there is no need to lock
                         bool localPage = pageIdManager.isNodeDirectoryOfPageId(request.pid);
                         if (localPage == false) {
                             response.resultType = RESULT::DirectoryChanged;
                             writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                             break;
                         }
                         auto guard = bm.findFrame<CONTENTION_METHOD::NON_BLOCKING>(request.pid, Invalidation(),
                                                                                    ctx.bmId);
                         // -------------------------------------------------------------------------------------



                         if ((guard.state == STATE::RETRY) && (guard.frame->pVersion == request.pVersion)) {
                             guard.frame->mhWaiting = true;
                             ensure(guard.latchState == LATCH_STATE::UNLATCHED);
                             mailboxes[mailboxIdx] = 1;
                             counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                             continue;
                         }

                         if ((guard.frame->pVersion > request.pVersion)) {
                             response.resultType = rdma::RESULT::UpdateFailed;
                             writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                             guard.frame->mhWaiting = false;
                             if (guard.latchState == LATCH_STATE::EXCLUSIVE) guard.frame->latch.unlatchExclusive();
                             counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                             continue;
                         }

                         ensure(guard.state != STATE::UNINITIALIZED);
                         ensure(guard.state != STATE::NOT_FOUND);
                         ensure(guard.state != STATE::RETRY);
                         ensure(request.pid == guard.frame->pid);
                         ensure(guard.frame->pVersion == request.pVersion);
                         // -------------------------------------------------------------------------------------
                         ensure(guard.frame->latch.isLatched());
                         ensure(guard.latchState == LATCH_STATE::EXCLUSIVE);
                         ensure((guard.frame->possessors.shared.test(ctx.bmId)));
                         // -------------------------------------------------------------------------------------
                         // Update states
                         // -------------- -----------------------------------------------------------------------
                         guard.frame->pVersion++;
                         response.pVersion = guard.frame->pVersion;
                         guard.frame->possessors.shared.reset(nodeId);  // reset own node id already
                         // test if there are more shared possessors
                         if (guard.frame->possessors.shared.any()) {
                             response.resultType = RESULT::UpdateSucceedWithSharedConflict;
                             response.conflictingNodeId = guard.frame->possessors.shared;
                         }
                         // -------------------------------------------------------------------------------------
                         // evict page as other node modifys it
                         if (guard.frame->state != BF_STATE::EVICTED) {
                             ensure(guard.frame->page);
                             ctx.activeInvalidationBatch->add(guard.frame->page);
                             guard.frame->page = nullptr;
                             guard.frame->state = BF_STATE::EVICTED;
                         }
                         // -------------------------------------------------------------------------------------
                         guard.frame->possession = POSSESSION::EXCLUSIVE;
                         guard.frame->setPossessor(ctx.bmId);
                         guard.frame->dirty = true;  // set dirty
                         ensure(guard.frame->isPossessor(ctx.bmId));
                         // -------------------------------------------------------------------------------------
                         writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                         // -------------------------------------------------------------------------------------
                         guard.frame->mhWaiting = false;
                         guard.frame->latch.unlatchExclusive();
                         // -------------------------------------------------------------------------------------
                         break;
                     }
                     case MESSAGE_TYPE::RAR: {
                         [[maybe_unused]] auto &request = *reinterpret_cast<RemoteAllocationRequest *>(ctx.request);
                         // -------------------------------------------------------------------------------------
                         PID pid = PID(pageIdManager.addPage());
                         // -------------------------------------------------------------------------------------
                         BufferFrame &frame = bm.insertFrame(pid, [&](BufferFrame &frame) {
                             frame.latch.latchExclusive();
                             frame.setPossession(POSSESSION::EXCLUSIVE);
                             frame.setPossessor(ctx.bmId);
                             frame.state = BF_STATE::EVICTED;
                             frame.pid = pid;
                             frame.pVersion = 0;
                             frame.dirty = true;
                             frame.epoch = bm.globalEpoch.load();
                         });

                         frame.latch.unlatchExclusive();
                         // -------------------------------------------------------------------------------------
                         auto &response = *MessageFabric::createMessage<rdma::RemoteAllocationResponse>(ctx.response,
                                                                                                        pid);
                         writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                         break;
                     }
                     case MESSAGE_TYPE::NLUR: {
                         auto &request = *reinterpret_cast<NodeLeavingUpdateRequest *>(ctx.request);
                         pageIdManager.prepareForShuffle(request.leavingNodeId);
                         auto &response = *MessageFabric::createMessage<rdma::NodeLeavingUpdateResponse>(ctx.response);
                         writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                         break;
                     }
                     case MESSAGE_TYPE::NFSR:{
                         auto &request = *reinterpret_cast<NodeFinishedShuffleRequest *>(ctx.request);
                         pageIdManager.handleNodeFinishedShuffling(request.leavingNodeId);
                         auto &response = *MessageFabric::createMessage<rdma::NodeFinishedShuffleResponse>(ctx.response);
                         writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                         std::cout<<"nsfr"<<std::endl;
                         break;
                     }
                     case MESSAGE_TYPE::CUSFR: {
                         auto &request = *reinterpret_cast<CreateOrUpdateShuffledFramesRequest *>(ctx.request);
                         uint8_t successfulShuffles = 0;
                         uint64_t successfulPids[AGGREGATED_SHUFFLE_MESSAGE_AMOUNT] = {};
                         for (int i = 0; i < request.amountSent; i++) {
                             PIDShuffleData pidShuffleData = request.shuffleData[i];
                             uint64_t shuffledPid = pidShuffleData.shuffledPid;
                             auto guard = bm.findFrameOrInsert<CONTENTION_METHOD::NON_BLOCKING>(PID(shuffledPid),
                                                                                                Protocol<storage::POSSESSION::EXCLUSIVE>(),
                                                                                                ctx.bmId, true);
                             if (guard.state == STATE::RETRY) {
                                 // this it to deal with a case of the distrubted deadlock
                             } else {
                                 pageIdManager.addPageWithExistingPageId(shuffledPid);
                                 //bool dirCheck = pageIdManager.isNodeDirectoryOfPageId(shuffledPid);
                                 //ensure(dirCheck);
                                 //ensure(pageIdManager.shuffleState != SHUFFLE_STATE::AFTER_SHUFFLE);
                                 guard.frame->possession = pidShuffleData.possession;
                                 if (pidShuffleData.possession == POSSESSION::SHARED) {
                                     guard.frame->possessors.shared.bitmap = pidShuffleData.possessors;
                                 } else {
                                     guard.frame->possessors.exclusive = pidShuffleData.possessors;
                                 }
                                 guard.frame->pid = shuffledPid;
                                 uint64_t localpVersion = guard.frame->pVersion.load();
                                 guard.frame->pVersion = (pidShuffleData.pVersion > localpVersion) ? pidShuffleData.pVersion: localpVersion;
                                 if (guard.frame->possession == POSSESSION::SHARED) {
                                     // shared, node not possessor
                                     if (guard.frame->isPossessor(bm.nodeId) == false) {
                                         guard.frame->state = BF_STATE::EVICTED;
                                         guard.frame->dirty = true;
                                         guard.frame->page = nullptr;
                                         // shared, node possessor
                                     } else {
                                         guard.frame->dirty = pidShuffleData.dirty;
                                         ensure(guard.frame->state == BF_STATE::HOT);
                                     }
                                 } else if (guard.frame->possession == POSSESSION::EXCLUSIVE) {
                                     // exclusive, node not possessor
                                     if (guard.frame->isPossessor(bm.nodeId) == false) {
                                         guard.frame->dirty = true;
                                         guard.frame->state = BF_STATE::EVICTED;
                                         guard.frame->page = nullptr;
                                         // exclusive, node possessor
                                     } else {
                                         guard.frame->dirty = true;
                                         ensure(guard.frame->state == BF_STATE::HOT);
                                     }
                                 } else {
                                     throw std::runtime_error("Invalid possession for shuffled frame");
                                 }

                                 guard.frame->shuffled = true; // just fot gdb debug
                                 guard.frame->latch.unlatchExclusive();
                                 successfulPids[successfulShuffles] = shuffledPid;
                                 successfulShuffles++;
                             }
                         }
                         auto &response = *MessageFabric::createMessage<rdma::CreateOrUpdateShuffledFramesResponse>(
                                 ctx.response);
                         response.successfulAmount = successfulShuffles;
                         for (int i = 0; i < successfulShuffles; i++) {
                             response.successfulShuffledPid[i] = successfulPids[i];
                         }
                         writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                         //if (t_i == 0) {std::cout << " f " << std::endl;}
                         break;
                     }
                     default:
                         throw std::runtime_error("Unexpected Message in MB " + std::to_string(mailboxIdx) + " type " +
                                                  std::to_string((size_t) ctx.request->type));
                 }
                 counters.incr(profiling::WorkerCounters::mh_msgs_handled);
                 mailboxIdx = ++startPosition;
                 // submit
                 [[maybe_unused]] auto nsubmit = async_read_buffer.submit();
                 const uint64_t polled_events = async_read_buffer.pollEventsSync();
                 async_read_buffer.getReadBfs(
                         [&](BufferFrame &frame, uint64_t client_id, bool recheck_msg) {
                             // unlatch
                             frame.latch.unlatchExclusive();
                             counters.incr(profiling::WorkerCounters::ssd_pages_read);
                             if (recheck_msg) mailboxes[client_id] = 1;
                         },
                         polled_events);
                 // check async reads
             }
             threadCount--;
         }
      });

      // threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
      if ((t_i % 2) == 0)
         threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
      else
         threads::CoreManager::getInstance().pinThreadToHT(t.native_handle());
      t.detach();
   }
}



bool MessageHandler::shuffleFrameAndIsLastShuffle(scalestore::threads::Worker* workerPtr, [[maybe_unused]]uint64_t t_i, uint64_t workerAmount){
    std::chrono::steady_clock::time_point shuffle_begin = std::chrono::steady_clock::now();//todo yuvi - clean

    PageIdManager::PagesShuffleJob pagesShuffleJob = pageIdManager.getNextPagesShuffleJob(t_i,workerAmount);
    if(pagesShuffleJob.last){
        return true;
    }
    std::chrono::steady_clock::time_point afterPopping = std::chrono::steady_clock::now();//todo yuvi - clean

    PIDShuffleData shuffleData [AGGREGATED_SHUFFLE_MESSAGE_AMOUNT];
    auto newNodeId = pagesShuffleJob.newNodeId;
    auto& context_ = workerPtr->cctxs[newNodeId];
    ensure(newNodeId != nodeId);
    std::vector<std::pair<Guard, uint64_t>> guardsAndPids;
    guardsAndPids.reserve(AGGREGATED_SHUFFLE_MESSAGE_AMOUNT);
    for(uint64_t i = 0 ; i< pagesShuffleJob.amountToSend; i++){
        auto pageId = pagesShuffleJob.pageIds[i];
        auto guard = bm.findFrameOrInsert<CONTENTION_METHOD::BLOCKING>(PID(pageId), Exclusive(), nodeId,false);
        ensure(guard.state != STATE::UNINITIALIZED);
        ensure(guard.state != STATE::RETRY);
        guardsAndPids.push_back({std::move(guard), pageId});
        // the case where this page was evicted and now needs to be read before it is shuffled
        if(guard.state == STATE::SSD && guard.frame->possession == POSSESSION::NOBODY) {
            readEvictedPageBeforeShuffle(guard);
            shuffleData[i].dirty = true;
            shuffleData[i].pVersion = 0;
            shuffleData[i].possession = POSSESSION::EXCLUSIVE;
            shuffleData[i].possessors = pageIdManager.nodeId;
            shuffleData[i].shuffledPid = pageId;
        }else {
            ensure(guard.state != storage::STATE::UNINITIALIZED)
            ensure(guard.state != storage::STATE::NOT_FOUND);
            ensure(guard.state != storage::STATE::RETRY);
            ensure(guard.frame->possession != POSSESSION::NOBODY);
            uint64_t possessorsAsUint64 = (guard.frame->possession == POSSESSION::SHARED)  ? guard.frame->possessors.shared.bitmap : guard.frame->possessors.exclusive;
            shuffleData[i].dirty = guard.frame->dirty;
            shuffleData[i].pVersion = guard.frame->pVersion;
            shuffleData[i].possession = guard.frame->possession;
            shuffleData[i].possessors = possessorsAsUint64;
            shuffleData[i].shuffledPid = pageId;
        }
    }
    std::chrono::steady_clock::time_point afterLocking = std::chrono::steady_clock::now();//todo yuvi - clean

    auto onTheWayUpdateRequest = *MessageFabric::createMessage<CreateOrUpdateShuffledFramesRequest>(context_.outgoing,shuffleData,pagesShuffleJob.amountToSend);
    [[maybe_unused]]auto& createdFramesResponse = scalestore::threads::Worker::my().writeMsgSync<scalestore::rdma::CreateOrUpdateShuffledFramesResponse>(newNodeId, onTheWayUpdateRequest);

    std::chrono::steady_clock::time_point afterMessage = std::chrono::steady_clock::now(); //todo yuvi - clean

    std::set<uint64_t> successfullyShufflePids;
    for(int i = 0; i < createdFramesResponse.successfulAmount; i++){
        successfullyShufflePids.insert(createdFramesResponse.successfulShuffledPid[i]);
    }

    for (auto& guardPidPair : guardsAndPids) {
        // the case where the page was shuffled successfully
        auto& guard = guardPidPair.first;
        auto& pid = guardPidPair.second;
        if(successfullyShufflePids.find(guardPidPair.second) != successfullyShufflePids.end()) {
            if(guard.frame->isPossessor(pageIdManager.nodeId) == false){
                bm.removeFrame(*guard.frame, [](BufferFrame& /*frame*/) {});
                // guard is unlatched here^^^^^
            }else{
                guard.frame->shuffled = true; // just for debug
                guard.frame->latch.unlatchExclusive();
            }

        }// the case where the page wasn't shuffled successfully
        else{
            pageIdManager.pushJobToStack(pid,newNodeId,t_i);
            guard.frame->latch.unlatchExclusive();
        }
    }
    std::chrono::steady_clock::time_point afterAll = std::chrono::steady_clock::now();//todo yuvi - clean
    if( aggregatedTimeMeasureCounter[t_i] < aggregatedMsgAmount ){
        afterPoppingResults[t_i][aggregatedTimeMeasureCounter[t_i]] = double(std::chrono::duration_cast<std::chrono::microseconds>(afterPopping - shuffle_begin).count());
        afterLockingResults[t_i][aggregatedTimeMeasureCounter[t_i]] = double(std::chrono::duration_cast<std::chrono::microseconds>(afterLocking - shuffle_begin).count());
        afterMessageResults[t_i][aggregatedTimeMeasureCounter[t_i]] = double(std::chrono::duration_cast<std::chrono::microseconds>(afterMessage - shuffle_begin).count());
        afterAllResults[t_i][aggregatedTimeMeasureCounter[t_i]] = double(std::chrono::duration_cast<std::chrono::microseconds>(afterAll - shuffle_begin).count());
        aggregatedTimeMeasureCounter[t_i]++;
    }
    workerPtr->counters.incr_by(profiling::WorkerCounters::shuffled_frames,createdFramesResponse.successfulAmount);
    pageIdManager.pagesShuffled += createdFramesResponse.successfulAmount;
    return false;
}

void MessageHandler::printShuffleLatency (uint64_t numThreads, [[maybe_unused]]uint64_t tiToPrint){

    for(uint64_t t_i = 0; t_i < numThreads; t_i++){
        double afterPoppingMeasure = 0;
        double afterLockingMeasure = 0;
        double afterMessageMeasure = 0;
        double afterAllMeasure = 0;
        for(uint64_t i = 0; i < aggregatedMsgAmount; i++){
            afterPoppingMeasure += afterPoppingResults[t_i][i];
            afterLockingMeasure += afterLockingResults[t_i][i];
            afterMessageMeasure += afterMessageResults[t_i][i];
            afterAllMeasure += afterAllResults[t_i][i];
        }
        double aggregatedPoppingResult = afterPoppingMeasure / (double) aggregatedMsgAmount;
        double aggregatedLockingResult = afterLockingMeasure / (double) aggregatedMsgAmount;
        double aggregatedMessageResult = afterMessageMeasure / (double) aggregatedMsgAmount;
        double aggregatedAllResult = afterAllMeasure / (double) aggregatedMsgAmount;
        if(t_i == 10){
            std::cout<<"pop:"<< aggregatedPoppingResult <<std::endl;
            std::cout<<"lock:"<< aggregatedLockingResult <<std::endl;
            std::cout<<"msg:"<< aggregatedMessageResult <<std::endl;
        }
    std::cout<<t_i<<" all:"<< aggregatedAllResult <<std::endl;
    }
}


// -------------------------------------------------------------------------------------
void MessageHandler::stopThread() {
   threadsRunning = false;
   while (threadCount)
      ;  // wait
};

}  // namespace rdma
}  // namespace scalestore



