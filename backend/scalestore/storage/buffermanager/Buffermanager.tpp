template <CONTENTION_METHOD method, typename ACCESS>
Guard Buffermanager::findFrameOrInsert(PID pid, ACCESS functor, NodeID nodeId_, [[maybe_unused]]bool fromShuffle)  // move functor
{
   Guard g;
   auto& b = pTable[pid];
   auto& ht_latch = b.ht_bucket_latch;
   // -------------------------------------------------------------------------------------
restart:
   auto b_version = ht_latch.optimisticLatchOrRestart();
   if (!b_version.has_value()) goto restart;
   // -------------------------------------------------------------------------------------
   // handle fast and slow path in one loop
   BufferFrame* b_ptr = &b;
   BufferFrame** current_slot = &b_ptr;
   // -------------------------------------------------------------------------------------
   while (*current_slot) {
      BufferFrame* tmp = *current_slot;                                  // copy pointer value next field in BF can change;
      RESTART(!ht_latch.optimisticUnlatch(b_version.value()), restart);  // validate against that
      if (tmp->pid == pid) {
         g.frame = tmp;
         RESTART(!ht_latch.optimisticUnlatch(b_version.value()), restart);
         functor(g, nodeId_,pageIdManager);
         // -------------------------------------------------------------------------------------
         // non-blocking does not restart
         if constexpr (method == CONTENTION_METHOD::NON_BLOCKING) {
            if (!ht_latch.checkOrRestart(b_version.value())) {
                functor.undo(g);
            }
             return g;
         }
         // -------------------------------------------------------------------------------------
         // blocking goes to restart
         if (g.state == STATE::RETRY) goto restart;
         if (!ht_latch.checkOrRestart(b_version.value())) {  // Lock-coupling
            functor.undo(g);
            goto restart;
         }
         return g;
      }
      current_slot = &(tmp->next);                                       // take address of next field
      RESTART(!ht_latch.optimisticUnlatch(b_version.value()), restart);  // validate against a nullptr change
   }
   // -------------------------------------------------------------------------------------
   // Insert
   // -------------------------------------------------------------------------------------
   RESTART(!ht_latch.optimisticUnlatch(b_version.value()), restart);         // validate against a nullptr change
   Page* page = pageFreeList.pop(threads::ThreadContext::my().page_handle);  // we can block because no locks are acquired
   if (!ht_latch.upgradeToExclusiveLatch(b_version.value())) {
      pageFreeList.push(page, threads::ThreadContext::my().page_handle);
      goto restart;
   }
   // -------------------------------------------------------------------------------------
   g.frame = &b;
   if (b.state != BF_STATE::FREE) {  // inline bucket not empty
      if (!frameFreeList.try_pop(*current_slot, threads::ThreadContext::my().bf_handle)) {
         ht_latch.unlatchExclusive();
         pageFreeList.push(page, threads::ThreadContext::my().page_handle);
         goto restart;
      }
      g.frame = *current_slot;
   }
   // -------------------------------------------------------------------------------------
    g.frame->latch.latchExclusive(); // POSSIBLE ERROR?
    ensure(g.frame->pid == EMPTY_PID);
    bool localPage = pageIdManager.isNodeDirectoryOfPageId(pid.id);
    g.frame->state =
            localPage ? BF_STATE::IO_SSD : BF_STATE::IO_RDMA;  // important to modify state before releasing the hashtable latch
   g.frame->page = page;
   g.frame->pid = pid;
   g.frame->epoch = globalEpoch.load();
   g.frame->setPossession(POSSESSION::NOBODY);
   // -------------------------------------------------------------------------------------
   ht_latch.unlatchExclusive();
   // -------------------------------------------------------------------------------------
   g.state = localPage ? STATE::SSD : STATE::REMOTE;
   g.vAcquired = g.frame->latch.version;
   g.latchState = LATCH_STATE::EXCLUSIVE;
   return g;
}

template <CONTENTION_METHOD method, typename ACCESS>
Guard Buffermanager::findFrame(PID pid, ACCESS functor, NodeID nodeId_)  // move functor
{
   Guard g;
   auto& b = pTable[pid];
   auto& ht_latch = b.ht_bucket_latch;
   // -------------------------------------------------------------------------------------
restart:
   auto b_version = ht_latch.optimisticLatchOrRestart();
   if (!b_version.has_value()) goto restart;
   // -------------------------------------------------------------------------------------
   // handle fast and slow path in one loop
   BufferFrame* b_ptr = &b;
   BufferFrame** current_slot = &b_ptr;
   // -------------------------------------------------------------------------------------
   while (*current_slot) {
      BufferFrame* tmp = *current_slot;                                  // copy pointer value next field in BF can change;
      RESTART(!ht_latch.optimisticUnlatch(b_version.value()), restart);  // validate against that
      if (tmp->pid == pid) {
         g.frame = tmp;
         RESTART(!ht_latch.optimisticUnlatch(b_version.value()), restart);
         functor(g, nodeId_,pageIdManager);
         // -------------------------------------------------------------------------------------
         // non-blocking does not restart
         if constexpr (method == CONTENTION_METHOD::NON_BLOCKING) {
            if (!ht_latch.checkOrRestart(b_version.value())) { functor.undo(g); }
            return g;
         }
         // -------------------------------------------------------------------------------------
         // blocking restarts
         if (g.state == STATE::RETRY) goto restart;
         if (!ht_latch.checkOrRestart(b_version.value())) {  // Lock-coupling
            functor.undo(g);
            goto restart;
         }
         return g;
      }
      current_slot = &(tmp->next);                                       // take address of next field
      RESTART(!ht_latch.optimisticUnlatch(b_version.value()), restart);  // validate against a nullptr change
   }
   // -------------------------------------------------------------------------------------
   // PID not found
   RESTART(!ht_latch.optimisticUnlatch(b_version.value()), restart);  // validate against an concurrent insert
   g.state = STATE::NOT_FOUND;
   return g;
}
// -------------------------------------------------------------------------------------
template <typename ACCESS>
Guard Buffermanager::fix(PID pid, ACCESS functor) {
    bool usingOldRing = true; // this is for the page id manager
    using namespace rdma;
   // -------------------------------------------------------------------------------------
restart:
   Guard guard = findFrameOrInsert<CONTENTION_METHOD::BLOCKING>(pid, functor, nodeId, false);
   ensure(guard.state != STATE::UNINITIALIZED);
   ensure(guard.state != STATE::RETRY);
   // -------------------------------------------------------------------------------------
   // hot path
   // -------------------------------------------------------------------------------------
   if (guard.state == STATE::INITIALIZED) {
      _mm_prefetch(&guard.frame->page->data[0], _MM_HINT_T0);
      if (guard.frame->epoch < globalEpoch) guard.frame->epoch = globalEpoch.load();
      return guard; // here paged is avaiable locally and fix finsihes
   }
   // -------------------------------------------------------------------------------------
   // helper lambdas
   // -------------------------------------------------------------------------------------
   auto invalidateSharedConflicts = [&](StaticBitmap<64>& shared, uint64_t pVersion) {
      // Invalidate all conflicts
      shared.applyForAll([&](uint64_t id) {
         auto& context_ = threads::Worker::my().cctxs[id];
         auto pmRequest = *MessageFabric::createMessage<PossessionMoveRequest>(context_.outgoing, pid, false, 0,
                                                                               pVersion);  // move possesion no page page
         threads::Worker::my().writeMsgASync<PossessionMoveResponse>(id, pmRequest);
      });

      shared.applyForAll([&](uint64_t id) {
         auto& pmResponse = threads::Worker::my().collectResponseMsgASync<PossessionMoveResponse>(id);
         // -------------------------------------------------------------------------------------
         ensure(pmResponse.resultType == RESULT::NoPage);
         shared.reset(id);
      });
   };
   // -------------------------------------------------------------------------------------
   auto movePageRnd = [&](StaticBitmap<64>& shared, uintptr_t pageOffset, uint64_t pVersion) {
      shared.applyToOneRnd([&](uint64_t id) {
         auto& context_ = threads::Worker::my().cctxs[id];
         auto pmRequest = *MessageFabric::createMessage<PossessionMoveRequest>(context_.outgoing, pid, true, pageOffset,
                                                                               pVersion);  // move possesion in page
         auto& pmResponse = threads::Worker::my().writeMsgSync<PossessionMoveResponse>(id, pmRequest);
         // -------------------------------------------------------------------------------------
         ensure(pmResponse.resultType == RESULT::WithPage);
         shared.reset(id);
      });
   };
   // -------------------------------------------------------------------------------------
   auto copyPageRnd = [&](StaticBitmap<64>& shared, uintptr_t pageOffset, uint64_t pVersion, RESULT& result, uint64_t& randomId) {
      shared.applyToOneRnd([&](uint64_t id) {
         randomId = id;
         auto& context_ = threads::Worker::my().cctxs[id];
         auto pcRequest = *MessageFabric::createMessage<PossessionCopyRequest>(context_.outgoing, pid, pageOffset,
                                                                               pVersion);  // move possesion incl page
         auto& pcResponse = threads::Worker::my().writeMsgSync<PossessionCopyResponse>(id, pcRequest);
         // -------------------------------------------------------------------------------------
         result = pcResponse.resultType;
      });
   };
   // cold path
   // -------------------------------------------------------------------------------------
   // SSD
   // -------------------------------------------------------------------------------------
   volatile int mask = 1;  // for backoff
   switch (guard.state) {
      case STATE::SSD:
      {
         ensure(guard.frame != nullptr);
         ensure(guard.frame->latch.isLatched());

         readPageSync(guard.frame->pid, reinterpret_cast<uint8_t*>(guard.frame->page));
         // -------------------------------------------------------------------------------------
         // update state
         guard.frame->possession = (functor.type == LATCH_STATE::EXCLUSIVE) ? POSSESSION::EXCLUSIVE : POSSESSION::SHARED;
         guard.frame->setPossessor(nodeId);
         guard.frame->state = BF_STATE::HOT;  // important as it allows to remote copy without latch
         ensure(guard.frame->pid != EMPTY_PID);
         // -------------------------------------------------------------------------------------
         // downgrade latch
         if (guard.needDowngrade(functor.type)) {
            guard.downgrade(functor.type);
            if (guard.state == STATE::RETRY) { goto restart; }
         }
         // -------------------------------------------------------------------------------------
         guard.state = STATE::INITIALIZED;
         // -------------------------------------------------------------------------------------
         break;
      }
      // -------------------------------------------------------------------------------------
      // Remote Fix - no page and need to request it from remote
      // -------------------------------------------------------------------------------------
      case STATE::REMOTE: {
         // ------------------------------------------------------------------------------------->
         ensure(guard.frame);
         ensure(guard.frame->state == BF_STATE::IO_RDMA);
         ensure(FLAGS_nodes > 1);
         ensure(guard.frame->latch.isLatched());
         ensure(guard.latchState == LATCH_STATE::EXCLUSIVE);
         // -------------------------------------------------------------------------------------
         // -------------------------------------------------------------------------------------
         guard.frame->state = BF_STATE::IO_RDMA;
         // -------------------------------------------------------------------------------------
         uintptr_t pageOffset = (uintptr_t)guard.frame->page;
         // -------------------------------------------------------------------------------------
         uint64_t ownerId = pageIdManager.getUpdatedNodeIdOfPage(pid, usingOldRing);
         if(ownerId == nodeId){
             ownerId = pageIdManager.getUpdatedNodeIdOfPage(pid, false);
         }
         ensure(ownerId != nodeId);
         auto& contextT = threads::Worker::my().cctxs[ownerId];
         auto& request = *MessageFabric::createMessage<PossessionRequest>(
             contextT.outgoing, ((functor.type == LATCH_STATE::EXCLUSIVE) ? MESSAGE_TYPE::PRX : MESSAGE_TYPE::PRS), pid, pageOffset);
         threads::Worker::my().writeMsgASync<PossessionResponse>(ownerId, request);
         // -------------------------------------------------------------------------------------
         _mm_prefetch(&guard.frame->page->data[0], _MM_HINT_T0);  // prefetch first cache line of page
         // -------------------------------------------------------------------------------------
          auto& response = threads::Worker::my().collectResponseMsgASync<PossessionResponse>(ownerId);
         // -------------------------------------------------------------------------------------
         // set version from owner
         guard.frame->pVersion = response.pVersion;

         // -------------------------------------------------------------------------------------
         if (response.resultType == RESULT::NoPageExclusiveConflict) {
            // -------------------------------------------------------------------------------------
            // Resolve Exclusive Conflict
            // -------------------------------------------------------------------------------------
            auto& context_ = threads::Worker::my().cctxs[response.conflictingNodeId];
            auto& pmRequest = *MessageFabric::createMessage<PossessionMoveRequest>(context_.outgoing, pid, true, pageOffset,
                                                                                   guard.frame->pVersion);  // move possesion incl page
            auto& pmResponse = threads::Worker::my().writeMsgSync<PossessionMoveResponse>(response.conflictingNodeId, pmRequest);
            // -------------------------------------------------------------------------------------
            ensure(pmResponse.resultType == RESULT::WithPage);
            // -------------------------------------------------------------------------------------
         } else if ((response.resultType == RESULT::NoPageSharedConflict) | (response.resultType == RESULT::WithPageSharedConflict)) {
            // -------------------------------------------------------------------------------------
            // Resolve Shared Conflicts
            // -------------------------------------------------------------------------------------
            StaticBitmap<64> shared(response.conflictingNodeId);
            ensure(shared.any());
            // -------------------------------------------------------------------------------------
            // Get page from random node
            if (response.resultType == RESULT::NoPageSharedConflict) { movePageRnd(shared, pageOffset, guard.frame->pVersion); }
            invalidateSharedConflicts(shared, guard.frame->pVersion);
            // -------------------------------------------------------------------------------------
         } else if (response.resultType == RESULT::NoPageEvicted) {
            // -------------------------------------------------------------------------------------
            // Copy up-to-date page from remote
            // -------------------------------------------------------------------------------------
         restartNoPageEvicted:
            StaticBitmap<64> shared(response.conflictingNodeId);
            ensure(shared.any());
            RESULT result;  // remote copy can fail
            uint64_t randomId = 0;
            // -------------------------------------------------------------------------------------
            copyPageRnd(shared, pageOffset, guard.frame->pVersion, result, randomId);
            // -------------------------------------------------------------------------------------
            if (result != RESULT::WithPage) {
               // new page hence we should reclaim it?
               if (guard.frame->mhWaiting == true) {
                  ensure(guard.frame->latch.isLatched());
                  guard.frame->latch.unlatchExclusive();
                  BACKOFF();
                  goto restart;
               }
               goto restartNoPageEvicted;
            }
         } else if (response.resultType == RESULT::DirectoryChanged){
             std::cout<<"o"<<std::endl;
             usingOldRing = false;
             functor.undo(guard);
             goto restart;

         }
         // -------------------------------------------------------------------------------------
         // ensure(guard.frame->page->magicDebuggingNumber == pid);
         // -------------------------------------------------------------------------------------
         // update state
         guard.frame->possession = (functor.type == LATCH_STATE::EXCLUSIVE) ? POSSESSION::EXCLUSIVE : POSSESSION::SHARED;
         guard.frame->setPossessor(nodeId);
         guard.frame->state = BF_STATE::HOT;  // important as it allows to remote copy without latch
         // -------------------------------------------------------------------------------------
         // downgrade latch
         if (guard.needDowngrade(functor.type)) {
            guard.downgrade(functor.type);
            if (guard.state == STATE::RETRY) {
               goto restart;
            }
         }
         // -------------------------------------------------------------------------------------
         guard.state = STATE::INITIALIZED;
         threads::Worker::my().counters.incr(profiling::WorkerCounters::rdma_pages_rx);
         // -------------------------------------------------------------------------------------
         break;
      }
      // -------------------------------------------------------------------------------------
      // Upgrade we are owner and need to change possession or page evicted
      // ------------------------------------------------------------------------------------
      case STATE::LOCAL_POSSESSION_CHANGE: {
         bool localPage = pageIdManager.isNodeDirectoryOfPageId(guard.frame->pid);

         ensure(localPage);
         ensure(guard.frame->latch.isLatched());
         ensure(guard.frame->possession != POSSESSION::NOBODY);
         // -------------------------------------------------------------------------------------
         if (guard.frame->state == BF_STATE::EVICTED) { guard.frame->page = pageFreeList.pop(threads::ThreadContext::my().page_handle); }
         uintptr_t pageOffset = (uintptr_t)guard.frame->page;
         // -------------------------------------------------------------------------------------
         if (guard.frame->possession == POSSESSION::EXCLUSIVE) {
            // -------------------------------------------------------------------------------------
            // Resolve Exclusive Conflict
            // -------------------------------------------------------------------------------------
            guard.frame->pVersion++;
            NodeID conflict = guard.frame->possessors.exclusive;
            auto& context_ = threads::Worker::my().cctxs[conflict];
            auto& pmRequest = *MessageFabric::createMessage<PossessionMoveRequest>(context_.outgoing, pid, true, pageOffset,
                                                                                   guard.frame->pVersion);  // move possesion incl page
            // -------------------------------------------------------------------------------------
            _mm_prefetch(&guard.frame->page->data[0], _MM_HINT_T0);  // prefetch first cache line of page
            // -------------------------------------------------------------------------------------
            auto& pmResponse = threads::Worker::my().writeMsgSync<PossessionMoveResponse>(conflict, pmRequest);
            // -------------------------------------------------------------------------------------
            guard.frame->possessors.exclusive = 0;  // reset
            // -------------------------------------------------------------------------------------
            ensure(pmResponse.resultType == RESULT::WithPage);
         } else if (guard.frame->possession == POSSESSION::SHARED) {
            //  Upgrade
            // -------------------------------------------------------------------------------------
            _mm_prefetch(&guard.frame->page->data[0], _MM_HINT_T0);  // prefetch first cache line of page
            // -------------------------------------------------------------------------------------
            if (functor.type == LATCH_STATE::EXCLUSIVE) {
               // -------------------------------------------------------------------------------------
               // -------------------------------------------------------------------------------------
               guard.frame->pVersion++;
               guard.frame->possessors.shared.reset(nodeId);
               auto& shared = guard.frame->possessors.shared;
               if (guard.frame->state == BF_STATE::EVICTED) { movePageRnd(shared, pageOffset, guard.frame->pVersion); }
               // -------------------------------------------------------------------------------------
               invalidateSharedConflicts(shared, guard.frame->pVersion);
               // -------------------------------------------------------------------------------------
            } else { // this means - this page is held by another node is shared possession and we also want it shared, we are the directory
               ensure(guard.frame->state == BF_STATE::EVICTED);
               auto& shared = guard.frame->possessors.shared;
               ensure(shared.any());
               RESULT result;
               uint64_t randomId = 0;
               // move possessionfirst with page copy
               // -------------------------------------------------------------------------------------
               copyPageRnd(shared, pageOffset, guard.frame->pVersion, result, randomId);
               // -------------------------------------------------------------------------------------
               if (result != RESULT::WithPage) {
                  pageFreeList.push(guard.frame->page, threads::ThreadContext::my().page_handle);
                  guard.frame->page = nullptr;
                  ensure(guard.frame->state == BF_STATE::EVICTED);
                  ensure(guard.frame->latch.isLatched());
                  guard.frame->latch.unlatchExclusive();
                  BACKOFF();
                  goto restart;
               }
            }
         } else
            throw std::runtime_error("Invalid state in fix");
         // -------------------------------------------------------------------------------------
         if (functor.type == LATCH_STATE::EXCLUSIVE)
            guard.frame->possession = POSSESSION::EXCLUSIVE;
         else
            guard.frame->possession = POSSESSION::SHARED;
         // -------------------------------------------------------------------------------------
         guard.frame->setPossessor(nodeId);
         ensure(guard.frame->isPossessor(nodeId));
         // -------------------------------------------------------------------------------------
         guard.frame->state = BF_STATE::HOT;
         // -------------------------------------------------------------------------------------
         // downgrade latch
         ensure(guard.frame->pid != EMPTY_PID);
         if (guard.needDowngrade(functor.type)) {
            guard.downgrade(functor.type);
            if (guard.state == STATE::RETRY) {
               goto restart;
            }
         }
         // -------------------------------------------------------------------------------------
         guard.state = STATE::INITIALIZED;
         break;
      }
      // -------------------------------------------------------------------------------------
      // Upgrade case we have the page, but need to upgrade possession on the owner / remote
      // -------------------------------------------------------------------------------------
      case STATE::REMOTE_POSSESSION_CHANGE: {
         threads::Worker::my().counters.incr(profiling::WorkerCounters::w_rpc_tried);
         ensure(FLAGS_nodes > 1);
         ensure(guard.frame != nullptr);
         ensure(guard.frame->latch.isLatched());
         ensure(guard.frame->possession == POSSESSION::SHARED);
         ensure(guard.frame->state == BF_STATE::HOT);
         auto pVersionOld = guard.frame->pVersion.load();
         guard.frame->pVersion++;  // update here to prevent distributed deadlock
         // -------------------------------------------------------------------------------------
          uint64_t pidOwner = pageIdManager.getUpdatedNodeIdOfPage(pid, usingOldRing);
          if(pidOwner == nodeId){
              pidOwner = pageIdManager.getUpdatedNodeIdOfPage(pid, false);
          }
          auto& contextT = threads::Worker::my().cctxs[pidOwner];
         auto& request = *MessageFabric::createMessage<PossessionUpdateRequest>(contextT.outgoing, pid, pVersionOld);
         // ----------------------------------------------------------- --------------------------
          auto& response = threads::Worker::my().writeMsgSync<PossessionUpdateResponse>(pidOwner, request);

          if(response.resultType == RESULT::DirectoryChanged){
             std::cout<<"D"<<std::endl;
             usingOldRing = false;
             ensure(guard.frame->latch.isLatched());
             guard.frame->latch.unlatchExclusive();
             guard.frame->pVersion = pVersionOld;
             goto restart;
         }
         if (response.resultType == RESULT::UpdateFailed) {
            ensure(guard.frame->latch.isLatched());
            guard.frame->pVersion = pVersionOld;
            guard.frame->latch.unlatchExclusive();
            threads::Worker::my().counters.incr(profiling::WorkerCounters::w_rpc_restarted);
            goto restart;
         }
         // -------------------------------------------------------------------------------------
         _mm_prefetch(&guard.frame->page->data[0], _MM_HINT_T0);  // prefetch first cache line of page
         // -------------------------------------------------------------------------------------
         ensure(guard.frame->pVersion == response.pVersion);
         guard.frame->pVersion = response.pVersion;
         // -------------------------------------------------------------------------------------
         if (response.resultType == RESULT::UpdateSucceedWithSharedConflict) {
            StaticBitmap<64> shared(response.conflictingNodeId);
            shared.reset(nodeId);
            invalidateSharedConflicts(shared, guard.frame->pVersion);
         }
         // -------------------------------------------------------------------------------------
         // update state
         guard.frame->possession = POSSESSION::EXCLUSIVE;
         guard.frame->setPossessor(nodeId);
         // -------------------------------------------------------------------------------------
         guard.frame->state = BF_STATE::HOT;
         // -------------------------------------------------------------------------------------
         guard.state = STATE::INITIALIZED;
         break;
      }
      default:
         break;
   }
   // -------------------------------------------------------------------------------------
   if (guard.frame->epoch < globalEpoch) guard.frame->epoch = globalEpoch.load();
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   ensure(guard.state == STATE::INITIALIZED);
   if (guard.latchState != LATCH_STATE::OPTIMISTIC) { ensure(guard.frame != nullptr); }
   // -------------------------------------------------------------------------------------
   return guard;
}
