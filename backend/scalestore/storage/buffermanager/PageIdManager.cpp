//
// Created by YuvalFreund on 08.02.24.
//
#include "PageIdManager.h"

// init functions
void PageIdManager::initPageIdManager(){
    numPartitions = FLAGS_pageIdManagerPartitions;
    std::srand (std::time (0)); // this generates a new seed for randomness
    initConsistentHashingInfo(true);
    initSsdPartitions();
    initPageIdIvs();
    initPageIdToSsdSlotMaps();
}

void PageIdManager::initConsistentHashingInfo(bool firstInit){
    if(firstInit){
        for(unsigned long long i : nodeIdsInCluster){ //key - place on ring. value - node id
            for(uint64_t j = 0; j<CONSISTENT_HASHING_WEIGHT; j++){
                nodesRingLocationMap[scalestore::utils::FNV::hash(i * CONSISTENT_HASHING_WEIGHT + j) ] = i;
            }
        }
        for(auto & it : nodesRingLocationMap) {
            nodeRingLocationsVector.push_back(it.first);
        }
        std::sort (nodeRingLocationsVector.begin(), nodeRingLocationsVector.end());
    }else{
        for(auto it : nodesRingLocationMap){
            auto check = nodeIdsInCluster.find(it.second);
            if (check != nodeIdsInCluster.end()){
                newNodesRingLocationMap[it.first] = it.second;
                newNodeRingLocationsVector.push_back(it.first);
            }
        }
        std::sort (newNodeRingLocationsVector.begin(), newNodeRingLocationsVector.end());
    }
}

void PageIdManager::initSsdPartitions(){
    int ssdMaximumPagesAmount = (FLAGS_ssd_gib * 1024 * 1024) / 4;
    int partitionSize = ssdMaximumPagesAmount / FLAGS_pageIdManagerPartitions;
    uint64_t runningSSdSlotBegin = 0;
    for(uint64_t i = 0; i < numPartitions; i++){
        freeSsdSlotPartitions.try_emplace(i,runningSSdSlotBegin,partitionSize);
        runningSSdSlotBegin += partitionSize;
    }
}

void PageIdManager::initPageIdIvs(){
    uint64_t maxValue = 0xFFFFFFFFFFFFFFFF;
    uint64_t nodeAllowedIvSpaceStart = maxValue / FLAGS_max_nodes * nodeId;
    uint64_t nodePartitionSize = maxValue / FLAGS_max_nodes;
    uint64_t ivPartitionsSize = nodePartitionSize / numPartitions ; // this +1 is to avoid page id being exactly on the ring separators
    uint64_t rollingIvStart = nodeAllowedIvSpaceStart + ivPartitionsSize;
    for(uint64_t i = 0; i < numPartitions; i++){
        pageIdIvPartitions.try_emplace(i,rollingIvStart);
        rollingIvStart += ivPartitionsSize;
    }
}

void PageIdManager::initPageIdToSsdSlotMaps(){
    /*int partitionSize =  65536; // todo yuval - this needs to be parameterized for evaluation later.
    for(int i = 0; i<partitionSize; i++){
        pageIdToSsdSlotMap.try_emplace(i);
    }*/
}

// add and remove pages functions
uint64_t PageIdManager::addPage(){
    bool createPidByOldRing = (shuffleState == SHUFFLE_STATE::BEFORE_SHUFFLE);
    uint64_t retVal = getNewPageId(createPidByOldRing);
    uint64_t ssdSlotForNewPage = getFreeSsdSlot();
    ssdSlotForNewPage |= nodeIdAtMSB;
    uint64_t partition = retVal & PARTITION_MASK;
    pageIdToSsdSlotMap[partition].insertToMap(retVal,ssdSlotForNewPage);
    pagesAdded++; // todo yuvi clean

    return retVal;
}

void PageIdManager::addPageWithExistingPageId(uint64_t existingPageId){
    uint64_t ssdSlotForNewPage = getFreeSsdSlot();
    ssdSlotForNewPage |= nodeIdAtMSB;
    uint64_t partition = existingPageId & PARTITION_MASK;
    pageIdToSsdSlotMap[partition].insertToMap(existingPageId,ssdSlotForNewPage);
}

void PageIdManager::removePage(uint64_t pageId){
    uint64_t partition = pageId & PARTITION_MASK;
    uint64_t slotToFree = pageIdToSsdSlotMap[partition].getSsdSlotOfPageAndRemove(pageId);
    redeemSsdSlot(slotToFree);
}


uint64_t PageIdManager::getTargetNodeForEviction(uint64_t pageId){
    uint64_t retVal;
    if(shuffleState == SHUFFLE_STATE::BEFORE_SHUFFLE){
        retVal = searchRingForNode(pageId, true);
    }else if (shuffleState == SHUFFLE_STATE::DURING_SHUFFLE) {
        uint64_t checkCachedLocation = getCachedDirectoryOfPage(pageId);
        if(checkCachedLocation != INVALID_NODE_ID){
            retVal = checkCachedLocation; // this node is either old or new directory
        }else{
            int randomPickOldOrNew = rand() % 2;
            if(randomPickOldOrNew == 0){
                retVal = searchRingForNode(pageId, true);
            }else{
                retVal = searchRingForNode(pageId, false);
            }
        }
        if(retVal == nodeId){
            retVal = searchRingForNode(pageId, true);
        }
    }else if (shuffleState == SHUFFLE_STATE::AFTER_SHUFFLE){ // after shuffle
        retVal = searchRingForNode(pageId, false);
    }
    return retVal;
}

bool PageIdManager::isNodeDirectoryOfPageId(uint64_t pageId){
    bool retVal;
    uint64_t foundNodeId;

    // before shuffle - search old ring
    if(shuffleState == SHUFFLE_STATE::BEFORE_SHUFFLE){
        foundNodeId = searchRingForNode(pageId, true);
        retVal = (foundNodeId == nodeId);
        // during shuffle - check the cached directory
    }else if (shuffleState == SHUFFLE_STATE::DURING_SHUFFLE){
        uint64_t cachedDir = getCachedDirectoryOfPage(pageId);
        if (cachedDir == nodeId){
            retVal = true;
        }else{
            retVal = false;
        }
        // after shuffle - search the new ring
   } else if (shuffleState == SHUFFLE_STATE::AFTER_SHUFFLE){
        foundNodeId = searchRingForNode(pageId, false);
        retVal = (foundNodeId == nodeId);
   }

   return retVal;
}

uint64_t PageIdManager::getUpdatedNodeIdOfPage(uint64_t pageId, bool searchOldRing){
    uint64_t retVal;
    retVal = searchRingForNode(pageId,searchOldRing);
    return retVal;
}

uint64_t PageIdManager::getSsdSlotOfPageId(uint64_t pageId){
    uint64_t retVal;
    uint64_t partition = pageId & PARTITION_MASK;
    retVal = pageIdToSsdSlotMap[partition].getSsdSlotOfPage(pageId);
    retVal &= SSD_SLOT_MASK;
    return retVal;
}

void PageIdManager::prepareForShuffle(uint64_t nodeIdLeft){
    nodeIdsInCluster.erase(nodeIdLeft);
    initConsistentHashingInfo(false);
    if(nodeIdLeft != nodeId){
        // for the leaving node this is done after all other nodes are aware
        shuffleState = SHUFFLE_STATE::DURING_SHUFFLE;
    }else{ // leaving node has to prepare stack for shuffle jobs
        for(int i = 0; i< 20; i++){ // todo yuvi should maybe be worker flags..
            // -1 ensures that the first thread is starting with 0.
            threadsWorkingShuffleMapIdx[i] = -1;
            // This is initiated that way so when the first shuffle starts, the map will be initiated
            highestNodeIdForShuffleJobs[i] = 0;
            currentNodeIdForShuffleJobs[i] = 10;
        }
    }
}


uint64_t PageIdManager::getCachedDirectoryOfPage(uint64_t pageId){
    uint64_t retVal = INVALID_NODE_ID;
    uint64_t partition = pageId & PARTITION_MASK;
    retVal = pageIdToSsdSlotMap[partition].getDirectoryOfPage(pageId);
    return retVal;
}

void PageIdManager::setDirectoryOfPage(uint64_t pageId, uint64_t directory){
    uint64_t partition = pageId & PARTITION_MASK;
    pageIdToSsdSlotMap[partition].setDirectoryForPage(pageId,directory);
}

PageIdManager::PagesShuffleJob PageIdManager::getNextPagesShuffleJob(uint64_t t_i, uint64_t workerAmount){
    PagesShuffleJob retVal;

    restart:
    // preparing a new map of stacks
    if(highestNodeIdForShuffleJobs[t_i] < currentNodeIdForShuffleJobs[t_i]){
        // this is to ensure correct initiation
        if(threadsWorkingShuffleMapIdx[t_i] < 0){
            threadsWorkingShuffleMapIdx[t_i] = t_i;
        }else{
            threadsWorkingShuffleMapIdx[t_i] += workerAmount;
        }
        if(threadsWorkingShuffleMapIdx[t_i] >= SSD_PID_MAPS_AMOUNT){ // the case where there is no more to shuffle
            retVal.last = true;
            return retVal;
        }
        highestNodeIdForShuffleJobs[t_i] = 0;
        currentNodeIdForShuffleJobs[t_i] = 0;
        pageIdToSsdSlotMap[threadsWorkingShuffleMapIdx[t_i]].partitionLock.lock();
        for(auto pair : pageIdToSsdSlotMap[threadsWorkingShuffleMapIdx[t_i]].map){
            uint64_t pageToShuffle = pair.first;
            uint64_t destNode = 1; // todo yuval - change here
            if(destNode > highestNodeIdForShuffleJobs[t_i] ) highestNodeIdForShuffleJobs[t_i] = destNode;
            mapOfStacksForShuffle[t_i][destNode].push(pageToShuffle);
        }
        pageIdToSsdSlotMap[threadsWorkingShuffleMapIdx[t_i]].partitionLock.unlock();
    }
    // loop to try and get AGGREGATED_SHUFFLE_MESSAGE_AMOUNT jobs
    int shuffledJobs = 0;
    while(mapOfStacksForShuffle[t_i][currentNodeIdForShuffleJobs[t_i]].empty() == false && shuffledJobs < AGGREGATED_SHUFFLE_MESSAGE_AMOUNT){
        uint64_t pageToShuffle = mapOfStacksForShuffle[t_i][currentNodeIdForShuffleJobs[t_i]].top();
        mapOfStacksForShuffle[t_i][currentNodeIdForShuffleJobs[t_i]].pop();
        retVal.newNodeId = currentNodeIdForShuffleJobs[t_i];
        retVal.pageIds[shuffledJobs] = pageToShuffle;
        shuffledJobs++;
    }

    if(shuffledJobs < AGGREGATED_SHUFFLE_MESSAGE_AMOUNT) {
        // didn't get AGGREGATED_SHUFFLE_MESSAGE_AMOUNT pages to shuffle. if it is more than 0 - we return whatever we found.
        currentNodeIdForShuffleJobs[t_i]++;
        if (shuffledJobs == 0){
            // either stack is finished or mapped is finished- but still we want to return pages to shuffle
            goto restart;
        }
    }

    retVal.amountToSend = shuffledJobs;
    return retVal;
}


void  PageIdManager::broadcastNodeIsLeaving( scalestore::threads::Worker* workerPtr ) {
    prepareForShuffle(nodeId);
    for (auto nodeToUpdate: nodeIdsInCluster) {
        if (nodeToUpdate == nodeId) continue;
        auto &context_ = workerPtr->cctxs[nodeToUpdate];
        auto nodeLeavingRequest = *scalestore::rdma::MessageFabric::createMessage<scalestore::rdma::NodeLeavingUpdateRequest>(
                context_.outgoing, nodeId);
        [[maybe_unused]]auto &nodeLeavingResponse = workerPtr->writeMsgSync<scalestore::rdma::NodeLeavingUpdateResponse>(
                nodeToUpdate, nodeLeavingRequest);
    }
}

void  PageIdManager::broadcastNodeFinishedShuffling(scalestore::threads::Worker* workerPtr){
    this->shuffleState = SHUFFLE_STATE::AFTER_SHUFFLE;
    for (auto nodeToUpdate: nodeIdsInCluster) {
        if (nodeToUpdate == nodeId) continue;
        auto &context_ = workerPtr->cctxs[nodeToUpdate];
        auto nodeFinishedShuffleRequest = *scalestore::rdma::MessageFabric::createMessage<scalestore::rdma::NodeFinishedShuffleRequest>(
                context_.outgoing, nodeId);
        [[maybe_unused]]auto &nodeFinishedShuffleResponse = workerPtr->writeMsgSync<scalestore::rdma::NodeLeavingUpdateResponse>(
                nodeToUpdate, nodeFinishedShuffleRequest);
    }
}


uint64_t PageIdManager::getFreeSsdSlot(){
    uint64_t  retVal;
    while(true){
        auto chosenPartition = freeSsdSlotPartitions.find(rand() % numPartitions);
        retVal = chosenPartition->second.getFreeSlotForPage();
        if(retVal != INVALID_SSD_SLOT){
            break;
        }
    }
    return retVal;
}

void PageIdManager::pushJobToStack(uint64_t pageId, uint64_t nodeIdToShuffle, uint64_t t_i){
    pageIdShuffleMtx.lock();
    mapOfStacksForShuffle[t_i][nodeIdToShuffle].push(pageId);
    pageIdShuffleMtx.unlock();
}

void PageIdManager::redeemSsdSlot(uint64_t freedSsdSlot){
    auto chosenPartition = freeSsdSlotPartitions.find(rand() % numPartitions);
    chosenPartition->second.insertFreedSsdSlot(freedSsdSlot);
}

uint64_t PageIdManager::linearSearchOnTheRing(uint64_t pageId, bool searchOldRing) {
    uint64_t retVal;
    int foundLocation = -1;
    std::map<uint64_t, uint64_t> * mapToSearch = searchOldRing ? (&nodesRingLocationMap ) : (&newNodesRingLocationMap);
    std::vector<uint64_t> * vectorToSearch = searchOldRing ? (&nodeRingLocationsVector) : (&newNodeRingLocationsVector);
    //cyclic case
    int vecSize = vectorToSearch->size();
    for(int idx = 0 ; idx < vecSize; idx++ ){
        if(pageId < vectorToSearch->at(idx)){
            // cyclic case - after the 0 spot
            if(idx == 0){
                foundLocation = vectorToSearch->size() -1;
            }else{
                foundLocation = idx - 1;
            }
            break;
        }
    }
    // cyclic case - before the 0 spot
    if(foundLocation == -1){
        foundLocation = vectorToSearch->size() -1;
    }
    retVal = mapToSearch->find(vectorToSearch->at(foundLocation))->second;
    ensure(retVal < 2);
    return retVal;
}

uint64_t PageIdManager::binarySearchOnRing(uint64_t pageId, bool searchOldRing){
    uint64_t retVal;
    std::map<uint64_t, uint64_t> * mapToSearch = searchOldRing ? (&nodesRingLocationMap ) : (&newNodesRingLocationMap);
    std::vector<uint64_t> * vectorToSearch = searchOldRing ? (&nodeRingLocationsVector) : (&newNodeRingLocationsVector);
    uint64_t l = 0;
    uint64_t r = vectorToSearch->size() - 1;
    // edge case for cyclic operation
    if(pageId < vectorToSearch->at(l) || pageId > vectorToSearch->at(r)) {
        auto itr = mapToSearch->find(vectorToSearch->at(r));
        return itr->second;
    }
    // binary search
    while (l <= r) {
        uint64_t m = l + (r - l) / 2;
        if (vectorToSearch->at(m) <= pageId && vectorToSearch->at(m + 1) > pageId) {
            auto itr = mapToSearch->find(vectorToSearch->at(r));
            retVal = itr->second;
            break;
        }
        if (vectorToSearch->at(m) < pageId) {
            l = m + 1;
        } else{
            r = m - 1;
        }
    }
    return retVal;
}

uint64_t PageIdManager::searchRingForNode(uint64_t pageId, bool searchOldRing){
    return linearSearchOnTheRing(pageId , searchOldRing);
}

uint64_t PageIdManager::getNewPageId(bool oldRing){
    uint64_t retVal;
    bool lockCheck;
    while(true){
        auto chosenPartition = pageIdIvPartitions.find(rand() % numPartitions);
        lockCheck = chosenPartition->second.pageIdPartitionMtx.try_lock();
        if(lockCheck){
            uint64_t temp = chosenPartition->second.guess;
            temp++;
            retVal = scalestore::utils::FNV::hash(temp);
            while(getUpdatedNodeIdOfPage(retVal, oldRing) != nodeId){
                temp++;
                retVal = scalestore::utils::FNV::hash(temp);
            }
            chosenPartition->second.storeGuess(temp);
            break;
        }
    }

    return retVal;
}



void PageIdManager::handleNodeFinishedShuffling([[maybe_unused]]uint64_t nodeIdLeaving){
    shuffleState = SHUFFLE_STATE::AFTER_SHUFFLE;
}




