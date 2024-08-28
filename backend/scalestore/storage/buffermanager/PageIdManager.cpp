//
// Created by YuvalFreund on 08.02.24.
//
#include "PageIdManager.h"



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
    int partitionSize = ssdMaximumPagesAmount / FLAGS_pageIdManagerPartitions; // todo yuval - this needs to be parameterized for evaluation later.
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

uint64_t PageIdManager::addPage(){
    uint64_t retVal = getNewPageId(isBeforeShuffle);
    uint64_t ssdSlotForNewPage = getFreeSsdSlot();
    ssdSlotForNewPage |= nodeIdAtMSB;
    uint64_t partition = retVal & PARTITION_MASK;
    pageIdToSsdSlotMap[partition].insertToMap(retVal,ssdSlotForNewPage);
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
    if(isBeforeShuffle){
        retVal = searchRingForNode(pageId, true);
    }else{
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
    }
    return retVal;
}

bool PageIdManager::isNodeDirectoryOfPageId(uint64_t pageId){
    bool retVal;
    if(isBeforeShuffle){
        uint64_t foundNodeId = searchRingForNode(pageId, true);
        retVal = (foundNodeId == nodeId);
    }else{
        uint64_t cachedDir = getCachedDirectoryOfPage(pageId);
        if (cachedDir == nodeId){
            retVal = true;
        }else{
            retVal = false;
        }
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
    nodeLeaving = nodeIdLeft;
    initConsistentHashingInfo(false);
    if(nodeIdLeft != nodeId){
        isBeforeShuffle = false;
    }else{
        stackForShuffleJob = pageIdToSsdSlotMap[workingShuffleMapIdx].getStackForShuffling();
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

PageIdManager::PageShuffleJob PageIdManager::getNextPageShuffleJob(){
    PageShuffleJob retVal(55885,0);
    pageIdShuffleMtx.lock();
    while(stackForShuffleJob.empty()){
        workingShuffleMapIdx++;
        if(workingShuffleMapIdx > SSD_PID_MAPS_AMOUNT) {
            retVal.last = true; // done shuffling
            return retVal;
        }
        if(pageIdToSsdSlotMap[workingShuffleMapIdx].map.size() != 0){
            stackForShuffleJob = pageIdToSsdSlotMap[workingShuffleMapIdx].getStackForShuffling();
        }
    }
    uint64_t pageToShuffle = stackForShuffleJob.top();
    stackForShuffleJob.pop();
    uint64_t destNode = getUpdatedNodeIdOfPage(pageToShuffle, false);
    retVal = PageShuffleJob(pageToShuffle,destNode);
    pageIdShuffleMtx.unlock();
    return retVal;
}

void  PageIdManager::gossipNodeIsLeaving( scalestore::threads::Worker* workerPtr ) {
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

void PageIdManager::pushJobToStack(uint64_t pageId){
    pageIdShuffleMtx.lock();
    stackForShuffleJob.push(pageId);
    pageIdShuffleMtx.unlock();
}

void PageIdManager::redeemSsdSlot(uint64_t freedSsdSlot){
    auto chosenPartition = freeSsdSlotPartitions.find(rand() % numPartitions);
    chosenPartition->second.insertFreedSsdSlot(freedSsdSlot);
}

uint64_t PageIdManager::searchRingForNode(uint64_t pageId, bool searchOldRing){
    uint64_t retVal;
    std::map<uint64_t, uint64_t> *mapToSearch = searchOldRing ? (&nodesRingLocationMap ) : (&newNodesRingLocationMap);
    std::vector<uint64_t> * vectorToSearch = searchOldRing ? (&nodeRingLocationsVector) : (&newNodeRingLocationsVector);
    if(mapToSearch->size() == 1){
        return 0;
    }
    uint64_t hashedPageId = scalestore::utils::FNV::hash(pageId);
    uint64_t l = 0;
    uint64_t r = vectorToSearch->size() - 1;
    // edge case for cyclic operation
    if(hashedPageId < vectorToSearch->at(l) || hashedPageId > vectorToSearch->at(r)) {
        auto itr = mapToSearch->find(vectorToSearch->at(r));
        return itr->second;
    }
    // binary search
    while (l <= r) {
        uint64_t m = l + (r - l) / 2;
        if (vectorToSearch->at(m) <= hashedPageId && vectorToSearch->at(m + 1) > hashedPageId) {
            auto itr = mapToSearch->find(vectorToSearch->at(r));
            retVal = itr->second;
            break;
        }
        if (vectorToSearch->at(m) < hashedPageId) {
            l = m + 1;
        } else{
            r = m - 1;
        }
    }
    return retVal;
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
            while(getUpdatedNodeIdOfPage(temp, oldRing) != nodeId){
                temp++;
            }
            chosenPartition->second.storeGuess(temp);
            retVal = temp;
            break;
        }
    }

    return retVal;
}

