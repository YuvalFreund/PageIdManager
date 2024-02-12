//
// Created by YuvalFreund on 08.02.24.
//
#include "PageIdManager.h"



void PageIdManager::initPageIdManager(){
    numPartitions = 400; // todo yuval - this needs to be parameterized later..
    std::srand (std::time (0)); // this generates a new seed for randomness
    initConsistentHashingInfo(true);
    initSsdPartitions();
    initPageIdIvs();
    initPageIdToSsdSlotMaps();
}

void PageIdManager::initConsistentHashingInfo(bool firstInit){
    std::map<uint64_t, uint64_t> * mapToUpdate = firstInit ?  (&nodesRingLocationMap) : (&newNodesRingLocationMap);
    std::vector<uint64_t> * vectorToUpdate = firstInit ? (&nodeRingLocationsVector) :(&newNodeRingLocationsVector);
    std::vector<uint64_t> nodeIdsVector(nodeIdsInCluster.begin(), nodeIdsInCluster.end());

    for(unsigned long long i : nodeIdsVector){
        for(uint64_t j = 0; j<CONSISTENT_HASHING_WEIGHT; j++){
            (*mapToUpdate)[tripleHash(i * CONSISTENT_HASHING_WEIGHT + j) ] = i;
        }
    }
    for(auto & it : *mapToUpdate) {
        vectorToUpdate->push_back(it.first );
    }
    std::sort (vectorToUpdate->begin(), vectorToUpdate->end());
}

void PageIdManager::initSsdPartitions(){
    int partitionSize = 65536; // todo yuval - this needs to be parameterized for evaluation later.
    uint64_t runningSSdSlotBegin = 0;
    for(uint64_t i = 0; i < numPartitions; i++){
        ssdSlotPartitions.try_emplace(i,runningSSdSlotBegin,partitionSize);
        runningSSdSlotBegin += partitionSize;
    }
}

void PageIdManager::initPageIdIvs(){
    uint64_t maxValue = 0xFFFFFFFFFFFFFFFF;
    uint64_t partitionSize = maxValue / numPartitions + 1; // this +1 is to avoid page id being exactly on the ring separators
    uint64_t rollingIvStart = partitionSize;
    for(uint64_t i = 0; i < numPartitions; i++){
        pageIdIvPartitions.try_emplace(i,rollingIvStart);
        rollingIvStart += partitionSize;
    }
}

void PageIdManager::initPageIdToSsdSlotMaps(){
    int partitionSize =  65536; // todo yuval - this needs to be parameterized for evaluation later.
    for(int i = 0; i<partitionSize; i++){
        pageIdToSsdSlotMap.try_emplace(i);
    }
}

uint64_t PageIdManager::addPage(){
    uint64_t retVal = getNewPageId(isBeforeShuffle);
    uint64_t ssdSlotForNewPage = getFreeSsdSlot();
    uint64_t partition = retVal & PAGE_ID_MASK;
    pageIdToSsdSlotMap[partition].insertToMap(retVal,ssdSlotForNewPage);
    return retVal;
}

void PageIdManager::removePage(uint64_t pageId){
    uint64_t partition = pageId & PAGE_ID_MASK;
    uint64_t slotToFree = pageIdToSsdSlotMap[partition].getSsdSlotOfPage(pageId); //todo yuval - deal with page not found
    redeemSsdSlot(slotToFree);
}

uint64_t PageIdManager::getNodeIdOfPage(uint64_t pageId, bool searchOldRing){
    uint64_t retVal;
    std::map<uint64_t, uint64_t> *mapToSearch = searchOldRing ? (&nodesRingLocationMap ) : (&newNodesRingLocationMap);
    std::vector<uint64_t> * vectorToSearch = searchOldRing ? (&nodeRingLocationsVector) : (&newNodeRingLocationsVector);
    uint64_t hashedPageId = tripleHash(pageId);
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

uint64_t PageIdManager::getSsdSlotOfPageId(uint64_t pageId){
    uint64_t retVal;
    uint64_t partition = pageId & PAGE_ID_MASK;
    retVal = pageIdToSsdSlotMap[partition].getSsdSlotOfPage(pageId);
    return retVal;
}


void PageIdManager::prepareForShuffle(uint64_t nodeIdLeft){
    isBeforeShuffle = false;
    nodeIdsInCluster.erase(nodeIdLeft);
    initConsistentHashingInfo(false);
}

bool PageIdManager::hasPageMovedDirectory(uint64_t pageId){
    bool retVal;
    uint64_t partition = pageId & PAGE_ID_MASK;
    retVal = pageIdToSsdSlotMap[partition].isDirectoryChangedForPage(pageId);
    return retVal;
}

void PageIdManager::setPageMovedDirectory(uint64_t pageId){
    uint64_t partition = pageId & PAGE_ID_MASK;
    pageIdToSsdSlotMap[partition].setDirectoryChangedForPage(pageId);
}


PageIdManager::PageShuffleJob PageIdManager::getNextPageShuffleJob(){
    PageShuffleJob retVal(0,0);
    pageIdShuffleMtx.lock();

    while(stackForShuffleJob.empty()){
        workingShuffleMapIdx++;
        if(workingShuffleMapIdx < ShuffleMapAmount) {
            retVal.last = true; // done shuffling
            return retVal;
        }
        if(pageIdToSsdSlotMap.find(workingShuffleMapIdx) != pageIdToSsdSlotMap.end()){
            stackForShuffleJob = pageIdToSsdSlotMap[workingShuffleMapIdx].getStackForShuffling();
        }
    }
    while(true){
        uint64_t pageToShuffle = stackForShuffleJob.top();
        stackForShuffleJob.pop();
        uint64_t destNode = getNodeIdOfPage(pageToShuffle, false);
        if(destNode != nodeId){
            retVal = PageShuffleJob(pageToShuffle,destNode);
            pageIdShuffleMtx.unlock();
            break;
        }
    }
    return retVal;
}


void  PageIdManager::gossipNodeIsLeaving( scalestore::threads::Worker* workerPtr ) {
    for (auto nodeToUpdate: nodeIdsInCluster) {
        if (nodeToUpdate == nodeId) continue;
        auto &context_ = workerPtr->cctxs[nodeToUpdate];
        auto nodeLeavingRequest = *scalestore::rdma::MessageFabric::createMessage<scalestore::rdma::NodeLeavingUpdateRequest>(
                context_.outgoing, nodeId);
        [[maybe_unused]]auto &nodeLeavingResponse = scalestore::threads::Worker::my().writeMsgSync<scalestore::rdma::NodeLeavingUpdateResponse>(
                nodeToUpdate, nodeLeavingRequest);
    }
}

uint64_t PageIdManager::getFreeSsdSlot(){
    uint64_t  retVal;
    while(true){
        auto chosenPartition = ssdSlotPartitions.find(rand() % numPartitions);
        retVal = chosenPartition->second.getFreeSlotForPage();
        if(retVal != INVALID_SSD_SLOT){
            break;
        }
    }
    return retVal;
}

void PageIdManager::redeemSsdSlot(uint64_t freedSsdSlot){
    auto chosenPartition = ssdSlotPartitions.find(rand() % numPartitions);
    chosenPartition->second.insertFreedSsdSlot(freedSsdSlot);
}

uint64_t PageIdManager::getNewPageId(bool oldRing){
    uint64_t retVal;
    bool lockCheck;
    while(true){
        auto chosenPartition = pageIdIvPartitions.find(rand() % numPartitions);
        lockCheck = chosenPartition->second.pageIdPartitionMtx.try_lock();
        if(lockCheck){
            uint64_t temp = chosenPartition->second.iv;
            temp++;
            while(getNodeIdOfPage(temp  ,oldRing) != nodeId){
                temp++;
            }
            chosenPartition->second.storeIv(temp);
            retVal = temp;
            break;
        }
    }

    return retVal;
}

inline uint64_t PageIdManager::FasterHash(uint64_t input) {
    uint64_t local_rand = input;
    uint64_t local_rand_hash = 8;
    local_rand_hash = 40343 * local_rand_hash + ((local_rand) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 16) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + ((local_rand >> 32) & 0xFFFF);
    local_rand_hash = 40343 * local_rand_hash + (local_rand >> 48);
    local_rand_hash = 40343 * local_rand_hash;
    return local_rand_hash; // if 64 do not rotate
    // else:
    // return Rotr64(local_rand_hash, 56);
}

uint64_t PageIdManager::tripleHash(uint64_t input){
    return FasterHash(FasterHash(FasterHash(input)));
}