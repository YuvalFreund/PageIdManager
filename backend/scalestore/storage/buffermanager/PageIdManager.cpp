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
    initPageIdSets();
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

void PageIdManager::initPageIdSets(){
    for(uint64_t i = 0; i< 65536; i++){ // todo yuval this needs to be parameterized
        pageIdSetPartitions.try_emplace(i);
    }
}

uint64_t PageIdManager::addPage(){
    uint64_t retVal = getNewPageId(isBeforeShuffle);
    uint64_t ssdSlotForNewPage = getFreeSsdSlot();
    pageIdSsdMapMtx.lock();
    pageIdToSsdSlotMap[retVal] = ssdSlotForNewPage;
    pageIdSsdMapMtx.unlock(); // todo yuval - this is of course a performance disaster
    uint64_t pageSet = retVal & PAGE_ID_MASK;

    pageIdSetPartitions[pageSet].addToSet(retVal);
    return retVal;
}

void PageIdManager::removePage(uint64_t pageId){
    pageIdSsdMapMtx.lock();
    uint64_t slotToFree = pageIdToSsdSlotMap[pageId]; //todo yuval - deal with page not found
    pageIdSsdMapMtx.unlock();
    redeemSsdSlot(slotToFree);
    uint64_t pageSet = pageId & PAGE_ID_MASK;
    pageIdSetPartitions[pageSet].removeFromSet(pageId);
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
    pageIdSsdMapMtx.lock();
    retVal = pageIdToSsdSlotMap.at(pageId);
    pageIdSsdMapMtx.unlock(); // todo yuval - this is of course a performance disaster. will be replaced by b-tree
    return retVal;
}


void PageIdManager::prepareForShuffle(uint64_t nodeIdLeft){
    isBeforeShuffle = false;
    nodeIdsInCluster.erase(nodeIdLeft);
    initConsistentHashingInfo(false);
}

PageIdManager::PageShuffleJob PageIdManager::getNextPageShuffleJob(){
    PageShuffleJob retVal(0,0);
    pageIdShuffleMtx.lock();

    while(stackForShuffleJob.empty()){
        workingShuffleSetIdx++;
        if(workingShuffleSetIdx < shuffleSetAmount) {
            retVal.last = true; // done shuffling
            return retVal;
        }
        if(pageIdSetPartitions.find(workingShuffleSetIdx) != pageIdSetPartitions.end()){
            stackForShuffleJob = pageIdSetPartitions[workingShuffleSetIdx].getAllIdsFromSet();
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