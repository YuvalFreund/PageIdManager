//
// Created by YuvalFreund on 08.02.24.
//

#ifndef SCALESTOREDB_PAGEIDMANAGER_H
#define SCALESTOREDB_PAGEIDMANAGER_H

#include <map>
#include <stack>
#include <vector>
#include <mutex>
#include <set>
#include <atomic>
#include <algorithm>

#include "PageIdManagerDefs.h"
#include "Defs.hpp"
#include "scalestore/threads/Worker.hpp"
#include "scalestore/utils/FNVHash.hpp"
struct PageIdManager {

    // helper structs


    struct FreeSsdSlotPartition{
        uint64_t begin;
        std::stack<uint64_t> freeSlots;
        uint64_t partitionSize;
        std::mutex partitionLock;
        FreeSsdSlotPartition(uint64_t begin, uint64_t partitionSize) : begin(begin), partitionSize(partitionSize) {
            for(uint64_t i = 0; i<partitionSize; i++){
                this->freeSlots.push(begin + i);
            }
        }

        uint64_t getFreeSlotForPage(){
            uint64_t retVal;
            partitionLock.lock();
            if(freeSlots.empty()){
                retVal = INVALID_SSD_SLOT;
            }else{
                retVal = freeSlots.top();
                freeSlots.pop();
            }
            partitionLock.unlock();
            return retVal;
        }

        void insertFreedSsdSlot(uint64_t freedSsdSlot){
            partitionLock.lock();
            freeSlots.push(freedSsdSlot);
            partitionLock.unlock();
        }
    };

    struct PageShuffleJob{
        uint64_t pageId;
        uint64_t newNodeId;
        bool last = false;
        PageShuffleJob(uint64_t pageId, uint64_t newNodeId) : pageId(pageId), newNodeId(newNodeId) {}

    };
    struct FreePageIdPartition{
        // todo yuval -later switch to optimisitc locking
        std::mutex pageIdPartitionMtx;
        std::uint64_t guess;
        explicit FreePageIdPartition(uint64_t guess) : guess(guess){}

        void storeGuess( uint64_t newGuess){
            guess = newGuess;
            pageIdPartitionMtx.unlock();
        }
    };
    struct SsdSlotMapPartition{
        std::unordered_map<uint64_t, uint64_t> map;
        std::mutex partitionLock;

        void insertToMap(uint64_t pageId, uint64_t ssdSlot){
            partitionLock.lock();
            map[pageId] = ssdSlot;
            partitionLock.unlock();
        }

        void setDirectoryForPage(uint64_t pageId, uint64_t directory){
            partitionLock.lock();
            auto iter = map.find(pageId);
            ensure(iter != map.end());
            uint64_t directoryIn64Bit = directory;
            directoryIn64Bit <<= 56;
            uint64_t newValueForMap = map[pageId];
            newValueForMap &= PAGE_DIRECTORY_NEGATIVE_MASK;
            newValueForMap |= directoryIn64Bit;
            map[pageId] = newValueForMap;
            partitionLock.unlock();
        }

        uint64_t getDirectoryOfPage(uint64_t pageId){
            uint64_t retVal = INVALID_NODE_ID;
            partitionLock.lock();
            auto iter = map.find(pageId);
            if(iter != map.end()){
                retVal = iter->second;
                retVal &= CACHED_DIRECTORY_MASK;
                retVal >>= 56;
            }else{
                retVal = INVALID_NODE_ID;
            }
            partitionLock.unlock();
            return retVal;
        }

        uint64_t getSsdSlotOfPage(uint64_t pageId){
            uint64_t retVal;
            partitionLock.lock();
            auto iter = map.find(pageId);
            ensure(iter != map.end());
            retVal = iter->second;
            retVal &= SSD_SLOT_MASK;
            ensure(retVal < 26214401);
            partitionLock.unlock();
            return retVal;
        }

        std::stack<uint64_t> getStackForShuffling(){
            partitionLock.lock();
            std::stack<uint64_t> retVal;
            for(auto pair : map){
                retVal.push(pair.first);
            }
            partitionLock.unlock();
            return retVal;
        }

        uint64_t getSsdSlotOfPageAndRemove(uint64_t pageId){
            uint64_t retVal;
            partitionLock.lock();
            auto iter = map.find(pageId);
            ensure(iter != map.end());
            retVal = iter->second;
            retVal &= SSD_SLOT_MASK;
            map.erase(iter);
            partitionLock.unlock();
            return retVal;
        }

    };
    //constructor
    PageIdManager(uint64_t nodeId, const std::vector<uint64_t>& nodeIdsInput) : nodeId(nodeId){
        nodeIdAtMSB = nodeId;
        nodeIdAtMSB <<= 56;
        for(auto node: nodeIdsInput){
            nodeIdsInCluster.insert(node);
        }
        initPageIdManager();
    }

    // data structures for mapping
    std::unordered_map<uint64_t, FreeSsdSlotPartition> freeSsdSlotPartitions;
    std::unordered_map<uint64_t, FreePageIdPartition> pageIdIvPartitions;
    SsdSlotMapPartition pageIdToSsdSlotMap[65536] = {};

    // constants
    uint64_t numPartitions;
    uint64_t nodeId;
    int ShuffleMapAmount = 65536; // todo yuval -this needs to be parameterized
    uint64_t nodeIdAtMSB;

    // consistent hashing data
    std::set<uint64_t> nodeIdsInCluster;
    std::map<uint64_t, uint64_t> nodesRingLocationMap;
    std::vector<uint64_t> nodeRingLocationsVector;
    uint64_t nodeRingLocationsArray[100] = { 0 };
    std::map<uint64_t, uint64_t> newNodesRingLocationMap;
    std::vector<uint64_t> newNodeRingLocationsVector;
    uint64_t newNodeRingLocationsArray[100] = { 0 };


    //locks and atomics
    std::atomic<bool> isBeforeShuffle = true;
    std::atomic<int> workingShuffleMapIdx = 0;
    std::atomic<bool> shuffleDone = false;
    std::mutex pageIdSsdMapMtx;
    std::mutex pageIdShuffleMtx;

    //shuffling
    std::stack<uint64_t> stackForShuffleJob;

    // for page provider
    uint64_t nodeLeaving;

    // init functions
    void initPageIdManager();
    void initSsdPartitions();
    void initConsistentHashingInfo(bool firstInit);
    void initPageIdIvs();
    void initPageIdToSsdSlotMaps();


    // page id manager normal functionalities
    uint64_t addPage();
    void removePage(uint64_t pageId);
    uint64_t getUpdatedNodeIdOfPage(uint64_t pageId, bool searchOldRing);
    uint64_t getSsdSlotOfPageId(uint64_t pageId);
    void addPageWithExistingPageId(uint64_t existingPageId);

    // shuffling functions
    void prepareForShuffle(uint64_t nodeIdLeft);
    PageShuffleJob getNextPageShuffleJob();
    void pushJobToStack(uint64_t pageId);
    uint64_t getCachedDirectoryOfPage(uint64_t pageId);
    void setDirectoryOfPage(uint64_t pageId, uint64_t directory);
    bool isNodeDirectoryOfPageId(uint64_t pageId);
    uint64_t getTargetNodeForEviction(uint64_t pageId);


    // shuffling message
    void gossipNodeIsLeaving( scalestore::threads::Worker* workerPtr );

    //helper functions
    void redeemSsdSlot(uint64_t freedSsdSlot);
    uint64_t getNewPageId(bool oldRing);
    uint64_t getFreeSsdSlot();
    uint64_t searchRingForNode(uint64_t pageId, bool searchOldRing);

};




#endif //SCALESTOREDB_PAGEIDMANAGER_H
