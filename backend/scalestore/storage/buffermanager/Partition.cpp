#include "Partition.hpp"
namespace scalestore {
namespace storage {
Partition::Partition( u64 freeFramesSize, u64 freePagesSize)
    : frameFreeList(freeFramesSize), pageFreeList(freePagesSize)
{
}
}  // storage
}  // scalestore

