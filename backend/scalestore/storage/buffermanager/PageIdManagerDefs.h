//
// Created by YuvalFreund on 08.02.24.
//

#ifndef SCALESTOREDB_PAGEIDMANAGERDEFS_H
#define SCALESTOREDB_PAGEIDMANAGERDEFS_H


#define INVALID_SSD_SLOT 0xFFFFFFFFFFFFFFFF
#define INVALID_PAGE_ID 0xFFFFFFFFFFFFFFFF
#define INVALID_NODE_ID 0xFFFFFFFFFFFFFFFF
#define PAGE_ID_MASK 0x000000000000FFFF
#define PAGE_AT_OLD_NODE_MASK 0x4000000000000000
#define PAGE_AT_OLD_NODE_MASK_NEGATIVE 0xBFFFFFFFFFFFFFFF
#define PAGE_DIRECTORY_NEGATIVE_MASK 0xFF00FFFFFFFFFFFF
#define CACHED_DIRECTORY_MASK 0x00000000000000FF

#define CONSISTENT_HASHING_WEIGHT 10


#endif //SCALESTOREDB_PAGEIDMANAGERDEFS_H
