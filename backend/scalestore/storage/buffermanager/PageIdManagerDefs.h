//
// Created by YuvalFreund on 08.02.24.
//

#ifndef SCALESTOREDB_PAGEIDMANAGERDEFS_H
#define SCALESTOREDB_PAGEIDMANAGERDEFS_H


#define INVALID_SSD_SLOT 0xFFFFFFFFFFFFFFFF
#define INVALID_PAGE_ID 0xFFFFFFFFFFFFFFFF
#define INVALID_NODE_ID 0xFFFFFFFFFFFFFFFF
#define PAGE_ID_MASK 0x000000000000FFFF
#define PAGE_AT_OLD_NODE_MASK 0xFF00000000000000
#define SSD_SLOT_MASK 0X0000FFFFFFFFFFFF
#define PAGE_AT_OLD_NODE_SET 0x0100000000000000
#define PAGE_AT_OLD_NODE_MASK_NEGATIVE 0x00FFFFFFFFFFFFFF
#define PAGE_DIRECTORY_NEGATIVE_MASK 0xFF00FFFFFFFFFFFF
#define CACHED_DIRECTORY_MASK 0x00FF000000000000

#define CONSISTENT_HASHING_WEIGHT 10


#endif //SCALESTOREDB_PAGEIDMANAGERDEFS_H
