//
// Created by YuvalFreund on 17.03.24.
//
#pragma once
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <cassert>

#ifndef SCALESTOREDB_DEBUGPAGE_H
#define SCALESTOREDB_DEBUGPAGE_H
namespace scalestore
{
    namespace storage
    {
// -------------------------------------------------------------------------------------

        constexpr uint64_t PAGE_SIZE = 1024 * 4;
        struct DebugData{
            uint64_t createdAtNodeId;
            uint64_t shuffledToNodeId;
        };
// -------------------------------------------------------------------------------------
        struct alignas(512) DebugPage {
            volatile size_t magicDebuggingNumber = 999; // INIT used for RDMA debugging
            uint8_t data[PAGE_SIZE - sizeof(magicDebuggingNumber)];
            uint8_t* begin() { return data; }
            uint8_t* end() { return data + (PAGE_SIZE - sizeof(magicDebuggingNumber)) + 1; } // one byte past the end
        };
// -------------------------------------------------------------------------------------
        static constexpr uint64_t EFFECTIVE_PAGE_SIZE = sizeof(DebugPage::data);
// -------------------------------------------------------------------------------------
    }  // namespace storage

}  // namespace scalestore



#endif //SCALESTOREDB_DEBUGPAGE_H
