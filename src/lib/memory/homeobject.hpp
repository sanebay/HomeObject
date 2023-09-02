#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <set>
#include <utility>

#include <home_replication/repl_service.h>

#include "lib/homeobject_impl.hpp"

namespace homeobject {
struct BlobRoute {
    shard_id shard;
    blob_id blob;
};

inline std::string toString(BlobRoute const& r) { return fmt::format("{}:{}", r.shard, r.blob); }

inline bool operator<(BlobRoute const& lhs, BlobRoute const& rhs) { return toString(lhs) < toString(rhs); }

inline bool operator==(BlobRoute const& lhs, BlobRoute const& rhs) { return toString(lhs) == toString(rhs); }
} // namespace homeobject

template <>
struct std::hash< homeobject::BlobRoute > {
    std::size_t operator()(homeobject::BlobRoute const& r) const noexcept {
        return std::hash< std::string >()(homeobject::toString(r));
    }
};

namespace homeobject {

class MemoryHomeObject : public HomeObjectImpl {
    /// Simulates the append-only disk Chunk in DataSvc
    mutable std::mutex _data_lock;
    std::list< Blob > _in_memory_disk;
    using blkid = decltype(_in_memory_disk)::const_iterator;
    ///

    /// Simulates the Shard=>Chunk mapping in IndexSvc
    mutable std::shared_mutex _index_lock;
    std::unordered_map< BlobRoute, blkid > _in_memory_index;
    ///

    /// Helpers
    // ShardManager
    folly::Future< ShardManager::Result< ShardInfo > > _get_shard(shard_id) const override;
    ShardManager::Result< ShardInfo > _create_shard(pg_id, uint64_t size_bytes) override;
    ShardManager::Result< ShardInfo > _seal_shard(shard_id) override;
    ShardManager::Result< InfoList > _list_shards(pg_id) const override;

    // BlobManager
    BlobManager::Result< blob_id > _put_blob(ShardInfo const&, Blob&&) override;
    BlobManager::Result< Blob > _get_blob(ShardInfo const&, blob_id) const override;
    BlobManager::NullResult _del_blob(ShardInfo const&, blob_id) override;
    ///

public:
    using HomeObjectImpl::HomeObjectImpl;
    ~MemoryHomeObject() override = default;
};

} // namespace homeobject