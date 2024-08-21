#include "homeobj_fixture.hpp"
#include "lib/homestore_backend/index_kv.hpp"
#include "generated/resync_pg_shard_generated.h"
#include "generated/resync_blob_data_generated.h"

TEST(HomeObject, BasicEquivalence) {
    auto app = std::make_shared< FixtureApp >();
    auto obj_inst = homeobject::init_homeobject(std::weak_ptr< homeobject::HomeObjectApplication >(app));
    ASSERT_TRUE(!!obj_inst);
    auto shard_mgr = obj_inst->shard_manager();
    auto pg_mgr = obj_inst->pg_manager();
    auto blob_mgr = obj_inst->blob_manager();
    EXPECT_EQ(obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(shard_mgr.get()));
    EXPECT_EQ(obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(pg_mgr.get()));
    EXPECT_EQ(obj_inst.get(), dynamic_cast< homeobject::HomeObject* >(blob_mgr.get()));
}

TEST_F(HomeObjectFixture, BasicPutGetDelBlobWRestart) {
    auto num_pgs = SISL_OPTIONS["num_pgs"].as< uint64_t >();
    auto num_shards_per_pg = SISL_OPTIONS["num_shards"].as< uint64_t >() / num_pgs;
    auto num_blobs_per_shard = SISL_OPTIONS["num_blobs"].as< uint64_t >() / num_shards_per_pg;
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    blob_map_t blob_map;

    // Create blob size in range (1, 16kb) and user key in range (1, 1kb)
    const uint32_t max_blob_size = 16 * 1024;

    for (uint64_t i = 1; i <= num_pgs; i++) {
        create_pg(i /* pg_id */);
        for (uint64_t j = 0; j < num_shards_per_pg; j++) {
            auto shard = _obj_inst->shard_manager()->create_shard(i /* pg_id */, 64 * Mi).get();
            ASSERT_TRUE(!!shard);
            pg_shard_id_vec.emplace_back(i, shard->id);
            LOGINFO("pg {} shard {}", i, shard->id);
        }
    }

    // Put blob for all shards in all pg's.
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    // Verify all get blobs
    verify_get_blob(blob_map);

    // Verify the stats
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // for (uint64_t i = 1; i <= num_pgs; i++) {
    //     r_cast< HSHomeObject* >(_obj_inst.get())->print_btree_index(i);
    // }

    LOGINFO("Flushing CP.");
    trigger_cp(true /* wait */);

    // Restart homeobject
    restart();

    // Verify all get blobs after restart
    verify_get_blob(blob_map);

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard, num_shards_per_pg, false /* deleted */);

    // Put blob after restart to test the persistance of blob sequence number
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    // Verify all get blobs with random offset and length.
    verify_get_blob(blob_map, true /* use_random_offset */);

    // Verify the stats after put blobs after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, false /* deleted */);

    // Delete all blobs
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id).get();
        ASSERT_TRUE(g);
        LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
    }

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // Delete again should have no errors.
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->del(shard_id, blob_id).get();
        ASSERT_TRUE(g);
        LOGINFO("delete blob shard {} blob {}", shard_id, blob_id);
    }

    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);

    // After delete all blobs, get should fail
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        ASSERT_TRUE(!g);
    }

    // all the deleted blobs should be tombstone in index table
    auto hs_homeobject = dynamic_cast< HSHomeObject* >(_obj_inst.get());
    for (const auto& [id, blob] : blob_map) {
        int64_t pg_id = std::get< 0 >(id), shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        shared< BlobIndexTable > index_table;
        {
            std::shared_lock lock_guard(hs_homeobject->_pg_lock);
            auto iter = hs_homeobject->_pg_map.find(pg_id);
            ASSERT_TRUE(iter != hs_homeobject->_pg_map.end());
            index_table = static_cast< HSHomeObject::HS_PG* >(iter->second.get())->index_table_;
        }

        auto g = hs_homeobject->get_blob_from_index_table(index_table, shard_id, blob_id);
        ASSERT_FALSE(!!g);
        EXPECT_EQ(BlobError::UNKNOWN_BLOB, g.error());
    }

    LOGINFO("Flushing CP.");
    trigger_cp(true /* wait */);

    // Restart homeobject
    restart();

    // After restart, for all deleted blobs, get should fail
    for (const auto& [id, blob] : blob_map) {
        int64_t shard_id = std::get< 1 >(id), blob_id = std::get< 2 >(id);
        auto g = _obj_inst->blob_manager()->get(shard_id, blob_id).get();
        ASSERT_TRUE(!g);
    }

    // Verify the stats after restart
    verify_obj_count(num_pgs, num_blobs_per_shard * 2, num_shards_per_pg, true /* deleted */);
}

TEST_F(HomeObjectFixture, SealShardWithRestart) {
    // Create a pg, shard, put blob should succeed, seal and put blob again should fail.
    // Recover and put blob again should fail.
    pg_id_t pg_id{1};
    create_pg(pg_id);

    auto s = _obj_inst->shard_manager()->create_shard(pg_id, 64 * Mi).get();
    ASSERT_TRUE(!!s);
    auto shard_info = s.value();
    auto shard_id = shard_info.id;
    s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);

    LOGINFO("Got shard {}", shard_id);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::OPEN);
    auto b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!!b);
    LOGINFO("Put blob {}", b.value());

    s = _obj_inst->shard_manager()->seal_shard(shard_id).get();
    ASSERT_TRUE(!!s);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);
    LOGINFO("Sealed shard {}", shard_id);

    b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!b);
    ASSERT_EQ(b.error(), BlobError::SEALED_SHARD);
    LOGINFO("Put blob {}", b.error());

    // Restart homeobject
    restart();

    // Verify shard is sealed.
    s = _obj_inst->shard_manager()->get_shard(shard_id).get();
    ASSERT_TRUE(!!s);

    LOGINFO("After restart shard {}", shard_id);
    shard_info = s.value();
    EXPECT_EQ(shard_info.id, shard_id);
    EXPECT_EQ(shard_info.placement_group, pg_id);
    EXPECT_EQ(shard_info.state, ShardInfo::State::SEALED);

    b = _obj_inst->blob_manager()->put(shard_id, Blob{sisl::io_blob_safe(512u, 512u), "test_blob", 0ul}).get();
    ASSERT_TRUE(!b);
    ASSERT_EQ(b.error(), BlobError::SEALED_SHARD);
    LOGINFO("Put blob {}", b.error());
}

static uint64_t cur_snapshot_batch_num = 0;

int read_snapshot_data1(homeobject::HSHomeObject* home_object, peer_id_t replica_set_uuid,
                        std::shared_ptr< homestore::snapshot_context > context,
                        std::shared_ptr< homestore::snapshot_data > snp_data) {
    HSHomeObject::PGBlobIterator* pg_iter = nullptr;

    if (snp_data->user_ctx == nullptr) {
        // Create the pg blob iterator for the first time.
        pg_iter = new HSHomeObject::PGBlobIterator(*home_object, replica_set_uuid);
        snp_data->user_ctx = (void*)pg_iter;
    } else {
        pg_iter = r_cast< HSHomeObject::PGBlobIterator* >(snp_data->user_ctx);
    }

    int64_t obj_id = snp_data->offset;
    if (obj_id == 0) {
        // obj_id = 0 means its the first message and we send the pg and its shards metadata.
        cur_snapshot_batch_num = 0;
        pg_iter->create_pg_shard_snapshot_data(snp_data->blob);
        RELEASE_ASSERT(snp_data->blob.size() > 0, "Empty metadata snapshot data");
        return 0;
    }

    // obj_id = shard_seq_num(6 bytes) | batch_number(2 bytes)
    uint64_t shard_seq_num = obj_id >> 16;
    uint64_t batch_number = obj_id & 0xFFFF;
    if (shard_seq_num != pg_iter->cur_shard_seq_num_ || batch_number != cur_snapshot_batch_num) {
        // Validate whats the expected shard_id and batch_num
        LOGE("Shard or batch number mismatch in iterator shard={}/{} batch_num={}/{}", shard_seq_num,
             pg_iter->cur_shard_seq_num_, batch_number, cur_snapshot_batch_num);
        return -1;
    }

    if (pg_iter->end_of_scan()) {
        // No more shards to read, baseline resync is finished after this.
        snp_data->is_last_obj = true;
        return 0;
    }

    // Get next set of blobs in the batch.
    std::vector< HSHomeObject::BlobInfoData > blob_data_vec;
    bool end_of_shard;
    auto result = pg_iter->get_next_blobs(1000, 64 * 1024 * 1024, blob_data_vec, end_of_shard);
    if (result != 0) {
        LOGE("Failed to get next blobs in snapshot read result={}", result);
        return -1;
    }

    // Create snapshot flatbuffer data.
    pg_iter->create_blobs_snapshot_data(blob_data_vec, snp_data->blob, end_of_shard);
    if (end_of_shard) {
        cur_snapshot_batch_num = 0;
    } else {
        cur_snapshot_batch_num++;
    }
    return 0;
}

void write_snapshot_data1(homeobject::HSHomeObject* home_object, std::shared_ptr< homestore::snapshot_context > context,
                          std::shared_ptr< homestore::snapshot_data > snp_data) {
    // LOGE("write_snapshot_data not implemented");
    if (snp_data->is_last_obj) return;
    int64_t obj_id = snp_data->offset;
    if (obj_id == 0) {
        snp_data->offset = 1 << 16;
        auto snp = GetSizePrefixedResyncPGShardInfo(snp_data->blob.bytes());
        PGInfo pg_info{static_cast< pg_id_t >(snp->pg()->pg_id())};
        std::memcpy(&pg_info.replica_set_uuid, snp->pg()->replica_set_uuid()->Data(),
                    snp->pg()->replica_set_uuid()->size());
        for (auto const& m : *(snp->pg()->members())) {
            peer_id_t peer;
            std::string name{m->name()->begin(), m->name()->end()};
            std::memcpy(&peer, m->uuid()->Data(), m->uuid()->size());
            PGMember member{peer, name};
            pg_info.members.insert(std::move(member));
        }

        LOGINFO("write_snapshot_data Creating PG {}", pg_info.id, boost::uuids::to_string(pg_info.replica_set_uuid));
        for (auto& m : pg_info.members) {
            LOGINFO("write_snapshot_data pg members {} {}", boost::uuids::to_string(m.id), m.name);
        }
        auto r = home_object->create_pg(std::move(pg_info)).get();
        RELEASE_ASSERT(!!r, "create pg failed");

        return;
    }

    auto snp = GetSizePrefixedResyncBlobDataBatch(snp_data->blob.bytes());
    for (auto const& b : *(snp->data_array())) {
        Blob blob;
        RELEASE_ASSERT(b->data_size() == b->data()->size(), "size mismatch");
        blob.body = sisl::io_blob_safe{b->data_size()};
        std::memcpy(blob.body.bytes(), b->data()->Data(), b->data()->size());
        blob.user_key = std::string(b->user_key()->begin(), b->user_key()->end());
        LOGINFO("write_snapshot_data put shard {} blob id {} {}", b->shard_id(), b->blob_id(),
                hex_bytes(blob.body.cbytes(), 10));
        auto r = home_object->put(b->shard_id(), std::move(blob)).get();
        RELEASE_ASSERT(!!r, "put blob failed 1");
    }

    uint64_t shard_seq_num = obj_id >> 16;
    uint64_t batch_number = obj_id & 0xFFFF;
    if (snp->end_of_batch()) {
        snp_data->offset = (shard_seq_num + 1) << 16;
    } else {
        snp_data->offset = (shard_seq_num << 16) | (batch_number + 1);
    }
}

TEST_F(HomeObjectFixture, TestLocalWrite) {
    create_pg(1 /* pg_id */);
    auto shard = _obj_inst->shard_manager()->create_shard(1 /* pg_id */, 64 * Mi).get();
    ASSERT_TRUE(!!shard);
    auto shard_id = shard->id;
    homeobject::Blob put_blob{sisl::io_blob_safe(4096 * 2, 4096), {}, 42ul};
    BitsGenerator::gen_random_bits(put_blob.body);
    LOGINFO("Put blob pg {} shard {} data {}", 1, shard_id, hex_bytes(put_blob.body.cbytes(), 128));
    auto b = _obj_inst->blob_manager()->local_put(shard_id, std::move(put_blob)).get();
    if (!b && b.error() == BlobError::NOT_LEADER) {
        LOGINFO("Failed to put blob due to not leader, sleep 1s and retry put", 1, shard_id);
        return;
    }

    auto blob_id = b.value();

    auto g = _obj_inst->blob_manager()->get(shard_id, blob_id, 0, 0).get();
    ASSERT_TRUE(!!g);
    auto result = std::move(g.value());
    LOGINFO("Get blob pg {} shard {} blob {} data {}", 1, shard_id, blob_id, hex_bytes(result.body.cbytes(), 128));
}

TEST_F(HomeObjectFixture, PGBlobIterator) {
    uint64_t num_shards_per_pg = 3;
    uint64_t num_blobs_per_shard = 5;
    std::vector< std::pair< pg_id_t, shard_id_t > > pg_shard_id_vec;
    blob_map_t blob_map;

    // Create blob size in range (1, 16kb) and user key in range (1, 1kb)
    const uint32_t max_blob_size = 16 * 1024;

    create_pg(1 /* pg_id */);
    for (uint64_t j = 0; j < num_shards_per_pg; j++) {
        auto shard = _obj_inst->shard_manager()->create_shard(1 /* pg_id */, 64 * Mi).get();
        ASSERT_TRUE(!!shard);
        pg_shard_id_vec.emplace_back(1, shard->id);
        LOGINFO("pg {} shard {}", 1, shard->id);
    }

    // Put blob for all shards in all pg's.
    put_blob(blob_map, pg_shard_id_vec, num_blobs_per_shard, max_blob_size);

    auto ho = dynamic_cast< homeobject::HSHomeObject* >(_obj_inst.get());
    PG* pg1;
    {
        auto lg = std::shared_lock(ho->_pg_lock);
        auto iter = ho->_pg_map.find(1);
        ASSERT_TRUE(iter != ho->_pg_map.end());
        pg1 = iter->second.get();
    }

    auto pg1_iter = std::make_shared< homeobject::HSHomeObject::PGBlobIterator >(*ho, pg1->pg_info_.replica_set_uuid);
    ASSERT_EQ(pg1_iter->end_of_scan(), false);

    // Verify PG shard meta data.
    sisl::io_blob_safe meta_blob;
    pg1_iter->create_pg_shard_snapshot_data(meta_blob);
    ASSERT_TRUE(meta_blob.size() > 0);

    auto pg_req = GetSizePrefixedResyncPGShardInfo(meta_blob.bytes());
    ASSERT_EQ(pg_req->pg()->pg_id(), pg1->pg_info_.id);
    auto u1 = pg_req->pg()->replica_set_uuid();
    auto u2 = pg1->pg_info_.replica_set_uuid;
    ASSERT_EQ(std::string(u1->begin(), u1->end()), std::string(u2.begin(), u2.end()));
    {
        auto i = pg_req->pg()->members()->begin();
        auto j = pg1->pg_info_.members.begin();
        for (; i != pg_req->pg()->members()->end() && j != pg1->pg_info_.members.end(); i++, j++) {
            ASSERT_EQ(std::string(i->uuid()->begin(), i->uuid()->end()), std::string(j->id.begin(), j->id.end()));
        }
    }

    // Verify get blobs for pg.
    uint64_t max_num_blobs_in_batch = 3, max_batch_size_bytes = 128 * Mi;
    std::vector< HSHomeObject::BlobInfoData > blob_data_vec;
    while (!pg1_iter->end_of_scan()) {
        std::vector< HSHomeObject::BlobInfoData > vec;
        bool end_of_shard;
        auto result = pg1_iter->get_next_blobs(max_num_blobs_in_batch, max_batch_size_bytes, vec, end_of_shard);
        ASSERT_EQ(result, 0);
        for (auto& v : vec) {
            blob_data_vec.push_back(std::move(v));
        }
    }

    ASSERT_EQ(blob_data_vec.size(), num_shards_per_pg * num_blobs_per_shard);
    for (auto& b : blob_data_vec) {
        auto g = _obj_inst->blob_manager()->get(b.shard_id, b.blob_id, 0, 0).get();
        ASSERT_TRUE(!!g);
        auto result = std::move(g.value());
        LOGINFO("Get blob pg {} shard {} blob {} len {} data {}", 1, b.shard_id, b.blob_id, b.blob.body.size(),
                hex_bytes(result.body.cbytes(), 5));
        EXPECT_EQ(result.body.size(), b.blob.body.size());
        EXPECT_EQ(std::memcmp(result.body.bytes(), b.blob.body.cbytes(), result.body.size()), 0);
        EXPECT_EQ(result.user_key.size(), b.blob.user_key.size());
        EXPECT_EQ(result.user_key, b.blob.user_key);
        EXPECT_EQ(result.object_off, b.blob.object_off);
    }

    class tmp_context : public homestore::snapshot_context {
    public:
        tmp_context(int64_t lsn) : homestore::snapshot_context(lsn) {}
        virtual void deserialize(const sisl::io_blob_safe& snp_ctx) {};
        virtual sisl::io_blob_safe serialize() { return {}; };
        int64_t get_lsn() { return 0; }
    };

#if 0
    auto context = std::make_shared< tmp_context >(0);
    auto snapshot_data = std::make_shared< homestore::snapshot_data >();
    while (!snapshot_data->is_last_obj) {
        read_snapshot_data1(ho, pg1->pg_info_.replica_set_uuid, context, snapshot_data);
        write_snapshot_data1(ho, context, snapshot_data);
    }

    delete r_cast< HSHomeObject::PGBlobIterator* >(snapshot_data->user_ctx);
#endif
}
