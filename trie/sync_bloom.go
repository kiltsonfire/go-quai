// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	bloomfilter "github.com/holiman/bloomfilter/v2"

	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/core/rawdb"
	"github.com/dominant-strategies/go-quai/ethdb"
	"github.com/dominant-strategies/go-quai/log"
)

// SyncBloom is a bloom filter used during fast sync to quickly decide if a trie
// node or contract code already exists on disk or not. It self populates from the
// provided disk database on creation in a background thread and will only start
// returning live results once that's finished.
type SyncBloom struct {
	bloom   *bloomfilter.Filter
	inited  uint32
	closer  sync.Once
	closed  uint32
	pend    sync.WaitGroup
	closeCh chan struct{}
}

// NewSyncBloom creates a new bloom filter of the given size (in megabytes) and
// initializes it from the database. The bloom is hard coded to use 3 filters.
func NewSyncBloom(memory uint64, database ethdb.Iteratee) *SyncBloom {
	// Create the bloom filter to track known trie nodes
	bloom, err := bloomfilter.New(memory*1024*1024*8, 4)
	if err != nil {
		panic(fmt.Sprintf("failed to create bloom: %v", err))
	}
	log.Global.WithField("size", common.StorageSize(memory*1024*1024)).Info("Allocated fast sync bloom")

	// Assemble the fast sync bloom and init it from previous sessions
	b := &SyncBloom{
		bloom:   bloom,
		closeCh: make(chan struct{}),
	}
	b.pend.Add(2)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Global.WithFields(log.Fields{
					"error":      r,
					"stacktrace": string(debug.Stack()),
				}).Fatal("Go-Quai Panicked")
			}
		}()
		defer b.pend.Done()
		b.init(database)
	}()
	return b
}

// init iterates over the database, pushing every trie hash into the bloom filter.
func (b *SyncBloom) init(database ethdb.Iteratee) {
	// Iterate over the database, but restart every now and again to avoid holding
	// a persistent snapshot since fast sync can push a ton of data concurrently,
	// bloating the disk.
	//
	// Note, this is fine, because everything inserted into leveldb by fast sync is
	// also pushed into the bloom directly, so we're not missing anything when the
	// iterator is swapped out for a new one.
	it := database.NewIterator(nil, nil)

	var (
		start = time.Now()
		swap  = time.Now()
	)
	for it.Next() && atomic.LoadUint32(&b.closed) == 0 {
		// If the database entry is a trie node, add it to the bloom
		key := it.Key()
		if len(key) == common.HashLength {
			b.bloom.AddHash(binary.BigEndian.Uint64(key))
		} else if ok, hash := rawdb.IsCodeKey(key); ok {
			// If the database entry is a contract code, add it to the bloom
			b.bloom.AddHash(binary.BigEndian.Uint64(hash))
		}
		// If enough time elapsed since the last iterator swap, restart
		if time.Since(swap) > 8*time.Second {
			key := common.CopyBytes(it.Key())

			it.Release()
			it = database.NewIterator(nil, key)

			log.Global.WithFields(log.Fields{
				"items":     b.bloom.N(),
				"errorrate": b.bloom.FalsePosititveProbability(),
				"elapsed":   common.PrettyDuration(time.Since(start)),
			}).Info("Initializing state bloom")
			swap = time.Now()
		}
	}
	it.Release()

	// Mark the bloom filter inited and return
	log.Global.WithFields(log.Fields{
		"items":     b.bloom.N(),
		"errorrate": b.bloom.FalsePosititveProbability(),
		"elapsed":   common.PrettyDuration(time.Since(start)),
	}).Info("Initialized state bloom")
	atomic.StoreUint32(&b.inited, 1)
}

// Close terminates any background initializer still running and releases all the
// memory allocated for the bloom.
func (b *SyncBloom) Close() error {
	b.closer.Do(func() {
		// Ensure the initializer is stopped
		atomic.StoreUint32(&b.closed, 1)
		close(b.closeCh)
		b.pend.Wait()

		// Wipe the bloom, but mark it "uninited" just in case someone attempts an access
		log.Global.WithFields(log.Fields{
			"items":     b.bloom.N(),
			"errorrate": b.bloom.FalsePosititveProbability(),
		}).Info("Deallocating state bloom")

		atomic.StoreUint32(&b.inited, 0)
		b.bloom = nil
	})
	return nil
}

// Add inserts a new trie node hash into the bloom filter.
func (b *SyncBloom) Add(hash []byte) {
	if atomic.LoadUint32(&b.closed) == 1 {
		return
	}
	b.bloom.AddHash(binary.BigEndian.Uint64(hash))
}

// Contains tests if the bloom filter contains the given hash:
//   - false: the bloom definitely does not contain hash
//   - true:  the bloom maybe contains hash
//
// While the bloom is being initialized, any query will return true.
func (b *SyncBloom) Contains(hash []byte) bool {
	if atomic.LoadUint32(&b.inited) == 0 {
		// We didn't load all the trie nodes from the previous run of Quai yet. As
		// such, we can't say for sure if a hash is not present for anything. Until
		// the init is done, we're faking "possible presence" for everything.
		return true
	}
	// Bloom initialized, check the real one and report any successful misses
	maybe := b.bloom.ContainsHash(binary.BigEndian.Uint64(hash))
	return maybe
}
