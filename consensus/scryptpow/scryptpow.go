package scryptpow

import (
	"math/big"
	"math/rand"
	"sync"
	"time"

	"github.com/dominant-strategies/go-quai/cmd/genallocs"
	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/log"
)

var (
	// sharedScryptpow is a full instance that can be shared between multiple users.
	sharedScryptpow *Scryptpow
)

// Mode defines the type and amount of PoW verification a scryptpow engine makes.
type Mode uint

const (
	ModeNormal Mode = iota
	ModeShared
	ModeTest
	ModeFake
	ModeFullFake
)

// Config are the configuration parameters of the scryptpow.
type Config struct {
	PowMode Mode

	DurationLimit *big.Int

	GasCeil uint64

	GenAllocs []genallocs.GenesisAccount

	NodeLocation common.Location

	MinDifficulty *big.Int

	WorkShareThreshold int

	// When set, notifications sent by the remote sealer will
	// be block header JSON objects instead of work package arrays.
	NotifyFull bool

	Log *log.Logger `toml:"-"`
	// Number of threads to mine on if mining
	NumThreads int
}

// Scryptpow is a proof-of-work consensus engine using the scrypt hash algorithm
type Scryptpow struct {
	config Config

	// Mining related fields
	rand    *rand.Rand    // Properly seeded random source for nonces
	threads int           // Number of threads to mine on if mining
	update  chan struct{} // Notification channel to update mining parameters

	// The fields below are hooks for testing
	shared    *Scryptpow    // Shared PoW verifier to avoid cache regeneration
	fakeFail  uint64        // Block number which fails PoW check even in fake mode
	fakeDelay time.Duration // Time delay to sleep for before returning from verify

	lock      sync.Mutex // Ensures thread safety for the in-memory caches and mining fields
	closeOnce sync.Once  // Ensures exit channel will not be closed twice.

	logger *log.Logger
}

// New creates a full sized scryptpow PoW scheme and starts a background thread for
// remote mining, also optionally notifying a batch of remote services of new work
// packages.
func New(config Config, notify []string, noverify bool, logger *log.Logger) *Scryptpow {
	scryptpow := &Scryptpow{
		config:  config,
		update:  make(chan struct{}),
		logger:  logger,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
		threads: config.NumThreads,
	}
	if config.PowMode == ModeShared {
		scryptpow.shared = sharedScryptpow
	}
	return scryptpow
}

// NewTester creates a small sized scryptpow PoW scheme useful only for testing
// purposes.
func NewTester(notify []string, noverify bool) *Scryptpow {
	return New(Config{PowMode: ModeTest}, notify, noverify, log.NewLogger("test-scryptpow.log", "info", 500))
}

// NewFaker creates a scryptpow consensus engine with a fake PoW scheme that accepts
// all blocks' seal as valid, though they still have to conform to the Quai
// consensus rules.
func NewFaker() *Scryptpow {
	return &Scryptpow{
		config: Config{
			PowMode: ModeFake,
		},
	}
}

// NewFakeFailer creates a scryptpow consensus engine with a fake PoW scheme that
// accepts all blocks as valid apart from the single one specified, though they
// still have to conform to the Quai consensus rules.
func NewFakeFailer(fail uint64) *Scryptpow {
	return &Scryptpow{
		config: Config{
			PowMode: ModeFake,
		},
		fakeFail: fail,
	}
}

// NewFakeDelayer creates a scryptpow consensus engine with a fake PoW scheme that
// accepts all blocks as valid, but delays verifications by some time, though
// they still have to conform to the Quai consensus rules.
func NewFakeDelayer(delay time.Duration) *Scryptpow {
	return &Scryptpow{
		config: Config{
			PowMode: ModeFake,
		},
		fakeDelay: delay,
	}
}

// NewFullFaker creates an scryptpow consensus engine with a full fake scheme that
// accepts all blocks as valid, without checking any consensus rules whatsoever.
func NewFullFaker() *Scryptpow {
	return &Scryptpow{
		config: Config{
			PowMode: ModeFullFake,
		},
	}
}

// NewShared creates a full sized scryptpow PoW shared between all requesters running
// in the same process.
func NewShared() *Scryptpow {
	return &Scryptpow{shared: sharedScryptpow}
}

// Threads returns the number of mining threads currently enabled. This doesn't
// necessarily mean that mining is running!
func (scryptpow *Scryptpow) Threads() int {
	scryptpow.lock.Lock()
	defer scryptpow.lock.Unlock()

	return scryptpow.threads
}

// SetThreads updates the number of mining threads currently enabled. Calling
// this method does not start mining, only sets the thread count. If zero is
// specified, the miner will use all cores of the machine. Setting a thread
// count below zero is allowed and will cause the miner to idle, without any
// work being done.
func (scryptpow *Scryptpow) SetThreads(threads int) {
	scryptpow.lock.Lock()
	defer scryptpow.lock.Unlock()

	if scryptpow.shared != nil {
		// If we're running a shared PoW, set the thread count on that instead
		scryptpow.shared.SetThreads(threads)
	} else {
		// Update the threads and ping any running seal to pull in any changes
		scryptpow.threads = threads
		select {
		case scryptpow.update <- struct{}{}:
		default:
		}
	}
}