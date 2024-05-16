package quai

import (
	"math/big"
	"math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/core"
	"github.com/dominant-strategies/go-quai/core/types"
	"github.com/dominant-strategies/go-quai/event"
	"github.com/dominant-strategies/go-quai/log"
	lru "github.com/hnlq715/golang-lru"
)

const (
	c_missingBlockChanSize          = 60
	c_checkNextPrimeBlockInterval   = 60 * time.Second
	c_provideMessageInterval        = 2 * time.Minute
	c_newTxsChanSize                = 1000
	c_recentBlockReqCache           = 1000
	c_recentBlockReqTimeout         = 1 * time.Minute
	c_broadcastTransactionsInterval = 2 * time.Second
	c_maxTxBatchSize                = 100
)

type handler struct {
	nodeLocation        common.Location
	p2pBackend          NetworkingAPI
	core                *core.Core
	missingBlockCh      chan types.BlockRequest
	missingBlockSub     event.Subscription
	txsCh               chan core.NewTxsEvent
	txsSub              event.Subscription
	wg                  sync.WaitGroup
	quitCh              chan struct{}
	logger              *log.Logger
	recentBlockReqCache *lru.Cache
	closeOnce           sync.Once
}

func newHandler(p2pBackend NetworkingAPI, core *core.Core, nodeLocation common.Location, logger *log.Logger) *handler {
	handler := &handler{
		nodeLocation: nodeLocation,
		p2pBackend:   p2pBackend,
		core:         core,
		quitCh:       make(chan struct{}),
		logger:       logger,
	}
	handler.recentBlockReqCache, _ = lru.NewWithExpire(c_recentBlockReqCache, c_recentBlockReqTimeout)
	return handler
}

func (h *handler) Start() {
	h.wg.Add(1)
	h.missingBlockCh = make(chan types.BlockRequest, c_missingBlockChanSize)
	h.missingBlockSub = h.core.SubscribeMissingBlockEvent(h.missingBlockCh)
	go h.missingBlockLoop()

	nodeCtx := h.nodeLocation.Context()
	if nodeCtx == common.ZONE_CTX && h.core.ProcessingState() {
		h.wg.Add(1)
		h.txsCh = make(chan core.NewTxsEvent, c_newTxsChanSize)
		h.txsSub = h.core.SubscribeNewTxsEvent(h.txsCh)
		go h.txBroadcastLoop()
	}

	if nodeCtx == common.PRIME_CTX {
		h.wg.Add(1)
		go h.checkNextPrimeBlock()
	}

	h.wg.Add(1)
	go h.broadcastProvideMessage()
}

func (h *handler) Stop() {
	h.closeOnce.Do(func() {
		h.missingBlockSub.Unsubscribe()
		nodeCtx := h.nodeLocation.Context()
		if nodeCtx == common.ZONE_CTX && h.core.ProcessingState() {
			h.txsSub.Unsubscribe()
		}
		close(h.quitCh)
		h.wg.Wait()
	})
}

func (h *handler) missingBlockLoop() {
	defer h.wg.Done()
	defer recoverPanic("missingBlockLoop", h.logger)

	for {
		select {
		case blockRequest := <-h.missingBlockCh:
			if _, exists := h.recentBlockReqCache.Get(blockRequest.Hash); !exists {
				h.recentBlockReqCache.Add(blockRequest.Hash, true)
			} else {
				continue
			}

			go func() {
				defer recoverPanic("requestBlock", h.logger)
				h.logger.WithFields(log.Fields{
					"hash":    blockRequest.Hash,
					"entropy": blockRequest.Entropy,
				}).Info("Requesting block from peers")
				resultCh := h.p2pBackend.Request(h.nodeLocation, blockRequest.Hash, &types.WorkObject{})
				block := <-resultCh
				if block != nil {
					h.core.WriteBlock(block.(*types.WorkObject))
					h.logger.WithFields(log.Fields{
						"block hash": block.(*types.WorkObject).Hash,
						"number":     block.(*types.WorkObject).Number,
					}).Info("Block returned from peer")
				}
				h.logger.WithFields(log.Fields{
					"other": nil,
				}).Info("other than block returned from peer")
			}()
		case <-h.missingBlockSub.Err():
			return
		case <-h.quitCh:
			return
		}
	}
}

func (h *handler) txBroadcastLoop() {
	defer h.wg.Done()
	defer recoverPanic("txBroadcastLoop", h.logger)

	transactions := make(types.Transactions, 0, c_maxTxBatchSize)
	broadcastTransactionsTicker := time.NewTicker(c_broadcastTransactionsInterval)
	defer broadcastTransactionsTicker.Stop()

	for {
		select {
		case event := <-h.txsCh:
			transactions = append(transactions, event.Txs...)
			if len(transactions) >= c_maxTxBatchSize {
				h.broadcastTransactions(transactions[:c_maxTxBatchSize])
				transactions = transactions[c_maxTxBatchSize:]
			}
		case <-broadcastTransactionsTicker.C:
			if len(transactions) > 0 {
				h.broadcastTransactions(transactions)
				transactions = make(types.Transactions, 0, c_maxTxBatchSize)
			}
		case <-h.txsSub.Err():
			return
		case <-h.quitCh:
			return
		}
	}
}

func (h *handler) broadcastTransactions(transactions types.Transactions) {
	if len(transactions) == 0 {
		return
	}
	if err := h.p2pBackend.Broadcast(h.nodeLocation, &transactions); err != nil {
		h.logger.Errorf("Error broadcasting transactions: %+v", err)
	}
}

func (h *handler) checkNextPrimeBlock() {
	defer h.wg.Done()
	defer recoverPanic("checkNextPrimeBlock", h.logger)

	checkNextPrimeBlockTimer := time.NewTicker(c_checkNextPrimeBlockInterval)
	defer checkNextPrimeBlockTimer.Stop()

	for {
		select {
		case <-checkNextPrimeBlockTimer.C:
			currentHeight := h.core.CurrentHeader().Number(h.nodeLocation.Context())
			for i := int64(0); i < 3; i++ {
				h.GetNextPrimeBlock(new(big.Int).Add(currentHeight, big.NewInt(i)))
			}
		case <-h.quitCh:
			return
		}
	}
}

func (h *handler) GetNextPrimeBlock(number *big.Int) {
	go func() {
		defer recoverPanic("GetNextPrimeBlock", h.logger)
		resultCh := h.p2pBackend.Request(h.nodeLocation, number, common.Hash{})
		data := <-resultCh
		if data != nil {
			blockHash, ok := data.(common.Hash)
			if ok {
				block := h.core.GetBlockByHash(blockHash)
				if block == nil {
					resultCh := h.p2pBackend.Request(h.nodeLocation, blockHash, &types.WorkObject{})
					block := <-resultCh
					if block != nil {
						h.core.WriteBlock(block.(*types.WorkObject))
					}
				}
			}
		}
	}()
}

func (h *handler) broadcastProvideMessage() {
	defer h.wg.Done()
	defer recoverPanic("broadcastProvideMessage", h.logger)

	provideTimer := time.NewTicker(c_provideMessageInterval)
	defer provideTimer.Stop()

	for {
		select {
		case <-provideTimer.C:
			time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
			h.p2pBackend.Broadcast(h.nodeLocation, &types.ProvideTopic{Topic: 0})
			if h.core.NodeCtx() == common.ZONE_CTX {
				h.p2pBackend.Broadcast(h.nodeLocation, &types.ProvideTopic{Topic: 1})
			}
		case <-h.quitCh:
			return
		}
	}
}

func recoverPanic(functionName string, logger *log.Logger) {
	if r := recover(); r != nil {
		logger.WithFields(log.Fields{
			"function":   functionName,
			"error":      r,
			"stacktrace": string(debug.Stack()),
		}).Fatal("Go-Quai Panicked")
	}
}
