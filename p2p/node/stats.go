package node

import (
	"runtime/debug"
	"time"

	"github.com/dominant-strategies/go-quai/log"
)

// connectionStats returns the number of peers in the routing table, as well as the number of active connections.
func (p *P2PNode) connectionStats() int {
	peers := p.peerManager.GetHost().Network().Peers()
	return len(peers)
}

// logConnectionStats logs the current number of connected peers.
func (p *P2PNode) logConnectionStats() {
	peersConnected := p.connectionStats()
	log.Global.Debugf("Number of peers connected: %d", peersConnected)
}

// statsLoop periodically logs the connection stats.
func (p *P2PNode) statsLoop() {
	defer func() {
		if r := recover(); r != nil {
			p.quitCh <- struct{}{}
			log.Global.WithFields(log.Fields{
				"error":      r,
				"stacktrace": string(debug.Stack()),
			}).Error("Go-Quai Panicked")
		}
	}()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.logConnectionStats()
		case <-p.ctx.Done():
			log.Global.Warnf("Context cancelled. Stopping stats loop...")
			return
		}
	}
}
