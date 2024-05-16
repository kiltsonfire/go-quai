package node

import (
	"math/big"
	"runtime/debug"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"

	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/core/types"
	"github.com/dominant-strategies/go-quai/log"
	"github.com/dominant-strategies/go-quai/p2p/node/peerManager"
	"github.com/dominant-strategies/go-quai/p2p/node/requestManager"
	"github.com/dominant-strategies/go-quai/p2p/pb"
	"github.com/dominant-strategies/go-quai/trie"
)

// requestFromPeer opens a stream to the given peer and requests data for the given hash at the given location.
func (p *P2PNode) requestFromPeer(peerID peer.ID, location common.Location, data interface{}, datatype interface{}) (interface{}, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Global.WithFields(log.Fields{
				"error":      r,
				"stacktrace": string(debug.Stack()),
			}).Error("Go-Quai Panicked")
		}
	}()

	log.Global.WithFields(log.Fields{
		"peerId":   peerID,
		"location": location.Name(),
		"data":     data,
		"datatype": datatype,
	}).Info("Requesting the data from peer")

	stream, err := p.NewStream(peerID)
	if err != nil {
		log.Global.WithFields(log.Fields{
			"peerId": peerID,
			"error":  err,
		}).Error("Failed to open stream to peer")
		return nil, err
	}
	defer stream.Close()

	id := p.requestManager.CreateRequest()
	defer p.requestManager.CloseRequest(id)

	requestBytes, err := pb.EncodeQuaiRequest(id, location, data, datatype)
	if err != nil {
		return nil, err
	}

	err = p.GetPeerManager().WriteMessageToStream(peerID, stream, requestBytes)
	if err != nil {
		return nil, err
	}

	dataChan, err := p.requestManager.GetRequestChan(id)
	if err != nil {
		return nil, err
	}

	var recvdType interface{}
	select {
	case recvdType = <-dataChan:
	case <-time.After(requestManager.C_requestTimeout):
		log.Global.WithFields(log.Fields{
			"peerId": peerID,
		}).Warn("Peer did not respond in time")
		p.peerManager.MarkUnresponsivePeer(peerID, location)
		return nil, errors.New("peer did not respond in time")
	}

	p.GetPeerManager().ClosePendingRequest(peerID)

	if recvdType == nil {
		return nil, nil
	}

	valid, result := p.validateResponse(recvdType, data, datatype, location)
	if !valid {
		// Optionally ban the peer for misbehaving
		// p.BanPeer(peerID)
		return nil, errors.New("invalid response")
	}

	return result, nil
}

// validateResponse checks if the received response matches the requested data type and hash.
func (p *P2PNode) validateResponse(recvdType interface{}, data interface{}, datatype interface{}, location common.Location) (bool, interface{}) {
	switch datatype.(type) {
	case *types.WorkObject:
		if block, ok := recvdType.(*types.WorkObject); ok {
			switch data := data.(type) {
			case common.Hash:
				if block.Hash() == data {
					return true, block
				}
				log.Global.Errorf("invalid response: expected block with hash %s, got %s", data, block.Hash())
			case *big.Int:
				nodeCtx := location.Context()
				if block.Number(nodeCtx).Cmp(data) == 0 {
					return true, block
				}
				log.Global.Errorf("invalid response: expected block with number %s, got %s", data, block.Number(nodeCtx))
			}
		}
	case *types.Header:
		if header, ok := recvdType.(*types.Header); ok && header.Hash() == data.(common.Hash) {
			return true, header
		}
	case *types.Transaction:
		if tx, ok := recvdType.(*types.Transaction); ok && tx.Hash() == data.(common.Hash) {
			return true, tx
		}
	case common.Hash:
		if hash, ok := recvdType.(common.Hash); ok {
			return true, hash
		}
	case *trie.TrieNodeRequest:
		if trieNode, ok := recvdType.(*trie.TrieNodeResponse); ok {
			return true, trieNode
		}
	default:
		log.Global.Warn("peer returned unexpected type")
	}
	return false, nil
}

func (p *P2PNode) GetRequestManager() requestManager.RequestManager {
	return p.requestManager
}

func (p *P2PNode) GetPeerManager() peerManager.PeerManager {
	return p.peerManager
}

func (p *P2PNode) GetHostBackend() host.Host {
	return p.peerManager.GetHost()
}
