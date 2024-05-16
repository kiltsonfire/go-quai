package protocol

import (
	"context"
	"errors"
	"io"
	"math/big"
	"runtime/debug"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/core/types"
	"github.com/dominant-strategies/go-quai/log"
	"github.com/dominant-strategies/go-quai/p2p/pb"
	"github.com/dominant-strategies/go-quai/trie"
)

const (
	numWorkers   = 10  // Number of workers per stream
	msgChanSize  = 100 // Larger queue for better handling of bursts
	protocolName = "quai-protocol"
)

func QuaiProtocolHandler(stream network.Stream, node QuaiP2PNode) {
	defer stream.Close()
	defer recoverPanic("QuaiProtocolHandler")

	log.Global.Debugf("Received a new stream from %s", stream.Conn().RemotePeer())

	if stream.Protocol() != ProtocolVersion {
		log.Global.Warnf("Invalid protocol: %s", stream.Protocol())
		return
	}

	msgChan := make(chan []byte, msgChanSize)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var once sync.Once

	for i := 0; i < numWorkers; i++ {
		go worker(ctx, stream, node, msgChan, &once)
	}

	readLoop(ctx, stream, msgChan, &once)
}

func recoverPanic(functionName string) {
	if r := recover(); r != nil {
		log.Global.WithFields(log.Fields{
			"function":   functionName,
			"error":      r,
			"stacktrace": string(debug.Stack()),
		}).Fatal("Go-Quai Panicked")
	}
}

func worker(ctx context.Context, stream network.Stream, node QuaiP2PNode, msgChan chan []byte, once *sync.Once) {
	defer recoverPanic("worker")

	for {
		select {
		case message := <-msgChan:
			handleMessage(message, stream, node)
		case <-ctx.Done():
			once.Do(func() { close(msgChan) })
			return
		}
	}
}

func readLoop(ctx context.Context, stream network.Stream, msgChan chan []byte, once *sync.Once) {
	full := 0

	for {
		data, err := common.ReadMessageFromStream(stream)
		if err != nil {
			if errors.Is(err, network.ErrReset) || errors.Is(err, io.EOF) {
				once.Do(func() { close(msgChan) })
				return
			}

			log.Global.Errorf("error reading message from stream: %s", err)
			continue
		}

		select {
		case msgChan <- data:
		case <-ctx.Done():
			once.Do(func() { close(msgChan) })
			return
		default:
			if full%1000 == 0 {
				log.Global.WithField("stream with peer", stream.Conn().RemotePeer()).Warnf("QuaiProtocolHandler message channel is full. Lost messages: %d", full)
			}
			full++
		}
	}
}

func handleMessage(data []byte, stream network.Stream, node QuaiP2PNode) {
	defer recoverPanic("handleMessage")

	quaiMsg, err := pb.DecodeQuaiMessage(data)
	if err != nil {
		log.Global.Errorf("error decoding quai message: %s", err)
		return
	}

	switch {
	case quaiMsg.GetRequest() != nil:
		handleRequest(quaiMsg.GetRequest(), stream, node)
		incrementMetric("requests")

	case quaiMsg.GetResponse() != nil:
		handleResponse(quaiMsg.GetResponse(), node)
		incrementMetric("responses")

	default:
		log.Global.WithField("quaiMsg", quaiMsg).Errorf("unsupported quai message type")
	}
}

func incrementMetric(label string) {
	if messageMetrics != nil {
		messageMetrics.WithLabelValues(label).Inc()
	}
}

func handleRequest(quaiMsg *pb.QuaiRequestMessage, stream network.Stream, node QuaiP2PNode) {
	id, decodedType, loc, query, err := pb.DecodeQuaiRequest(quaiMsg)
	if err != nil {
		log.Global.WithField("err", err).Errorf("error decoding quai request")
		return
	}

	logRequestDetails(id, decodedType, loc, query, stream.Conn().RemotePeer())

	switch decodedType.(type) {
	case *types.WorkObject:
		handleWorkObjectRequest(id, loc, query, stream, node)
	case *types.Header:
		handleHeaderRequest(id, loc, *query.(*common.Hash), stream, node)
	case *types.Transaction:
		handleTransactionRequest(id, loc, *query.(*common.Hash), stream, node)
	case *common.Hash:
		handleBlockNumberRequest(id, loc, query.(*big.Int), stream, node)
	case trie.TrieNodeRequest:
		handleTrieNodeRequest(id, loc, *query.(*common.Hash), stream, node)
	default:
		log.Global.WithField("request type", decodedType).Error("unsupported request data type")
	}
}

func logRequestDetails(id uint32, decodedType interface{}, loc common.Location, query interface{}, peerID peer.ID) {
	log.Global.WithFields(log.Fields{
		"requestID":   id,
		"decodedType": decodedType,
		"location":    loc,
		"query":       query,
		"peer":        peerID,
	}).Debug("Received request")
}

func handleWorkObjectRequest(id uint32, loc common.Location, query interface{}, stream network.Stream, node QuaiP2PNode) {
	requestedHash := &common.Hash{}
	switch query := query.(type) {
	case *common.Hash:
		requestedHash = query
	case *big.Int:
		number := query
		log.Global.Tracef("Looking hash for block %s and location %s", number.String(), loc.Name())
		requestedHash = node.GetBlockHashByNumber(number, loc)
		if requestedHash == nil {
			log.Global.Debugf("block hash not found for block %s and location %s", number.String(), loc.Name())
			return
		}
		log.Global.Tracef("Found hash for block %s and location: %s hash: %s", number.String(), loc.Name(), requestedHash)
	}

	if err := handleBlockRequest(id, loc, *requestedHash, stream, node); err != nil {
		log.Global.WithField("peer", stream.Conn().RemotePeer()).Errorf("error handling block request: %s", err)
	}
	incrementMetric("blocks")
}

func handleResponse(quaiResp *pb.QuaiResponseMessage, node QuaiP2PNode) {
	recvdID, recvdType, err := pb.DecodeQuaiResponse(quaiResp)
	if err != nil {
		log.Global.Errorf("error decoding quai response: %s", err)
		return
	}

	dataChan, err := node.GetRequestManager().GetRequestChan(recvdID)
	if err != nil {
		log.Global.WithFields(log.Fields{
			"requestID": recvdID,
			"err":       err,
		}).Error("error associating request ID with data channel")
		return
	}
	select {
	case dataChan <- recvdType:
	default:
	}
}

func handleBlockRequest(id uint32, loc common.Location, hash common.Hash, stream network.Stream, node QuaiP2PNode) error {
	block := node.GetWorkObject(hash, loc)
	if block == nil {
		log.Global.Debugf("block not found")
		return nil
	}
	log.Global.Debugf("block found %s", block.Hash())

	data, err := pb.EncodeQuaiResponse(id, loc, block)
	if err != nil {
		return err
	}

	return sendResponse(stream, data, block.Hash())
}

func sendResponse(stream network.Stream, data []byte, hash common.Hash) error {
	if err := common.WriteMessageToStream(stream, data); err != nil {
		return err
	}
	log.Global.WithFields(log.Fields{
		"hash": hash,
		"peer": stream.Conn().RemotePeer(),
	}).Trace("Sent response to peer")
	return nil
}

func handleHeaderRequest(id uint32, loc common.Location, hash common.Hash, stream network.Stream, node QuaiP2PNode) error {
	header := node.GetHeader(hash, loc)
	if header == nil {
		log.Global.Debugf("header not found")
		return nil
	}
	log.Global.Debugf("header found %s", header.Hash())

	data, err := pb.EncodeQuaiResponse(id, loc, header)
	if err != nil {
		return err
	}

	return sendResponse(stream, data, header.Hash())
}

func handleTransactionRequest(id uint32, loc common.Location, hash common.Hash, stream network.Stream, node QuaiP2PNode) error {
	// TODO: Implement handleTransactionRequest
	return nil
}

func handleBlockNumberRequest(id uint32, loc common.Location, number *big.Int, stream network.Stream, node QuaiP2PNode) error {
	blockHash := node.GetBlockHashByNumber(number, loc)
	if blockHash == nil {
		log.Global.Tracef("block not found")
		return nil
	}
	log.Global.Tracef("block found %s", blockHash)

	data, err := pb.EncodeQuaiResponse(id, loc, blockHash)
	if err != nil {
		return err
	}

	return sendResponse(stream, data, *blockHash)
}

func handleTrieNodeRequest(id uint32, loc common.Location, hash common.Hash, stream network.Stream, node QuaiP2PNode) error {
	trieNode := node.GetTrieNode(hash, loc)
	if trieNode == nil {
		log.Global.Tracef("trie node not found")
		return nil
	}
	log.Global.Tracef("trie node found")

	data, err := pb.EncodeQuaiResponse(id, loc, trieNode)
	if err != nil {
		return err
	}

	return sendResponse(stream, data, hash)
}
