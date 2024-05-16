package pb

import (
	"math/big"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/core/types"
	"github.com/dominant-strategies/go-quai/log"
	"github.com/dominant-strategies/go-quai/trie"
)

// DecodeQuaiMessage decodes a byte slice into a QuaiMessage protobuf.
func DecodeQuaiMessage(data []byte) (*QuaiMessage, error) {
	msg := &QuaiMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// EncodeQuaiRequest creates a marshaled protobuf message for a Quai Request.
func EncodeQuaiRequest(id uint32, location common.Location, data interface{}, datatype interface{}) ([]byte, error) {
	reqMsg, err := createQuaiRequestMessage(id, location, data, datatype)
	if err != nil {
		return nil, err
	}

	quaiMsg := QuaiMessage{
		Payload: &QuaiMessage_Request{Request: reqMsg},
	}
	return proto.Marshal(&quaiMsg)
}

// createQuaiRequestMessage creates a QuaiRequestMessage based on the provided data and datatype.
func createQuaiRequestMessage(id uint32, location common.Location, data interface{}, datatype interface{}) (*QuaiRequestMessage, error) {
	reqMsg := &QuaiRequestMessage{
		Id:       id,
		Location: location.ProtoEncode(),
	}

	switch d := data.(type) {
	case common.Hash:
		reqMsg.Data = &QuaiRequestMessage_Hash{Hash: d.ProtoEncode()}
	case *big.Int:
		reqMsg.Data = &QuaiRequestMessage_Number{Number: d.Bytes()}
	default:
		return nil, errors.Errorf("unsupported request input data field type: %T", data)
	}

	switch datatype.(type) {
	case *types.WorkObject:
		reqMsg.Request = &QuaiRequestMessage_WorkObject{}
	case *types.Transaction:
		reqMsg.Request = &QuaiRequestMessage_Transaction{}
	case common.Hash:
		reqMsg.Request = &QuaiRequestMessage_BlockHash{}
	case trie.TrieNodeRequest:
		reqMsg.Request = &QuaiRequestMessage_TrieNode{}
	default:
		return nil, errors.Errorf("unsupported request data type: %T", datatype)
	}

	return reqMsg, nil
}

// DecodeQuaiRequest unmarshals a protobuf message into a Quai Request.
func DecodeQuaiRequest(reqMsg *QuaiRequestMessage) (uint32, interface{}, common.Location, interface{}, error) {
	location := &common.Location{}
	location.ProtoDecode(reqMsg.Location)

	reqData, err := decodeRequestData(reqMsg.Data)
	if err != nil {
		return reqMsg.Id, nil, common.Location{}, nil, err
	}

	reqType, err := decodeRequestType(reqMsg.Request)
	if err != nil {
		return reqMsg.Id, nil, common.Location{}, nil, err
	}

	return reqMsg.Id, reqType, *location, reqData, nil
}

func decodeRequestData(data isQuaiRequestMessage_Data) (interface{}, error) {
	switch d := data.(type) {
	case *QuaiRequestMessage_Hash:
		hash := &common.Hash{}
		hash.ProtoDecode(d.Hash)

		return hash, nil
	case *QuaiRequestMessage_Number:
		return new(big.Int).SetBytes(d.Number), nil
	default:
		return nil, errors.New("unsupported request data field type")
	}
}

func decodeRequestType(req isQuaiRequestMessage_Request) (interface{}, error) {
	switch t := req.(type) {
	case *QuaiRequestMessage_WorkObject:
		return &types.WorkObject{}, nil
	case *QuaiRequestMessage_Transaction:
		return &types.Transaction{}, nil
	case *QuaiRequestMessage_BlockHash:
		blockHash := &common.Hash{}
		blockHash.ProtoDecode(t.BlockHash)

		return blockHash, nil
	case *QuaiRequestMessage_TrieNode:
		return trie.TrieNodeRequest{}, nil
	default:
		return nil, errors.New("unsupported request type")
	}
}

// EncodeQuaiResponse creates a marshaled protobuf message for a Quai Response.
func EncodeQuaiResponse(id uint32, location common.Location, data interface{}) ([]byte, error) {
	respMsg, err := createQuaiResponseMessage(id, location, data)
	if err != nil {
		return nil, err
	}

	quaiMsg := QuaiMessage{
		Payload: &QuaiMessage_Response{Response: respMsg},
	}

	return proto.Marshal(&quaiMsg)
}

// createQuaiResponseMessage creates a QuaiResponseMessage based on the provided data.
func createQuaiResponseMessage(id uint32, location common.Location, data interface{}) (*QuaiResponseMessage, error) {
	respMsg := &QuaiResponseMessage{
		Id:       id,
		Location: location.ProtoEncode(),
	}

	switch data := data.(type) {
	case *types.WorkObject:
		protoWorkObject, err := data.ProtoEncode(types.BlockObject)
		if err != nil {
			return nil, err
		}
		respMsg.Response = &QuaiResponseMessage_WorkObject{WorkObject: protoWorkObject}

	case *types.Transaction:
		protoTransaction, err := data.ProtoEncode()
		if err != nil {
			return nil, err
		}
		respMsg.Response = &QuaiResponseMessage_Transaction{Transaction: protoTransaction}

	case *trie.TrieNodeResponse:
		protoTrieNode := &trie.ProtoTrieNode{ProtoNodeData: data.NodeData}
		respMsg.Response = &QuaiResponseMessage_TrieNode{TrieNode: protoTrieNode}

	case *common.Hash:
		respMsg.Response = &QuaiResponseMessage_BlockHash{BlockHash: data.ProtoEncode()}

	default:
		return nil, errors.Errorf("unsupported response data type: %T", data)
	}

	return respMsg, nil
}

// DecodeQuaiResponse unmarshals a protobuf message into a Quai Response message.
func DecodeQuaiResponse(respMsg *QuaiResponseMessage) (uint32, interface{}, error) {
	id := respMsg.Id
	sourceLocation := &common.Location{}
	sourceLocation.ProtoDecode(respMsg.Location)

	return decodeResponse(id, respMsg.Response, *sourceLocation)
}

func decodeResponse(id uint32, resp isQuaiResponseMessage_Response, sourceLocation common.Location) (uint32, interface{}, error) {
	switch r := resp.(type) {
	case *QuaiResponseMessage_WorkObject:
		protoWorkObject := r.WorkObject
		block := &types.WorkObject{}
		if err := block.ProtoDecode(protoWorkObject, sourceLocation, types.BlockObject); err != nil {
			return id, nil, err
		}
		if messageMetrics != nil {
			messageMetrics.WithLabelValues("blocks").Inc()
		}
		return id, block, nil
	case *QuaiResponseMessage_Transaction:
		protoTransaction := r.Transaction
		transaction := &types.Transaction{}
		if err := transaction.ProtoDecode(protoTransaction, sourceLocation); err != nil {
			return id, nil, err
		}
		if messageMetrics != nil {
			messageMetrics.WithLabelValues("transactions").Inc()
		}
		return id, transaction, nil
	case *QuaiResponseMessage_BlockHash:
		blockHash := r.BlockHash
		hash := common.Hash{}
		hash.ProtoDecode(blockHash)

		return id, hash, nil
	case *QuaiResponseMessage_TrieNode:
		protoTrieNode := r.TrieNode
		trieNode := &trie.TrieNodeResponse{NodeData: protoTrieNode.ProtoNodeData}
		return id, trieNode, nil
	default:
		return id, nil, errors.New("unsupported response type")
	}
}

// ConvertAndMarshal converts a custom Go type to a proto type and marshals it into a protobuf message.
func ConvertAndMarshal(data interface{}) ([]byte, error) {
	switch data := data.(type) {
	case *types.WorkObject:
		log.Global.Tracef("marshalling block: %+v", data)
		protoBlock, err := data.ProtoEncode(types.BlockObject)
		if err != nil {
			return nil, err
		}
		return proto.Marshal(protoBlock)
	case *types.Header:
		log.Global.Tracef("marshalling header: %+v", data)
		protoHeader, err := data.ProtoEncode()
		if err != nil {
			return nil, err
		}
		return proto.Marshal(protoHeader)
	case common.Hash:
		log.Global.Tracef("marshalling hash: %+v", data)
		protoHash := data.ProtoEncode()
		return proto.Marshal(protoHash)
	case *types.Transactions:
		protoTransactions, err := data.ProtoEncode()
		if err != nil {
			return nil, err
		}
		return proto.Marshal(protoTransactions)
	case *types.ProvideTopic:
		protoProvideTopic := &types.ProtoProvideTopic{Topic: &data.Topic}
		return proto.Marshal(protoProvideTopic)
	case *types.WorkObjectHeader:
		log.Global.Tracef("marshalling block header: %+v", data)
		protoWoHeader, err := data.ProtoEncode()
		if err != nil {
			return nil, err
		}
		return proto.Marshal(protoWoHeader)
	default:
		return nil, errors.New("unsupported data type")
	}
}

// UnmarshalAndConvert unmarshals a protobuf message into a proto type and converts it to a custom Go type.
func UnmarshalAndConvert(data []byte, sourceLocation common.Location, dataPtr *interface{}, datatype interface{}) error {
	switch datatype.(type) {
	case *types.WorkObject:
		return unmarshalWorkObject(data, sourceLocation, dataPtr)
	case *types.WorkObjectHeader:
		return unmarshalWorkObjectHeader(data, dataPtr)
	case *types.Header:
		return unmarshalHeader(data, sourceLocation, dataPtr)
	case *types.Transactions:
		return unmarshalTransactions(data, sourceLocation, dataPtr)
	case common.Hash:
		return unmarshalHash(data, dataPtr)
	case *types.ProvideTopic:
		return unmarshalProvideTopic(data, dataPtr)
	default:
		return errors.New("unsupported data type")
	}
}

func unmarshalWorkObject(data []byte, sourceLocation common.Location, dataPtr *interface{}) error {
	protoWorkObject := &types.ProtoWorkObject{}
	if err := proto.Unmarshal(data, protoWorkObject); err != nil {
		return err
	}
	workObject := &types.WorkObject{}
	if protoWorkObject.WoHeader == nil {
		return errors.New("woheader is nil")
	}
	if protoWorkObject.WoHeader.Location == nil {
		return errors.New("location is nil")
	}
	if err := workObject.ProtoDecode(protoWorkObject, sourceLocation, types.BlockObject); err != nil {
		return err
	}
	*dataPtr = *workObject
	return nil
}

func unmarshalWorkObjectHeader(data []byte, dataPtr *interface{}) error {
	protoWorkObjectHeader := &types.ProtoWorkObjectHeader{}
	if err := proto.Unmarshal(data, protoWorkObjectHeader); err != nil {
		return err
	}
	workObjectHeader := &types.WorkObjectHeader{}
	if err := workObjectHeader.ProtoDecode(protoWorkObjectHeader); err != nil {
		return err
	}
	*dataPtr = *workObjectHeader
	return nil
}

func unmarshalHeader(data []byte, sourceLocation common.Location, dataPtr *interface{}) error {
	protoHeader := &types.ProtoHeader{}
	if err := proto.Unmarshal(data, protoHeader); err != nil {
		return err
	}
	header := &types.Header{}
	if err := header.ProtoDecode(protoHeader, sourceLocation); err != nil {
		return err
	}
	*dataPtr = *header
	return nil
}

func unmarshalTransactions(data []byte, sourceLocation common.Location, dataPtr *interface{}) error {
	protoTransactions := &types.ProtoTransactions{}
	if err := proto.Unmarshal(data, protoTransactions); err != nil {
		return err
	}
	transactions := &types.Transactions{}
	if err := transactions.ProtoDecode(protoTransactions, sourceLocation); err != nil {
		return err
	}
	*dataPtr = *transactions
	return nil
}

func unmarshalHash(data []byte, dataPtr *interface{}) error {
	protoHash := &common.ProtoHash{}
	if err := proto.Unmarshal(data, protoHash); err != nil {
		return err
	}
	hash := common.Hash{}
	hash.ProtoDecode(protoHash)
	*dataPtr = hash
	return nil
}

func unmarshalProvideTopic(data []byte, dataPtr *interface{}) error {
	protoProvideTopic := &types.ProtoProvideTopic{}
	if err := proto.Unmarshal(data, protoProvideTopic); err != nil {
		return err
	}
	provideTopic := &types.ProvideTopic{}
	provideTopic.Topic = *protoProvideTopic.Topic
	*dataPtr = *provideTopic
	return nil
}
