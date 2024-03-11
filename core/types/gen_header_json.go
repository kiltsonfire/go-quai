// Code generated by github.com/fjl/gencodec. DO NOT EDIT.

package types

import (
	"encoding/json"
	"errors"
	"math/big"

	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/common/hexutil"
)

var _ = (*headerMarshaling)(nil)

// MarshalJSON marshals as JSON.
func (h Header) MarshalJSON() ([]byte, error) {
	var enc struct {
		ParentHash    []common.Hash  `json:"parentHash"          gencodec:"required"`
		UncleHash     common.Hash    `json:"sha3Uncles"          gencodec:"required"`
		Coinbase      common.Address `json:"miner"               gencodec:"required"`
		EVMRoot       common.Hash    `json:"evmRoot"           gencodec:"required"`
		UTXORoot      common.Hash    `json:"utxoRoot"              gencodec:"required"`
		TxHash        common.Hash    `json:"transactionsRoot"    gencodec:"required"`
		EtxHash       common.Hash    `json:"extTransactionsRoot" gencodec:"required"`
		EtxRollupHash common.Hash    `json:"extRollupRoot"       gencodec:"required"`
		ManifestHash  []common.Hash  `json:"manifestHash"        gencodec:"required"`
		ReceiptHash   common.Hash    `json:"receiptsRoot"        gencodec:"required"`
		Difficulty    *hexutil.Big   `json:"difficulty"          gencodec:"required"`
		ParentEntropy []*hexutil.Big `json:"parentEntropy"       gencodec:"required"`
		ParentDeltaS  []*hexutil.Big `json:"parentDeltaS"        gencodec:"required"`
		Number        []*hexutil.Big `json:"number"              gencodec:"required"`
		GasLimit      hexutil.Uint64 `json:"gasLimit"            gencodec:"required"`
		GasUsed       hexutil.Uint64 `json:"gasUsed"             gencodec:"required"`
		BaseFee       *hexutil.Big   `json:"baseFeePerGas"       gencodec:"required"`
		Location      hexutil.Bytes  `json:"location"            gencodec:"required"`
		Time          hexutil.Uint64 `json:"timestamp"           gencodec:"required"`
		Extra         hexutil.Bytes  `json:"extraData"           gencodec:"required"`
		MixHash       common.Hash    `json:"mixHash"             gencodec:"required"`
		Nonce         BlockNonce     `json:"nonce"`
		Hash          common.Hash    `json:"hash"`
	}
	// Initialize the enc struct
	enc.ParentEntropy = make([]*hexutil.Big, common.HierarchyDepth)
	enc.ParentDeltaS = make([]*hexutil.Big, common.HierarchyDepth)
	enc.Number = make([]*hexutil.Big, common.HierarchyDepth)

	copy(enc.ParentHash, h.ParentHashArray())
	copy(enc.ManifestHash, h.ManifestHashArray())
	for i := 0; i < common.HierarchyDepth; i++ {
		enc.ParentEntropy[i] = (*hexutil.Big)(h.ParentEntropy(i))
		enc.ParentDeltaS[i] = (*hexutil.Big)(h.ParentDeltaS(i))
		enc.Number[i] = (*hexutil.Big)(h.Number(i))
	}
	enc.UncleHash = h.UncleHash()
	enc.Coinbase = h.Coinbase()
	enc.EVMRoot = h.EVMRoot()
	enc.UTXORoot = h.UTXORoot()
	enc.TxHash = h.TxHash()
	enc.EtxHash = h.EtxHash()
	enc.EtxRollupHash = h.EtxRollupHash()
	enc.ReceiptHash = h.ReceiptHash()
	enc.Difficulty = (*hexutil.Big)(h.Difficulty())
	enc.GasLimit = hexutil.Uint64(h.GasLimit())
	enc.GasUsed = hexutil.Uint64(h.GasUsed())
	enc.BaseFee = (*hexutil.Big)(h.BaseFee())
	enc.Location = hexutil.Bytes(h.Location())
	enc.Time = hexutil.Uint64(h.Time())
	enc.Extra = hexutil.Bytes(h.Extra())
	enc.MixHash = h.MixHash()
	enc.Nonce = h.Nonce()
	enc.Hash = h.Hash()
	raw, err := json.Marshal(&enc)
	return raw, err
}

// UnmarshalJSON unmarshals from JSON.
func (h *Header) UnmarshalJSON(input []byte) error {
	var dec struct {
		ParentHash    []common.Hash        `json:"parentHash"          gencodec:"required"`
		UncleHash     *common.Hash         `json:"sha3Uncles"          gencodec:"required"`
		Coinbase      *common.AddressBytes `json:"miner"               gencodec:"required"`
		EVMRoot       *common.Hash         `json:"evmRoot"           gencodec:"required"`
		UTXORoot      *common.Hash         `json:"utxoRoot"              gencodec:"required"`
		TxHash        *common.Hash         `json:"transactionsRoot"    gencodec:"required"`
		ReceiptHash   *common.Hash         `json:"receiptsRoot"        gencodec:"required"`
		EtxHash       *common.Hash         `json:"extTransactionsRoot" gencodec:"required"`
		EtxRollupHash *common.Hash         `json:"extRollupRoot"       gencodec:"required"`
		ManifestHash  []common.Hash        `json:"manifestHash"        gencodec:"required"`
		Difficulty    *hexutil.Big         `json:"difficulty"          gencodec:"required"`
		ParentEntropy []*hexutil.Big       `json:"parentEntropy"       gencodec:"required"`
		ParentDeltaS  []*hexutil.Big       `json:"parentDeltaS"        gencodec:"required"`
		Number        []*hexutil.Big       `json:"number"              gencodec:"required"`
		GasLimit      *hexutil.Uint64      `json:"gasLimit"            gencodec:"required"`
		GasUsed       *hexutil.Uint64      `json:"gasUsed"             gencodec:"required"`
		BaseFee       *hexutil.Big         `json:"baseFeePerGas"       gencodec:"required"`
		Location      hexutil.Bytes        `json:"location"            gencodec:"required"`
		Time          hexutil.Uint64       `json:"timestamp"           gencodec:"required"`
		Extra         hexutil.Bytes        `json:"extraData"           gencodec:"required"`
		MixHash       *common.Hash         `json:"MixHash"             gencodec:"required"`
		Nonce         BlockNonce           `json:"nonce"`
	}
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.ParentHash == nil {
		return errors.New("missing required field 'parentHash' for Header")
	}
	if dec.UncleHash == nil {
		return errors.New("missing required field 'sha3Uncles' for Header")
	}
	if dec.Coinbase == nil {
		return errors.New("missing required field 'miner' for Header")
	}
	if dec.EVMRoot == nil {
		return errors.New("missing required field 'evmRoot' for Header")
	}
	if dec.UTXORoot == nil {
		return errors.New("missing required field 'utxoRoot' for Header")
	}
	if dec.TxHash == nil {
		return errors.New("missing required field 'transactionsRoot' for Header")
	}
	if dec.EtxHash == nil {
		return errors.New("missing required field 'extTransactionsRoot' for Header")
	}
	if dec.EtxRollupHash == nil {
		return errors.New("missing required field 'extRollupRoot' for Header")
	}
	if dec.ManifestHash == nil {
		return errors.New("missing required field 'manifestHash' for Header")
	}
	if dec.ReceiptHash == nil {
		return errors.New("missing required field 'receiptsRoot' for Header")
	}
	if dec.Difficulty == nil {
		return errors.New("missing required field 'difficulty' for Header")
	}
	if dec.ParentEntropy == nil {
		return errors.New("missing required field 'parentEntropy' for Header")
	}
	if dec.ParentDeltaS == nil {
		return errors.New("missing required field 'parentDeltaS' for Header")
	}
	if dec.Number == nil {
		return errors.New("missing required field 'number' for Header")
	}
	if dec.GasLimit == nil {
		return errors.New("missing required field 'gasLimit' for Header")
	}
	if dec.GasUsed == nil {
		return errors.New("missing required field 'gasUsed' for Header")
	}
	if dec.BaseFee == nil {
		return errors.New("missing required field 'baseFee' for Header")
	}
	if dec.Extra == nil {
		return errors.New("missing required field 'extraData' for Header")
	}
	if dec.MixHash == nil {
		return errors.New("missing required field 'mixHash' for Header")
	}
	// Initialize the header
	h.parentHash = make([]common.Hash, common.HierarchyDepth)
	h.manifestHash = make([]common.Hash, common.HierarchyDepth)
	h.parentEntropy = make([]*big.Int, common.HierarchyDepth)
	h.parentDeltaS = make([]*big.Int, common.HierarchyDepth)
	h.number = make([]*big.Int, common.HierarchyDepth)

	for i := 0; i < common.HierarchyDepth; i++ {
		h.SetParentHash(dec.ParentHash[i], i)
		h.SetManifestHash(dec.ManifestHash[i], i)
		if dec.ParentEntropy[i] == nil {
			return errors.New("missing required field 'parentEntropy' for Header")
		}
		h.SetParentEntropy((*big.Int)(dec.ParentEntropy[i]), i)
		if dec.ParentDeltaS[i] == nil {
			return errors.New("missing required field 'parentDeltaS' for Header")
		}
		h.SetParentDeltaS((*big.Int)(dec.ParentDeltaS[i]), i)
		if dec.Number[i] == nil {
			return errors.New("missing required field 'number' for Header")
		}
		h.SetNumber((*big.Int)(dec.Number[i]), i)
	}
	h.SetUncleHash(*dec.UncleHash)
	if len(dec.Location) > 0 {
		h.location = make([]byte, len(dec.Location))
		copy(h.location, dec.Location)
	}
	coinbase := common.Bytes20ToAddress(*dec.Coinbase, h.location)
	h.SetCoinbase(coinbase)
	h.SetEVMRoot(*dec.EVMRoot)
	h.SetUTXORoot(*dec.UTXORoot)
	h.SetTxHash(*dec.TxHash)
	h.SetReceiptHash(*dec.ReceiptHash)
	h.SetEtxHash(*dec.EtxHash)
	h.SetEtxRollupHash(*dec.EtxRollupHash)
	h.SetDifficulty((*big.Int)(dec.Difficulty))
	h.SetGasLimit(uint64(*dec.GasLimit))
	h.SetGasUsed(uint64(*dec.GasUsed))
	h.SetBaseFee((*big.Int)(dec.BaseFee))
	h.SetTime(uint64(dec.Time))
	h.SetExtra(dec.Extra)
	h.SetMixHash(*dec.MixHash)
	h.SetNonce(dec.Nonce)
	return nil
}

func (t Termini) MarshalJSON() ([]byte, error) {
	var enc struct {
		DomTermini []common.Hash `json:"domTermini" gencodec:"required"`
		SubTermini []common.Hash `json:"subTermini"  gencodec:"required"`
	}
	copy(enc.SubTermini, t.SubTermini())
	copy(enc.DomTermini, t.DomTermini())
	raw, err := json.Marshal(&enc)
	return raw, err
}

func (t *Termini) UnmarshalJSON(input []byte) error {
	var dec struct {
		DomTermini []common.Hash `json:"domTermini" gencodec:"required"`
		SubTermini []common.Hash `json:"subTermini"  gencodec:"required"`
	}
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.DomTermini == nil {
		return errors.New("missing required field 'domTerminus' for Termini")
	}
	if dec.SubTermini == nil {
		return errors.New("missing required field 'subTermini' for Termini")
	}
	t.SetDomTermini(dec.DomTermini)
	t.SetSubTermini(dec.SubTermini)
	return nil
}

func (wh *WorkObjectHeader) MarshalJSON() ([]byte, error) {
	var enc struct {
		HeaderHash common.Hash   `json:"woheaderHash" gencoden:"required"`
		ParentHash common.Hash   `json:"woparentHash" gencoden:"required"`
		Number     *hexutil.Big  `json:"wonumber" gencoden:"required"`
		Difficulty *hexutil.Big  `json:"wodifficulty" gencoden:"required"`
		TxHash     common.Hash   `json:"wotxHash" gencoden:"required"`
		Location   hexutil.Bytes `json:"wolocation" gencoden:"required"`
		Nonce      BlockNonce    `json:"wononce" gencoden:"required"`
	}

	enc.HeaderHash = wh.HeaderHash()
	enc.Difficulty = (*hexutil.Big)(wh.Difficulty())
	enc.Number = (*hexutil.Big)(wh.Number())
	enc.TxHash = wh.TxHash()
	enc.Location = hexutil.Bytes(wh.Location())
	enc.Nonce = wh.Nonce()

	raw, err := json.Marshal(&enc)
	return raw, err
}

func (wh *WorkObjectHeader) UnmarshalJSON(input []byte) error {
	var dec struct {
		HeaderHash common.Hash     `json:"woheaderHash" gencoden:"required"`
		ParentHash common.Hash     `json:"woparentHash" gencoden:"required"`
		Number     *hexutil.Big    `json:"wonumber" gencoden:"required"`
		Difficulty *hexutil.Big    `json:"wodifficulty" gencoden:"required"`
		TxHash     common.Hash     `json:"wotxHash" gencoden:"required"`
		Location   common.Location `json:"wolocation" gencoden:"required"`
		Nonce      BlockNonce      `json:"wononce" gencoden:"required"`
	}

	err := json.Unmarshal(input, &dec)
	if err != nil {
		return err
	}

	wh.SetHeaderHash(dec.HeaderHash)
	wh.SetParentHash(dec.ParentHash)
	wh.SetNumber((*big.Int)(dec.Number))
	wh.SetDifficulty((*big.Int)(dec.Difficulty))
	wh.SetTxHash(dec.TxHash)
	if len(dec.Location) > 0 {
		wh.location = make([]byte, len(dec.Location))
		copy(wh.location, dec.Location)
	}
	wh.SetNonce(dec.Nonce)
	return nil
}

func (wb *WorkObjectBody) MarshalJSON() ([]byte, error) {
	var enc struct {
		Header *Header `json:"header" gencoden:"required"`
		Transactions Transactions `json:"transactions" gencoden:"required"`
		ExtTransactions Transactions `json:"extTransactions" gencoden:"required"`
		Uncles WorkObjects `json:"uncles" gencoden:"required"`
		Manifest BlockManifest `json:"manifest" gencoden:"required"`
	}

	enc.Header = wb.Header()
	enc.Transactions = wb.Transactions()
	enc.ExtTransactions = wb.ExtTransactions()
	enc.Manifest = wb.Manifest()

	raw, err := json.Marshal(&enc)
	return raw, err
}

func (wb *WorkObjectBody) UnmarshalJSON(input []byte) error {
	var dec struct {
		Header *Header `json:"header" gencoden:"required"`
		Transactions Transactions `json:"transactions" gencoden:"required"`
		ExtTransactions Transactions `json:"extTransactions" gencoden:"required"`
		Uncles WorkObjects `json:"uncles" gencoden:"required"`
		Manifest BlockManifest `json:"manifest" gencoden:"required"`
	}

	err := json.Unmarshal(input, &dec)
	if err != nil {
		return err
	}

	wb.SetHeader(dec.Header)
	wb.SetTransactions(dec.Transactions)
	wb.SetExtTransactions(dec.ExtTransactions)
	wb.SetManifest(dec.Manifest)
	return nil
}

func (wo *WorkObject) MarshalJSON() ([]byte, error) {
	var enc struct {
		WoHeader *WorkObjectHeader `json:"woHeader" gencoden:"required"`
		WoBody   *WorkObjectBody   `json:"woBody" gencoden:"required"`
	}

	enc.WoHeader = wo.WorkObjectHeader()
	enc.WoBody = wo.Body()

	raw, err := json.Marshal(&enc)
	return raw, err
}