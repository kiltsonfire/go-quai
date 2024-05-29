package types

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/common/hexutil"
	"github.com/dominant-strategies/go-quai/log"
	"google.golang.org/protobuf/proto"
	"lukechampine.com/blake3"
)

var (
	workObjectPool = sync.Pool{
		New: func() interface{} {
			return new(WorkObject)
		},
	}

	workObjectHeaderPool = sync.Pool{
		New: func() interface{} {
			return new(WorkObjectHeader)
		},
	}

	workObjectBodyPool = sync.Pool{
		New: func() interface{} {
			return new(WorkObjectBody)
		},
	}

	transactionPool = sync.Pool{
		New: func() interface{} {
			return new(Transaction)
		},
	}

	headerPool = sync.Pool{
		New: func() interface{} {
			return new(Header)
		},
	}

	blockManifestPool = sync.Pool{
		New: func() interface{} {
			return new(BlockManifest)
		},
	}

	interlinkHashesPool = sync.Pool{
		New: func() interface{} {
			return new(common.Hashes)
		},
	}
)

type WorkObject struct {
	woHeader *WorkObjectHeader
	woBody   *WorkObjectBody
	tx       *Transaction

	// caches
	appendTime atomic.Value

	// These fields are used to track
	// inter-peer block relay.
	ReceivedAt   time.Time
	ReceivedFrom interface{}
}

type WorkObjectHeader struct {
	headerHash common.Hash
	parentHash common.Hash
	number     *big.Int
	difficulty *big.Int
	txHash     common.Hash
	location   common.Location
	mixHash    common.Hash
	time       uint64
	nonce      BlockNonce

	PowHash   atomic.Value
	PowDigest atomic.Value
}

type WorkObjects []*WorkObject

type WorkObjectView int

// Work object types
const (
	BlockObject WorkObjectView = iota
	TxObject
	PEtxObject
	HeaderObject
	PhObject
	WorkShareObject
)

func (wo *WorkObject) Hash() common.Hash {
	return wo.WorkObjectHeader().Hash()
}

func (wo *WorkObject) SealHash() common.Hash {
	return wo.WorkObjectHeader().SealHash()
}

func (wo *WorkObject) IsUncle() bool {
	if wo.WorkObjectHeader() != nil &&
		wo.Body() == nil {
		return true
	}
	return false
}

////////////////////////////////////////////////////////////
/////////////////// Work Object Getters ///////////////
////////////////////////////////////////////////////////////

func (wo *WorkObject) WorkObjectHeader() *WorkObjectHeader {
	return wo.woHeader
}

func (wo *WorkObject) Body() *WorkObjectBody {
	return wo.woBody
}

func (wo *WorkObject) Tx() *Transaction {
	return wo.tx
}

////////////////////////////////////////////////////////////
/////////////////// Work Object Setters ///////////////
////////////////////////////////////////////////////////////

func (wo *WorkObject) SetWorkObjectHeader(header *WorkObjectHeader) {
	wo.woHeader = header
}

func (wo *WorkObject) SetBody(body *WorkObjectBody) {
	wo.woBody = body
}

func (wo *WorkObject) SetTx(tx *Transaction) {
	wo.tx = tx
}

func (wo *WorkObject) SetAppendTime(appendTime time.Duration) {
	wo.appendTime.Store(appendTime)
}

////////////////////////////////////////////////////////////
/////////////////// Work Object Generic Getters ///////////////
////////////////////////////////////////////////////////////

// GetAppendTime returns the appendTime of the block
// The appendTime is computed on the first call and cached thereafter.
func (wo *WorkObject) GetAppendTime() time.Duration {
	if appendTime := wo.appendTime.Load(); appendTime != nil {
		if val, ok := appendTime.(time.Duration); ok {
			return val
		}
	}
	return -1
}

// Size returns the true RLP encoded storage size of the block, either by encoding
// and returning it, or returning a previsouly cached value.
func (wo *WorkObject) Size() common.StorageSize {
	protoWorkObject, err := wo.ProtoEncode(BlockObject)
	if err != nil {
		return common.StorageSize(0)
	}
	data, err := proto.Marshal(protoWorkObject)
	if err != nil {
		return common.StorageSize(0)
	}
	return common.StorageSize(len(data))
}

func (wo *WorkObject) HeaderHash() common.Hash {
	return wo.WorkObjectHeader().HeaderHash()
}

func (wo *WorkObject) Difficulty() *big.Int {
	return wo.WorkObjectHeader().Difficulty()
}

func (wo *WorkObject) TxHash() common.Hash {
	return wo.WorkObjectHeader().TxHash()
}

func (wo *WorkObject) MixHash() common.Hash {
	return wo.WorkObjectHeader().MixHash()
}

func (wo *WorkObject) Nonce() BlockNonce {
	return wo.WorkObjectHeader().Nonce()
}

func (wo *WorkObject) Location() common.Location {
	return wo.WorkObjectHeader().Location()
}

func (wo *WorkObject) Time() uint64 {
	return wo.WorkObjectHeader().Time()
}

func (wo *WorkObject) Header() *Header {
	return wo.Body().Header()
}

func (wo *WorkObject) ParentHash(nodeCtx int) common.Hash {
	if nodeCtx == common.ZONE_CTX {
		return wo.WorkObjectHeader().ParentHash()
	} else {
		return wo.Body().Header().ParentHash(nodeCtx)
	}
}

func (wo *WorkObject) Number(nodeCtx int) *big.Int {
	if nodeCtx == common.ZONE_CTX {
		return wo.WorkObjectHeader().Number()
	} else {
		return wo.Body().Header().Number(nodeCtx)
	}
}

func (wo *WorkObject) NumberU64(nodeCtx int) uint64 {
	if nodeCtx == common.ZONE_CTX {
		return wo.WorkObjectHeader().NumberU64()
	} else {
		return wo.Body().Header().NumberU64(nodeCtx)
	}
}

func (wo *WorkObject) NonceU64() uint64 {
	return wo.WorkObjectHeader().Nonce().Uint64()
}

func (wo *WorkObject) UncledS() *big.Int {
	return wo.Header().UncledS()
}

func (wo *WorkObject) EVMRoot() common.Hash {
	return wo.Header().EVMRoot()
}

func (wo *WorkObject) ParentEntropy(nodeCtx int) *big.Int {
	return wo.Header().ParentEntropy(nodeCtx)
}

func (wo *WorkObject) EtxRollupHash() common.Hash {
	return wo.Header().EtxRollupHash()
}

func (wo *WorkObject) EtxSetRoot() common.Hash {
	return wo.Header().EtxSetRoot()
}

func (wo *WorkObject) BaseFee() *big.Int {
	return wo.Header().BaseFee()
}

func (wo *WorkObject) GasUsed() uint64 {
	return wo.Header().GasUsed()
}

func (wo *WorkObject) GasLimit() uint64 {
	return wo.Header().GasLimit()
}

func (wo *WorkObject) Coinbase() common.Address {
	return wo.Header().Coinbase()
}

func (wo *WorkObject) ManifestHash(nodeCtx int) common.Hash {
	return wo.Header().ManifestHash(nodeCtx)
}

func (wo *WorkObject) ParentDeltaS(nodeCtx int) *big.Int {
	return wo.Header().ParentDeltaS(nodeCtx)
}

func (wo *WorkObject) ParentUncledSubDeltaS(nodeCtx int) *big.Int {
	return wo.Header().ParentUncledSubDeltaS(nodeCtx)
}

func (wo *WorkObject) UncleHash() common.Hash {
	return wo.Header().UncleHash()
}

func (wo *WorkObject) EtxHash() common.Hash {
	return wo.Header().EtxHash()
}

func (wo *WorkObject) ReceiptHash() common.Hash {
	return wo.Header().ReceiptHash()
}

func (wo *WorkObject) Extra() []byte {
	return wo.Header().Extra()
}

func (wo *WorkObject) UTXORoot() common.Hash {
	return wo.Header().UTXORoot()
}

func (wo *WorkObject) EfficiencyScore() uint16 {
	return wo.Header().EfficiencyScore()
}

func (wo *WorkObject) ThresholdCount() uint16 {
	return wo.Header().ThresholdCount()
}

func (wo *WorkObject) ExpansionNumber() uint8 {
	return wo.Header().ExpansionNumber()
}

func (wo *WorkObject) EtxEligibleSlices() common.Hash {
	return wo.Header().EtxEligibleSlices()
}

func (wo *WorkObject) InterlinkRootHash() common.Hash {
	return wo.Header().InterlinkRootHash()
}

func (wo *WorkObject) PrimeTerminus() common.Hash {
	return wo.Header().PrimeTerminus()
}
func (wo *WorkObject) Transactions() Transactions {
	return wo.Body().Transactions()
}

func (wo *WorkObject) ExtTransactions() Transactions {
	return wo.Body().ExtTransactions()
}

func (wo *WorkObject) Uncles() []*WorkObjectHeader {
	return wo.Body().Uncles()
}

func (wo *WorkObject) Manifest() BlockManifest {
	return wo.Body().Manifest()
}

func (wo *WorkObject) InterlinkHashes() common.Hashes {
	return wo.Body().InterlinkHashes()
}

func (wo *WorkObject) QiTransactions() []*Transaction {
	return wo.Body().QiTransactions()
}

func (wo *WorkObject) QuaiTransactions() []*Transaction {
	return wo.Body().QuaiTransactions()
}

func (wo *WorkObject) QiTransactionsWithoutCoinbase() []*Transaction {
	// TODO: cache the UTXO loop
	qiTxs := make([]*Transaction, 0)
	for i, t := range wo.Transactions() {
		if i == 0 && IsCoinBaseTx(t, wo.woHeader.parentHash, wo.woHeader.location) {
			// ignore the Qi coinbase tx
			continue
		}
		if t.Type() == QiTxType {
			qiTxs = append(qiTxs, t)
		}
	}
	return qiTxs
}

func (wo *WorkObject) QuaiTransactionsWithoutCoinbase() []*Transaction {
	quaiTxs := make([]*Transaction, 0)
	for i, t := range wo.Transactions() {
		if i == 0 && IsCoinBaseTx(t, wo.woHeader.parentHash, wo.woHeader.location) || t.Type() == QiTxType || (t.Type() == ExternalTxType && t.To().IsInQiLedgerScope()) {
			// ignore the Quai coinbase tx and Quai->Qi to comply with prior functionality as it is not a normal transaction
			continue
		}
		if t.Type() != QiTxType {
			quaiTxs = append(quaiTxs, t)
		}
	}
	return quaiTxs
}

func (wo *WorkObject) InputsAndOutputsWithoutCoinbase() (uint, uint) {
	inputs := 0
	outputs := 0
	for i, tx := range wo.Transactions() {
		if i == 0 && IsCoinBaseTx(tx, wo.woHeader.parentHash, wo.woHeader.location) {
			// ignore the Qi coinbase tx
			continue
		}
		if tx.Type() == QiTxType {
			inputs += len(tx.TxIn())
			outputs += len(tx.TxOut())
		}
	}
	return uint(inputs), uint(outputs)
}

func (wo *WorkObject) QuaiTransactionsWithFees() []*Transaction {
	quaiTxs := make([]*Transaction, 0)
	for _, t := range wo.Transactions() {
		if t.Type() == QuaiTxType { // QuaiTxType is the only type that gives Quai fees to the miner
			quaiTxs = append(quaiTxs, t)
		}
	}
	return quaiTxs
}

func (wo *WorkObject) NumberArray() []*big.Int {
	numArray := make([]*big.Int, common.HierarchyDepth)
	for i := 0; i < common.HierarchyDepth; i++ {
		numArray[i] = wo.Number(i)
	}
	return numArray
}

func (wo *WorkObject) SetMixHash(mixHash common.Hash) {
	wo.woHeader.mixHash = mixHash
}

////////////////////////////////////////////////////////////
/////////////////// Work Object Generic Setters ///////////////
////////////////////////////////////////////////////////////

func (wo *WorkObject) SetParentHash(val common.Hash, nodeCtx int) {
	if nodeCtx == common.ZONE_CTX {
		wo.WorkObjectHeader().SetParentHash(val)
	} else {
		wo.Body().Header().SetParentHash(val, nodeCtx)
	}
}

func (wo *WorkObject) SetNumber(val *big.Int, nodeCtx int) {
	if nodeCtx == common.ZONE_CTX {
		wo.WorkObjectHeader().SetNumber(val)
	} else {
		wo.Body().Header().SetNumber(val, nodeCtx)
	}
}

////////////////////////////////////////////////////////////
/////////////////// Work Object Header Getters ///////////////
////////////////////////////////////////////////////////////

func (wh *WorkObjectHeader) HeaderHash() common.Hash {
	return wh.headerHash
}

func (wh *WorkObjectHeader) ParentHash() common.Hash {
	return wh.parentHash
}

func (wh *WorkObjectHeader) Number() *big.Int {
	return wh.number
}

func (wh *WorkObjectHeader) NumberU64() uint64 {
	return wh.number.Uint64()
}

func (wh *WorkObjectHeader) Difficulty() *big.Int {
	return wh.difficulty
}

func (wh *WorkObjectHeader) TxHash() common.Hash {
	return wh.txHash
}

func (wh *WorkObjectHeader) Location() common.Location {
	return wh.location
}

func (wh *WorkObjectHeader) MixHash() common.Hash {
	return wh.mixHash
}

func (wh *WorkObjectHeader) Nonce() BlockNonce {
	return wh.nonce
}

func (wh *WorkObjectHeader) NonceU64() uint64 {
	return wh.nonce.Uint64()
}

func (wh *WorkObjectHeader) Time() uint64 {
	return wh.time
}

////////////////////////////////////////////////////////////
/////////////////// Work Object Header Setters ///////////////
////////////////////////////////////////////////////////////

func (wh *WorkObjectHeader) SetHeaderHash(headerHash common.Hash) {
	wh.headerHash = headerHash
}

func (wh *WorkObjectHeader) SetParentHash(parentHash common.Hash) {
	wh.parentHash = parentHash
}

func (wh *WorkObjectHeader) SetNumber(number *big.Int) {
	wh.number = number
}

func (wh *WorkObjectHeader) SetDifficulty(difficulty *big.Int) {
	wh.difficulty = difficulty
}

func (wh *WorkObjectHeader) SetTxHash(txHash common.Hash) {
	wh.txHash = txHash
}

func (wh *WorkObjectHeader) SetLocation(location common.Location) {
	wh.location = location
}

func (wh *WorkObjectHeader) SetMixHash(mixHash common.Hash) {
	wh.mixHash = mixHash
}

func (wh *WorkObjectHeader) SetNonce(nonce BlockNonce) {
	wh.nonce = nonce
}

func (wh *WorkObjectHeader) SetTime(val uint64) {
	wh.time = val
}

type WorkObjectBody struct {
	header          *Header
	transactions    Transactions
	extTransactions Transactions
	uncles          []*WorkObjectHeader
	manifest        BlockManifest
	interlinkHashes common.Hashes
}

////////////////////////////////////////////////////////////
/////////////////// Work Object Body Setters ///////////////
////////////////////////////////////////////////////////////

func (wb *WorkObjectBody) SetHeader(header *Header) {
	wb.header = header
}

func (wb *WorkObjectBody) SetTransactions(transactions []*Transaction) {
	wb.transactions = transactions
}

func (wb *WorkObjectBody) SetExtTransactions(transactions []*Transaction) {
	wb.extTransactions = transactions
}

func (wb *WorkObjectBody) SetUncles(uncles []*WorkObjectHeader) {
	wb.uncles = uncles
}

func (wb *WorkObjectBody) SetManifest(manifest BlockManifest) {
	wb.manifest = manifest
}

func (wb *WorkObjectBody) SetInterlinkHashes(interlinkHashes common.Hashes) {
	wb.interlinkHashes = interlinkHashes
}

////////////////////////////////////////////////////////////
/////////////////// Work Object Body Getters ///////////////
////////////////////////////////////////////////////////////

func (wb *WorkObjectBody) Header() *Header {
	return wb.header
}

func (wb *WorkObjectBody) Transactions() []*Transaction {
	return wb.transactions
}

func (wb *WorkObjectBody) ExtTransactions() []*Transaction {
	return wb.extTransactions
}

func (wb *WorkObjectBody) Uncles() []*WorkObjectHeader {
	return wb.uncles
}

func (wb *WorkObjectBody) Manifest() BlockManifest {
	return wb.manifest
}

func (wb *WorkObjectBody) InterlinkHashes() common.Hashes {
	return wb.interlinkHashes
}

func (wb *WorkObjectBody) QiTransactions() []*Transaction {
	// TODO: cache the UTXO loop
	qiTxs := make([]*Transaction, 0)
	for _, t := range wb.Transactions() {
		if t.Type() == QiTxType {
			qiTxs = append(qiTxs, t)
		}
	}
	return qiTxs
}

func (wb *WorkObjectBody) QuaiTransactions() []*Transaction {
	quaiTxs := make([]*Transaction, 0)
	for _, t := range wb.Transactions() {
		if t.Type() != QiTxType {
			quaiTxs = append(quaiTxs, t)
		}
	}
	return quaiTxs
}

func (wb *WorkObjectBody) ExternalTransactions() []*Transaction {
	etxs := make([]*Transaction, 0)
	for _, t := range wb.Transactions() {
		if t.Type() == ExternalTxType {
			etxs = append(etxs, t)
		}
	}
	return etxs
}

func CalcUncleHash(uncles []*WorkObjectHeader) common.Hash {
	if len(uncles) == 0 {
		return EmptyUncleHash
	}
	return RlpHash(uncles)
}

////////////////////////////////////////////////////////////
/////////////////// New Object Creation Methods ////////////
////////////////////////////////////////////////////////////

func NewWorkObject(woHeader *WorkObjectHeader, woBody *WorkObjectBody, tx *Transaction) *WorkObject {
	return &WorkObject{
		woHeader: woHeader,
		woBody:   woBody,
		tx:       tx,
	}
}

func NewWorkObjectWithHeaderAndTx(header *WorkObjectHeader, tx *Transaction) *WorkObject {
	return &WorkObject{woHeader: CopyWorkObjectHeader(header), tx: tx}
}

func (wo *WorkObject) WithBody(header *Header, txs []*Transaction, etxs []*Transaction, uncles []*WorkObjectHeader, manifest BlockManifest, interlinkHashes common.Hashes) *WorkObject {
	woBody := &WorkObjectBody{
		header:          CopyHeader(header),
		transactions:    make([]*Transaction, len(txs)),
		uncles:          make([]*WorkObjectHeader, len(uncles)),
		extTransactions: make([]*Transaction, len(etxs)),
		manifest:        make(BlockManifest, len(manifest)),
		interlinkHashes: make(common.Hashes, len(interlinkHashes)),
	}
	copy(woBody.transactions, txs)
	copy(woBody.uncles, uncles)
	copy(woBody.extTransactions, etxs)
	copy(woBody.manifest, manifest)
	copy(woBody.interlinkHashes, interlinkHashes)
	for i := range uncles {
		woBody.uncles[i] = CopyWorkObjectHeader(uncles[i])
	}

	newWo := &WorkObject{
		woHeader: CopyWorkObjectHeader(wo.woHeader),
		woBody:   woBody,
		tx:       wo.tx,
	}
	return newWo
}

func NewWorkObjectBody(header *Header, txs []*Transaction, etxs []*Transaction, uncles []*WorkObjectHeader, manifest BlockManifest, receipts []*Receipt, hasher TrieHasher, nodeCtx int) (*WorkObjectBody, error) {
	b := &WorkObjectBody{}
	b.SetHeader(CopyHeader(header))

	if len(txs) == 0 {
		b.Header().SetTxHash(EmptyRootHash)
	} else {
		b.Header().SetTxHash(DeriveSha(Transactions(txs), hasher))
		b.transactions = make(Transactions, len(txs))
		copy(b.transactions, txs)
	}

	if len(receipts) == 0 {
		b.Header().SetReceiptHash(EmptyRootHash)
	} else {
		b.Header().SetReceiptHash(DeriveSha(Receipts(receipts), hasher))
	}

	if len(uncles) == 0 {
		b.Header().SetUncleHash(EmptyUncleHash)
	} else {
		b.Header().SetUncleHash(CalcUncleHash(uncles))
		b.uncles = make([]*WorkObjectHeader, len(uncles))
		for i := range uncles {
			b.uncles[i] = CopyWorkObjectHeader(uncles[i])
		}
	}

	if len(etxs) == 0 {
		b.Header().SetEtxHash(EmptyRootHash)
	} else {
		b.Header().SetEtxHash(DeriveSha(Transactions(etxs), hasher))
		b.extTransactions = make(Transactions, len(etxs))
		copy(b.extTransactions, etxs)
	}

	// Since the subordinate's manifest lives in our body, we still need to check
	// that the manifest matches the subordinate's manifest hash, but we do not set
	// the subordinate's manifest hash.
	subManifestHash := EmptyRootHash
	if len(manifest) != 0 {
		subManifestHash = DeriveSha(manifest, hasher)
		b.manifest = make(BlockManifest, len(manifest))
		copy(b.manifest, manifest)
	}
	if nodeCtx < common.ZONE_CTX && subManifestHash != b.Header().ManifestHash(nodeCtx+1) {
		return nil, fmt.Errorf("attempted to build block with invalid subordinate manifest")
	}

	return b, nil
}

func NewWorkObjectWithHeader(header *WorkObject, tx *Transaction, nodeCtx int, woType WorkObjectView) *WorkObject {
	woHeader := NewWorkObjectHeader(header.Hash(), header.ParentHash(common.ZONE_CTX), header.Number(common.ZONE_CTX), header.woHeader.difficulty, header.woHeader.txHash, header.woHeader.nonce, header.woHeader.time, header.Location())
	woBody, _ := NewWorkObjectBody(header.Body().Header(), nil, nil, nil, nil, nil, nil, nodeCtx)
	return NewWorkObject(woHeader, woBody, tx)
}

func (wo *WorkObject) RPCMarshalWorkObject() map[string]interface{} {
	result := map[string]interface{}{
		"woHeader": wo.woHeader.RPCMarshalWorkObjectHeader(),
	}
	if wo.woBody != nil {
		result["woBody"] = wo.woBody.RPCMarshalWorkObjectBody()
	}
	if wo.tx != nil {
		result["tx"] = wo.tx
	}
	return result
}

func (wo *WorkObject) ProtoEncode(woType WorkObjectView) (*ProtoWorkObject, error) {
	switch woType {
	case PEtxObject:
		header, err := wo.woHeader.ProtoEncode()
		if err != nil {
			return nil, err
		}
		bodyHeader, err := wo.woBody.header.ProtoEncode()
		if err != nil {
			return nil, errors.New("error encoding work object body header")
		}
		return &ProtoWorkObject{
			WoHeader: header,
			WoBody:   &ProtoWorkObjectBody{Header: bodyHeader},
		}, nil
	default:
		header, err := wo.woHeader.ProtoEncode()
		if err != nil {
			return nil, err
		}
		body, err := wo.woBody.ProtoEncode()
		if err != nil {
			return nil, err
		}
		if wo.tx == nil {
			return &ProtoWorkObject{
				WoHeader: header,
				WoBody:   body,
			}, nil
		} else {
			tx, err := wo.tx.ProtoEncode()
			if err != nil {
				return nil, err
			}
			return &ProtoWorkObject{
				WoHeader: header,
				WoBody:   body,
				Tx:       tx,
			}, nil
		}
	}
}

func (wo *WorkObjectHeaderView) ProtoEncode() (*ProtoWorkObjectHeaderView, error) {
	header, err := wo.woHeader.ProtoEncode()
	if err != nil {
		return nil, err
	}
	body, err := wo.woBody.ProtoEncode()
	if err != nil {
		return nil, err
	}
	return &ProtoWorkObjectHeaderView{
		WoHeader: header,
		WoBody:   body,
	}, nil
}

func (wo *WorkObjectBlockView) ProtoEncode() (*ProtoWorkObjectBlockView, error) {
	header, err := wo.woHeader.ProtoEncode()
	if err != nil {
		return nil, err
	}
	body, err := wo.woBody.ProtoEncode()
	if err != nil {
		return nil, err
	}
	return &ProtoWorkObjectBlockView{
		WoHeader: header,
		WoBody:   body,
	}, nil
}

func (wo *WorkObjectHeaderView) ProtoDecode(data *ProtoWorkObjectHeaderView, location common.Location) error {
	wo.woHeader = new(WorkObjectHeader)
	err := wo.woHeader.ProtoDecode(data.GetWoHeader())
	if err != nil {
		return err
	}
	wo.woBody = new(WorkObjectBody)
	err = wo.woBody.ProtoDecode(data.GetWoBody(), location, BlockObject)
	if err != nil {
		return err
	}
	return nil
}

func (wob *WorkObjectBlockView) ProtoDecode(data *ProtoWorkObjectBlockView, location common.Location) error {
	wob.woHeader = new(WorkObjectHeader)
	err := wob.woHeader.ProtoDecode(data.GetWoHeader())
	if err != nil {
		return err
	}
	wob.woBody = new(WorkObjectBody)
	err = wob.woBody.ProtoDecode(data.GetWoBody(), location, BlockObject)
	if err != nil {
		return err
	}
	return nil
}

func (wo *WorkObject) ProtoDecode(data *ProtoWorkObject, location common.Location, woType WorkObjectView) error {
	// Get a new WorkObject from the pool
	newWo := workObjectPool.Get().(*WorkObject)
	defer workObjectPool.Put(newWo) // Ensure newWo is returned to the pool

	// Reset the new WorkObject fields
	newWo.woHeader = nil
	newWo.woBody = nil
	newWo.tx = nil

	newWo.woHeader = workObjectHeaderPool.Get().(*WorkObjectHeader)
	err := newWo.woHeader.ProtoDecode(data.GetWoHeader())
	if err != nil {
		workObjectHeaderPool.Put(newWo.woHeader)
		return err
	}

	newWo.woBody = workObjectBodyPool.Get().(*WorkObjectBody)
	switch woType {
	case PEtxObject:
		bodyHeader := headerPool.Get().(*Header)
		err = bodyHeader.ProtoDecode(data.GetWoBody().Header, location)
		if err != nil {
			workObjectBodyPool.Put(newWo.woBody)
			workObjectHeaderPool.Put(newWo.woHeader)
			headerPool.Put(bodyHeader)
			return err
		}
		newWo.woBody.SetHeader(bodyHeader)
	default:
		data := data.GetWoBody()
		newWo.woBody = workObjectBodyPool.Get().(*WorkObjectBody)
		var transactions []*Transaction
		var extTransactions []*Transaction
		switch woType {
		case WorkShareObject:
			newWo.woBody.header = headerPool.Get().(*Header)
			defer headerPool.Put(newWo.woBody.header)
			err := newWo.woBody.header.ProtoDecode(data.GetHeader(), location)
			if err != nil {
				return err
			}
			newWo.woBody.uncles = make([]*WorkObjectHeader, len(data.GetUncles().GetWoHeaders()))
			for i, protoUncle := range data.GetUncles().GetWoHeaders() {
				uncle := workObjectHeaderPool.Get().(*WorkObjectHeader)
				defer workObjectHeaderPool.Put(uncle)
				err = uncle.ProtoDecode(protoUncle)
				if err != nil {
					return err
				}
				newWo.woBody.uncles[i] = uncle
			}
		default:
			newWo.woBody.header = headerPool.Get().(*Header)
			defer headerPool.Put(newWo.woBody.header)
			err := newWo.woBody.header.ProtoDecode(data.GetHeader(), location)
			if err != nil {
				return err
			}
			transactionsProto := data.GetTransactions().GetTransactions()
			transactions = make([]*Transaction, len(transactionsProto))
			for i, protoTx := range transactionsProto {
				transactions[i] = transactionPool.Get().(*Transaction)
				defer transactionPool.Put(transactions[i])
				err = transactions[i].ProtoDecode(protoTx, location)
				if err != nil {
					return err
				}
			}
			extTransactionsProto := data.GetExtTransactions().GetTransactions()
			extTransactions = make([]*Transaction, len(extTransactionsProto))
			for i, protoTx := range extTransactionsProto {
				extTransactions[i] = transactionPool.Get().(*Transaction)
				defer transactionPool.Put(extTransactions[i])
				err = extTransactions[i].ProtoDecode(protoTx, location)
				if err != nil {
					return err
				}
			}
			newWo.woBody.uncles = make([]*WorkObjectHeader, len(data.GetUncles().GetWoHeaders()))
			for i, protoUncle := range data.GetUncles().GetWoHeaders() {
				uncle := workObjectHeaderPool.Get().(*WorkObjectHeader)
				defer workObjectHeaderPool.Put(uncle)
				err = uncle.ProtoDecode(protoUncle)
				if err != nil {
					return err
				}
				newWo.woBody.uncles[i] = uncle
			}

			newWo.woBody.manifest = make([]common.Hash, len(data.GetManifest().Manifest))
			for i, protoHash := range data.GetManifest().Manifest {
				newWo.woBody.manifest[i] = common.BytesToHash(protoHash.Value)
			}

			newWo.woBody.interlinkHashes = make([]common.Hash, len(data.GetInterlinkHashes().GetHashes()))
			for i, protoHash := range data.GetInterlinkHashes().GetHashes() {
				newWo.woBody.interlinkHashes[i] = common.BytesToHash(protoHash.Value)
			}
			// Assign the dereferenced value
		}

		newWo.woBody.SetTransactions(transactions)
		newWo.woBody.SetExtTransactions(extTransactions)

	}
	if data.Tx != nil {
		newWo.tx = transactionPool.Get().(*Transaction)
		err = newWo.tx.ProtoDecode(data.GetTx(), location)
		if err != nil {
			transactionPool.Put(newWo.tx)
			workObjectBodyPool.Put(newWo.woBody)
			workObjectHeaderPool.Put(newWo.woHeader)
			return err
		}
	}

	// Copy the temporary WorkObject to the target WorkObject
	wo.SetWorkObjectHeader(CopyWorkObjectHeader(newWo.WorkObjectHeader()))
	wo.SetBody(CopyWorkObjectBody(newWo.Body()))
	wo.SetTx(nil)

	// Put objects back into the pool after copying
	workObjectHeaderPool.Put(newWo.woHeader)
	workObjectBodyPool.Put(newWo.woBody)
	if newWo.tx != nil {
		transactionPool.Put(newWo.tx)
	}

	return nil
}
func (wb *WorkObjectBody) ProtoDecode(data *ProtoWorkObjectBody, location common.Location, woType WorkObjectView) error {
	newWb := workObjectBodyPool.Get().(*WorkObjectBody)
	var transactions []*Transaction
	var extTransactions []*Transaction
	switch woType {
	case WorkShareObject:
		newWb.header = headerPool.Get().(*Header)
		defer headerPool.Put(newWb.header)
		err := newWb.header.ProtoDecode(data.GetHeader(), location)
		if err != nil {
			return err
		}
		newWb.uncles = make([]*WorkObjectHeader, len(data.GetUncles().GetWoHeaders()))
		for i, protoUncle := range data.GetUncles().GetWoHeaders() {
			uncle := workObjectHeaderPool.Get().(*WorkObjectHeader)
			defer workObjectHeaderPool.Put(uncle)
			err = uncle.ProtoDecode(protoUncle)
			if err != nil {
				return err
			}
			wb.uncles[i] = uncle
		}
	default:
		newWb.header = headerPool.Get().(*Header)
		defer headerPool.Put(newWb.header)
		err := newWb.header.ProtoDecode(data.GetHeader(), location)
		if err != nil {
			return err
		}
		transactionsProto := data.GetTransactions().GetTransactions()
		transactions = make([]*Transaction, len(transactionsProto))
		for i, protoTx := range transactionsProto {
			transactions[i] = &Transaction{}
			err = transactions[i].ProtoDecode(protoTx, location)
			if err != nil {
				return err
			}
		}
		extTransactionsProto := data.GetExtTransactions().GetTransactions()
		extTransactions = make([]*Transaction, len(extTransactionsProto))
		for i, protoTx := range extTransactionsProto {
			extTransactions[i] = &Transaction{}
			err = extTransactions[i].ProtoDecode(protoTx, location)
			if err != nil {
				return err
			}
		}
		newWb.uncles = make([]*WorkObjectHeader, len(data.GetUncles().GetWoHeaders()))
		for i, protoUncle := range data.GetUncles().GetWoHeaders() {
			uncle := workObjectHeaderPool.Get().(*WorkObjectHeader)
			defer workObjectHeaderPool.Put(uncle)
			err = uncle.ProtoDecode(protoUncle)
			if err != nil {
				return err
			}
			newWb.uncles[i] = uncle
		}

		newWb.manifest = make([]common.Hash, len(data.GetManifest().Manifest))
		for i, protoHash := range data.GetManifest().Manifest {
			newWb.manifest[i] = common.BytesToHash(protoHash.Value)
		}

		newWb.interlinkHashes = make([]common.Hash, len(data.GetInterlinkHashes().GetHashes()))
		for i, protoHash := range data.GetInterlinkHashes().GetHashes() {
			newWb.interlinkHashes[i] = common.BytesToHash(protoHash.Value)
		}
		// Assign the dereferenced value
	}

	wb.SetHeader(CopyHeader(newWb.Header()))
	wb.SetTransactions(transactions)
	wb.SetExtTransactions(extTransactions)
	wb.SetUncles(CopyWorkObjectHeaders(newWb.Uncles()))
	copy(wb.manifest, newWb.Manifest())
	copy(wb.interlinkHashes, newWb.InterlinkHashes())

	return nil
}

func NewWorkObjectHeader(headerHash common.Hash, parentHash common.Hash, number *big.Int, difficulty *big.Int, txHash common.Hash, nonce BlockNonce, time uint64, location common.Location) *WorkObjectHeader {
	return &WorkObjectHeader{
		headerHash: headerHash,
		parentHash: parentHash,
		number:     number,
		difficulty: difficulty,
		txHash:     txHash,
		nonce:      nonce,
		time:       time,
		location:   location,
	}
}

func CopyWorkObjectHeader(wh *WorkObjectHeader) *WorkObjectHeader {
	cpy := *wh
	cpy.SetHeaderHash(wh.HeaderHash())
	cpy.SetParentHash(wh.ParentHash())
	cpy.SetNumber(new(big.Int).Set(wh.Number()))
	cpy.SetDifficulty(new(big.Int).Set(wh.Difficulty()))
	cpy.SetTxHash(wh.TxHash())
	cpy.SetNonce(wh.Nonce())
	cpy.SetMixHash(wh.MixHash())
	cpy.SetLocation(wh.Location())
	cpy.SetTime(wh.Time())
	return &cpy
}

func CopyWorkObjectHeaders(woHeaders []*WorkObjectHeader) []*WorkObjectHeader {
	cpy := make([]*WorkObjectHeader, len(woHeaders))
	for i, wo := range woHeaders {
		cpy[i] = CopyWorkObjectHeader(wo)
	}
	return cpy
}

func (wh *WorkObjectHeader) RPCMarshalWorkObjectHeader() map[string]interface{} {
	result := map[string]interface{}{
		"headerHash": wh.HeaderHash(),
		"parentHash": wh.ParentHash(),
		"number":     (*hexutil.Big)(wh.Number()),
		"difficulty": (*hexutil.Big)(wh.Difficulty()),
		"nonce":      wh.Nonce(),
		"location":   hexutil.Bytes(wh.Location()),
		"txHash":     wh.TxHash(),
		"time":       hexutil.Uint64(wh.Time()),
		"mixHash":    wh.MixHash(),
	}
	return result
}

func (wh *WorkObjectHeader) Hash() (hash common.Hash) {
	sealHash := wh.SealHash().Bytes()
	hasherMu.Lock()
	defer hasherMu.Unlock()
	hasher.Reset()
	var hData [40]byte
	copy(hData[:], wh.Nonce().Bytes())
	copy(hData[len(wh.nonce):], sealHash)
	sum := blake3.Sum256(hData[:])
	hash.SetBytes(sum[:])
	return hash
}

func (wh *WorkObjectHeader) SealHash() (hash common.Hash) {
	hasherMu.Lock()
	defer hasherMu.Unlock()
	hasher.Reset()
	protoSealData := wh.SealEncode()
	data, err := proto.Marshal(protoSealData)
	if err != nil {
		log.Global.Error("Failed to marshal seal data ", "err", err)
	}
	sum := blake3.Sum256(data[:])
	hash.SetBytes(sum[:])
	return hash
}

func (wh *WorkObjectHeader) SealEncode() *ProtoWorkObjectHeader {
	hash := common.ProtoHash{Value: wh.HeaderHash().Bytes()}
	parentHash := common.ProtoHash{Value: wh.ParentHash().Bytes()}
	txHash := common.ProtoHash{Value: wh.TxHash().Bytes()}
	number := wh.Number().Bytes()
	difficulty := wh.Difficulty().Bytes()
	location := wh.Location().ProtoEncode()
	time := wh.Time()

	return &ProtoWorkObjectHeader{
		HeaderHash: &hash,
		ParentHash: &parentHash,
		Number:     number,
		Difficulty: difficulty,
		TxHash:     &txHash,
		Location:   location,
		Time:       &time,
	}
}

func (wh *WorkObjectHeader) ProtoEncode() (*ProtoWorkObjectHeader, error) {
	hash := common.ProtoHash{Value: wh.HeaderHash().Bytes()}
	parentHash := common.ProtoHash{Value: wh.ParentHash().Bytes()}
	txHash := common.ProtoHash{Value: wh.TxHash().Bytes()}
	number := wh.Number().Bytes()
	difficulty := wh.Difficulty().Bytes()
	location := wh.Location().ProtoEncode()
	nonce := wh.Nonce().Uint64()
	mixHash := common.ProtoHash{Value: wh.MixHash().Bytes()}

	return &ProtoWorkObjectHeader{
		HeaderHash: &hash,
		ParentHash: &parentHash,
		Number:     number,
		Difficulty: difficulty,
		TxHash:     &txHash,
		Location:   location,
		Nonce:      &nonce,
		MixHash:    &mixHash,
		Time:       &wh.time,
	}, nil
}

func (wh *WorkObjectHeader) ProtoDecode(data *ProtoWorkObjectHeader) error {
	if data.HeaderHash == nil || data.ParentHash == nil || data.Number == nil || data.Difficulty == nil || data.TxHash == nil || data.Nonce == nil || data.Location == nil {
		return errors.New("failed to decode work object header")
	}
	wh.SetHeaderHash(common.BytesToHash(data.GetHeaderHash().Value))
	wh.SetParentHash(common.BytesToHash(data.GetParentHash().Value))
	wh.SetNumber(new(big.Int).SetBytes(data.GetNumber()))
	wh.SetDifficulty(new(big.Int).SetBytes(data.Difficulty))
	wh.SetTxHash(common.BytesToHash(data.GetTxHash().Value))
	wh.SetNonce(uint64ToByteArr(data.GetNonce()))
	wh.SetLocation(data.GetLocation().GetValue())
	wh.SetMixHash(common.BytesToHash(data.GetMixHash().Value))
	wh.SetTime(data.GetTime())

	return nil
}

func CopyWorkObjectBody(wb *WorkObjectBody) *WorkObjectBody {
	cpy := &WorkObjectBody{header: CopyHeader(wb.header)}

	cpy.transactions = make(Transactions, len(wb.transactions))
	for i, tx := range wb.transactions {
		cpy.transactions[i] = CopyTransaction(tx)
	}

	cpy.extTransactions = make(Transactions, len(wb.extTransactions))
	for i, tx := range wb.extTransactions {
		cpy.extTransactions[i] = CopyTransaction(tx)
	}

	cpy.uncles = make([]*WorkObjectHeader, len(wb.uncles))
	for i, uncle := range wb.uncles {
		cpy.uncles[i] = CopyWorkObjectHeader(uncle)
	}

	cpy.manifest = make(BlockManifest, len(wb.manifest))
	copy(cpy.manifest, wb.manifest)

	cpy.interlinkHashes = make(common.Hashes, len(wb.interlinkHashes))
	copy(cpy.interlinkHashes, wb.interlinkHashes)

	return cpy
}

func (wb *WorkObjectBody) ProtoEncode() (*ProtoWorkObjectBody, error) {
	header, err := wb.header.ProtoEncode()
	if err != nil {
		return nil, err
	}

	protoTransactions, err := wb.transactions.ProtoEncode()
	if err != nil {
		return nil, err
	}

	protoExtTransactions, err := wb.extTransactions.ProtoEncode()
	if err != nil {
		return nil, err
	}

	protoUncles := &ProtoWorkObjectHeaders{}
	for _, unc := range wb.uncles {
		protoUncle, err := unc.ProtoEncode()
		if err != nil {
			return nil, err
		}
		protoUncles.WoHeaders = append(protoUncles.WoHeaders, protoUncle)
	}

	protoManifest, err := wb.manifest.ProtoEncode()
	if err != nil {
		return nil, err
	}

	protoInterlinkHashes := wb.interlinkHashes.ProtoEncode()

	return &ProtoWorkObjectBody{
		Header:          header,
		Transactions:    protoTransactions,
		ExtTransactions: protoExtTransactions,
		Uncles:          protoUncles,
		Manifest:        protoManifest,
		InterlinkHashes: protoInterlinkHashes,
	}, nil
}

func (wb *WorkObjectBody) ProtoDecodeHeader(data *ProtoWorkObjectBody, location common.Location) error {
	wb.header = &Header{}
	return wb.header.ProtoDecode(data.GetHeader(), location)
}

func (wb *WorkObjectBody) RPCMarshalWorkObjectBody() map[string]interface{} {
	result := map[string]interface{}{
		"header":          wb.header.RPCMarshalHeader(),
		"transactions":    wb.Transactions(),
		"extTransactions": wb.ExtTransactions(),
		"manifest":        wb.Manifest(),
		"interlinkHashes": wb.InterlinkHashes(),
	}

	workedUncles := make([]map[string]interface{}, len(wb.Uncles()))
	for i, uncle := range wb.Uncles() {
		workedUncles[i] = uncle.RPCMarshalWorkObjectHeader()
	}
	result["uncles"] = workedUncles

	return result
}

func CopyWorkObject(wo *WorkObject) *WorkObject {
	newWo := &WorkObject{
		woHeader: CopyWorkObjectHeader(wo.woHeader),
		woBody:   CopyWorkObjectBody(wo.woBody),
		tx:       wo.tx,
	}
	return newWo
}

////////////////////////////////////////////////////////////
///////////////////// Work Object Views ////////////////////
////////////////////////////////////////////////////////////

type WorkObjectBlockView struct {
	*WorkObject
}

type WorkObjectHeaderView struct {
	*WorkObject
}

////////////////////////////////////////////////////////////
////////////// View Conversion/Getter Methods //////////////
////////////////////////////////////////////////////////////

func (wo *WorkObject) ConvertToHeaderView() *WorkObjectHeaderView {
	newWo := NewWorkObject(wo.woHeader, wo.woBody, wo.tx)
	newWo.Body().SetTransactions(Transactions{})
	newWo.Body().SetInterlinkHashes(common.Hashes{})
	return &WorkObjectHeaderView{
		WorkObject: newWo,
	}
}

func (wo *WorkObject) ConvertToBlockView() *WorkObjectBlockView {
	return &WorkObjectBlockView{
		WorkObject: wo,
	}
}
