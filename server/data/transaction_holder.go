package data

type TransactionHolder struct {
	transactionMap map[int64]*Transaction
}

func NewTransactionHolder() *TransactionHolder {
	return &TransactionHolder{transactionMap: map[int64]*Transaction{}}
}

func (h *TransactionHolder) Add(trx *Transaction) bool {
	if _, ok := h.transactionMap[trx.Number]; ok {
		return false
	}
	h.transactionMap[trx.Number] = trx
	return true
}

func (h *TransactionHolder) Get(num int64) *Transaction {
	if num == -1 {
		return CreateImmediateTransaction()
	}
	trx, ok := h.transactionMap[num]
	if !ok {
		return nil
	}
	return trx
}
