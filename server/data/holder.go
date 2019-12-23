package data

type TransactionHolder struct {
	transactionMap map[int]*Transaction
}

func NewHolder() *TransactionHolder {
	return &TransactionHolder{transactionMap: map[int]*Transaction{}}
}

func (h *TransactionHolder) Add(trx *Transaction) bool {
	if _, ok := h.transactionMap[trx.Number]; ok {
		return false
	}
	h.transactionMap[trx.Number] = trx
	return true
}

func (h *TransactionHolder) Get(num int) *Transaction {
	if num == -1 {
		return ImmediateTransaction
	}
	trx, ok := h.transactionMap[num]
	if !ok {
		return nil
	}
	return trx
}
