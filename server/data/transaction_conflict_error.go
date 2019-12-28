package data

type TransactionConflictError struct{}

func NewTransactionConflictError() *TransactionConflictError {
	return &TransactionConflictError{}
}

func (e *TransactionConflictError) Error() string {
	return "transaction conflicts"
}
