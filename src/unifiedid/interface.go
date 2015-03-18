package unifiedid

type UnifiedIDFactory interface {
	NextUnifiedID() uint64
}
