package writermsg

// WebWriterStart contains a message published by web writer at startup
type WebWriterStart struct {
	MessageType    string `json:"message-type"`
	TimestampRepr  string `json:"timestamp_repr"`
	SourceNodeName string `json:"source_node_name"`
}
