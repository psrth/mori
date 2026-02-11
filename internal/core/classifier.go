package core

// Classifier parses a query into a Classification.
// Each engine implements this interface with its own parser.
type Classifier interface {
	// Classify parses a SQL/command string and returns its classification.
	Classify(query string) (*Classification, error)

	// ClassifyWithParams classifies a parameterized query with bound values.
	// Used for the extended query protocol (Parse/Bind/Execute).
	ClassifyWithParams(query string, params []interface{}) (*Classification, error)
}
