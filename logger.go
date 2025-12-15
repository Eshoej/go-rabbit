package gorabbit

type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

type Field struct {
	Key   string
	Value any
}

func NewField(key string, value any) Field {
	return Field{Key: key, Value: value}
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...Field) {}
func (nopLogger) Info(string, ...Field)  {}
func (nopLogger) Warn(string, ...Field)  {}
func (nopLogger) Error(string, ...Field) {}
