package logging

func NewComposite(logger ...Logger) Logger {
	return &compositeLogger{loggers: logger}
}

// Implement the Logger interface
type compositeLogger struct {
	loggers []Logger
}

func (c *compositeLogger) Errorf(f string, v ...interface{}) {
	for _, l := range c.loggers {
		l.Errorf(f, v...)
	}
}

func (c *compositeLogger) Warningf(f string, v ...interface{}) {
	for _, l := range c.loggers {
		l.Warningf(f, v...)
	}
}

func (c *compositeLogger) Infof(f string, v ...interface{}) {
	for _, l := range c.loggers {
		l.Infof(f, v...)
	}
}

func (c *compositeLogger) Debugf(f string, v ...interface{}) {
	for _, l := range c.loggers {
		l.Debugf(f, v...)
	}
}
