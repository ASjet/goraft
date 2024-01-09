package raft

import (
	llog "log"
	"runtime"

	"github.com/sirupsen/logrus"
)

// Debugging
const (
	debug         = false
	BacktraceLock = false
)

type LogLevel int

const (
	LevelTrace LogLevel = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

func init() {
	llog.SetFlags(llog.Lmicroseconds)
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(logrus.StandardLogger().Formatter)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		llog.Printf(format, a...)
	}
	return
}

var (
	defaultLogger = NewLogger(LevelInfo)
)

func Trace(msg string, args ...interface{}) {
	defaultLogger.Trace(msg, args...)
	// logrus.Tracef(msg, args...)
}

func Debug(msg string, args ...interface{}) {
	defaultLogger.Debug(msg, args...)
	// logrus.Debugf(msg, args...)
}

func Info(msg string, args ...interface{}) {
	defaultLogger.Info(msg, args...)
	// logrus.Infof(msg, args...)
}

func Warn(msg string, args ...interface{}) {
	defaultLogger.Warn(msg, args...)
	// logrus.Warnf(msg, args...)
}

func Error(msg string, args ...interface{}) {
	defaultLogger.Error(msg, args...)
	// logrus.Errorf(msg, args...)
}

func Fatal(msg string, args ...interface{}) {
	defaultLogger.Fatal(msg, args...)
	// logrus.Fatalf(msg, args...)
}

type Logger struct {
	level LogLevel
}

func NewLogger(level LogLevel) *Logger {
	return &Logger{level: level}
}

func (l *Logger) Trace(msg string, args ...interface{}) {
	if l.level <= LevelTrace {
		llog.Printf("[TRAC]"+msg, args...)
	}
}

func (l *Logger) Debug(msg string, args ...interface{}) {
	if l.level <= LevelDebug {
		llog.Printf("[DEBG]"+msg, args...)
	}
}

func (l *Logger) Info(msg string, args ...interface{}) {
	if l.level <= LevelInfo {
		llog.Printf("[INFO]"+msg, args...)
	}
}

func (l *Logger) Warn(msg string, args ...interface{}) {
	if l.level <= LevelWarn {
		llog.Printf("[WARN]"+msg, args...)
	}
}

func (l *Logger) Error(msg string, args ...interface{}) {
	if l.level <= LevelError {
		llog.Printf("[EROR]"+msg, args...)
	}
}

func (l *Logger) Fatal(msg string, args ...interface{}) {
	if l.level <= LevelFatal {
		llog.Fatalf("[FATL]"+msg, args...)
	}
}

func Call() (name, file string, line int) {
	pc, f, l, ok := runtime.Caller(2)
	if !ok {
		panic("get call stack failed")
	}
	return runtime.FuncForPC(pc).Name(), f, l
}
