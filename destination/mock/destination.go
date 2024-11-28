// Code generated by MockGen. DO NOT EDIT.
// Source: destination/destination.go
//
// Generated by this command:
//
//	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
//

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	sdk "github.com/conduitio/conduit-connector-sdk"
	gomock "go.uber.org/mock/gomock"
)

// MockWriter is a mock of Writer interface.
type MockWriter struct {
	ctrl     *gomock.Controller
	recorder *MockWriterMockRecorder
	isgomock struct{}
}

// MockWriterMockRecorder is the mock recorder for MockWriter.
type MockWriterMockRecorder struct {
	mock *MockWriter
}

// NewMockWriter creates a new mock instance.
func NewMockWriter(ctrl *gomock.Controller) *MockWriter {
	mock := &MockWriter{ctrl: ctrl}
	mock.recorder = &MockWriterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWriter) EXPECT() *MockWriterMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockWriter) Close(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockWriterMockRecorder) Close(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockWriter)(nil).Close), ctx)
}

// InsertRecord mocks base method.
func (m *MockWriter) InsertRecord(ctx context.Context, record sdk.Record) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InsertRecord", ctx, record)
	ret0, _ := ret[0].(error)
	return ret0
}

// InsertRecord indicates an expected call of InsertRecord.
func (mr *MockWriterMockRecorder) InsertRecord(ctx, record any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InsertRecord", reflect.TypeOf((*MockWriter)(nil).InsertRecord), ctx, record)
}

// SetColumnTypes mocks base method.
func (m *MockWriter) SetColumnTypes(cl map[string]string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetColumnTypes", cl)
}

// SetColumnTypes indicates an expected call of SetColumnTypes.
func (mr *MockWriterMockRecorder) SetColumnTypes(cl any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetColumnTypes", reflect.TypeOf((*MockWriter)(nil).SetColumnTypes), cl)
}
