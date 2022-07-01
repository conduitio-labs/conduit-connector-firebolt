// Code generated by MockGen. DO NOT EDIT.
// Source: source/source.go

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	firebolt "github.com/conduitio-labs/conduit-connector-firebolt/firebolt"
	sdk "github.com/conduitio/conduit-connector-sdk"
	gomock "github.com/golang/mock/gomock"
)

// MockIterator is a mock of Iterator interface.
type MockIterator struct {
	ctrl     *gomock.Controller
	recorder *MockIteratorMockRecorder
}

// MockIteratorMockRecorder is the mock recorder for MockIterator.
type MockIteratorMockRecorder struct {
	mock *MockIterator
}

// NewMockIterator creates a new mock instance.
func NewMockIterator(ctrl *gomock.Controller) *MockIterator {
	mock := &MockIterator{ctrl: ctrl}
	mock.recorder = &MockIteratorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockIterator) EXPECT() *MockIteratorMockRecorder {
	return m.recorder
}

// Ack mocks base method.
func (m *MockIterator) Ack(p sdk.Position) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ack", p)
	ret0, _ := ret[0].(error)
	return ret0
}

// Ack indicates an expected call of Ack.
func (mr *MockIteratorMockRecorder) Ack(p interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockIterator)(nil).Ack), p)
}

// HasNext mocks base method.
func (m *MockIterator) HasNext(ctx context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HasNext", ctx)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HasNext indicates an expected call of HasNext.
func (mr *MockIteratorMockRecorder) HasNext(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HasNext", reflect.TypeOf((*MockIterator)(nil).HasNext), ctx)
}

// Next mocks base method.
func (m *MockIterator) Next(ctx context.Context) (sdk.Record, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next", ctx)
	ret0, _ := ret[0].(sdk.Record)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Next indicates an expected call of Next.
func (mr *MockIteratorMockRecorder) Next(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockIterator)(nil).Next), ctx)
}

// Setup mocks base method.
func (m *MockIterator) Setup(ctx context.Context, p sdk.Position) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Setup", ctx, p)
	ret0, _ := ret[0].(error)
	return ret0
}

// Setup indicates an expected call of Setup.
func (mr *MockIteratorMockRecorder) Setup(ctx, p interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Setup", reflect.TypeOf((*MockIterator)(nil).Setup), ctx, p)
}

// Stop mocks base method.
func (m *MockIterator) Stop(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockIteratorMockRecorder) Stop(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockIterator)(nil).Stop), ctx)
}

// MockFireboltClient is a mock of FireboltClient interface.
type MockFireboltClient struct {
	ctrl     *gomock.Controller
	recorder *MockFireboltClientMockRecorder
}

// MockFireboltClientMockRecorder is the mock recorder for MockFireboltClient.
type MockFireboltClientMockRecorder struct {
	mock *MockFireboltClient
}

// NewMockFireboltClient creates a new mock instance.
func NewMockFireboltClient(ctrl *gomock.Controller) *MockFireboltClient {
	mock := &MockFireboltClient{ctrl: ctrl}
	mock.recorder = &MockFireboltClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFireboltClient) EXPECT() *MockFireboltClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockFireboltClient) Close(ctx context.Context) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close", ctx)
}

// Close indicates an expected call of Close.
func (mr *MockFireboltClientMockRecorder) Close(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockFireboltClient)(nil).Close), ctx)
}

// IsEngineStarted mocks base method.
func (m *MockFireboltClient) IsEngineStarted(ctx context.Context) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsEngineStarted", ctx)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsEngineStarted indicates an expected call of IsEngineStarted.
func (mr *MockFireboltClientMockRecorder) IsEngineStarted(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsEngineStarted", reflect.TypeOf((*MockFireboltClient)(nil).IsEngineStarted), ctx)
}

// Login mocks base method.
func (m *MockFireboltClient) Login(ctx context.Context, params firebolt.LoginParams) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Login", ctx, params)
	ret0, _ := ret[0].(error)
	return ret0
}

// Login indicates an expected call of Login.
func (mr *MockFireboltClientMockRecorder) Login(ctx, params interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Login", reflect.TypeOf((*MockFireboltClient)(nil).Login), ctx, params)
}

// RunQuery mocks base method.
func (m *MockFireboltClient) RunQuery(ctx context.Context, query string) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RunQuery", ctx, query)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RunQuery indicates an expected call of RunQuery.
func (mr *MockFireboltClientMockRecorder) RunQuery(ctx, query interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RunQuery", reflect.TypeOf((*MockFireboltClient)(nil).RunQuery), ctx, query)
}

// StartEngine mocks base method.
func (m *MockFireboltClient) StartEngine(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartEngine", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// StartEngine indicates an expected call of StartEngine.
func (mr *MockFireboltClientMockRecorder) StartEngine(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartEngine", reflect.TypeOf((*MockFireboltClient)(nil).StartEngine), ctx)
}
