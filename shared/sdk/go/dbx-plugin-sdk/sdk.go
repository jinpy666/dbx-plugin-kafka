package dbxpluginsdk

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const ProtocolVersion = 1

const maxJSONBytes = 8 * 1024 * 1024

type Metadata struct {
	ID           string
	Version      string
	Capabilities []string
}

type RequestContext struct {
	RequestID json.RawMessage
	Driver    string
}

type PluginError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func NewError(code int, message string) *PluginError {
	return &PluginError{Code: code, Message: message}
}

func MethodNotFound(method string) *PluginError {
	return NewError(-32601, fmt.Sprintf("Method not found: %s", method))
}

type Handler interface {
	Handle(context RequestContext, method string, params json.RawMessage, emitter *Emitter) (any, *PluginError)
}

type HandlerFunc func(context RequestContext, method string, params json.RawMessage, emitter *Emitter) (any, *PluginError)

func (handler HandlerFunc) Handle(
	context RequestContext,
	method string,
	params json.RawMessage,
	emitter *Emitter,
) (any, *PluginError) {
	return handler(context, method, params, emitter)
}

type Emitter struct {
	writer io.Writer
	mutex  *sync.Mutex
}

func (emitter *Emitter) Event(method string, params any) *PluginError {
	if !validProtocolName(method) {
		return NewError(-32600, "Invalid event method")
	}
	return emitter.write(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	})
}

func (emitter *Emitter) respond(id json.RawMessage, result any, pluginError *PluginError) *PluginError {
	response := map[string]any{"jsonrpc": "2.0", "id": id}
	if pluginError != nil {
		response["error"] = pluginError
	} else {
		response["result"] = result
	}
	return emitter.write(response)
}

func (emitter *Emitter) write(value any) *PluginError {
	payload, err := json.Marshal(value)
	if err != nil {
		return NewError(-32603, err.Error())
	}
	if len(payload) > maxJSONBytes {
		return NewError(-32600, "JSON message is too large")
	}
	emitter.mutex.Lock()
	defer emitter.mutex.Unlock()
	if _, err := emitter.writer.Write(append(payload, '\n')); err != nil {
		return NewError(-32000, err.Error())
	}
	return nil
}

type Server struct {
	metadata Metadata
	handler  Handler
	input    io.Reader
	output   io.Writer
	errors   io.Writer
}

func NewServer(metadata Metadata, handler Handler) *Server {
	return &Server{
		metadata: metadata,
		handler:  handler,
		input:    os.Stdin,
		output:   os.Stdout,
		errors:   os.Stderr,
	}
}

func (server *Server) WithIO(input io.Reader, output io.Writer, errorsWriter io.Writer) *Server {
	server.input = input
	server.output = output
	server.errors = errorsWriter
	return server
}

// dispatchSafe 调用 handler 并把 panic 转为 -32603 插件错误（评审 H-2）：
// 单个畸形请求引发的 panic 不得带崩插件进程。
func (server *Server) dispatchSafe(
	ctx RequestContext,
	method string,
	params json.RawMessage,
	emitter *Emitter,
) (result any, pluginError *PluginError) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result, pluginError = nil, NewError(-32603, fmt.Sprintf("internal error: %v", recovered))
		}
	}()
	return server.handler.Handle(ctx, method, params, emitter)
}

func (server *Server) Serve() error {
	if server.handler == nil {
		return errors.New("plugin handler is required")
	}
	if !validProtocolName(server.metadata.ID) {
		return errors.New("plugin id is invalid")
	}
	emitter := &Emitter{writer: server.output, mutex: &sync.Mutex{}}
	reader := bufio.NewReaderSize(server.input, 64*1024)
	var workers sync.WaitGroup
	for {
		payload, tooLong, readErr := readSDKLine(reader)
		if tooLong {
			// 有界拒绝（评审 M-5）：此前 bufio.Scanner 报 ErrTooLong 终止
			// Serve → main log.Fatal 进程退出；这里拒绝该行，进程存活。
			if writeError := emitter.respond(json.RawMessage("null"), nil, NewError(-32700,
				fmt.Sprintf("Parse error: request line exceeds %d bytes", maxJSONBytes))); writeError != nil {
				fmt.Fprintf(server.errors, "[dbx-plugin-sdk-go] failed to write response: %s\n", writeError.Message)
			}
		} else if trimmed := bytes.TrimSpace(payload); len(trimmed) > 0 {
			request, decodeErr := decodeRequest(trimmed)
			if decodeErr != nil {
				// 畸形请求结构化处置（评审 M-5）：静默丢弃会让同步等响应的
				// 宿主永久挂起。解析失败 -32700（规约要求 id 置 null）与带
				// 可关联 id 的信封错误都应答；无 id / null id 的信封错误
				// （通知类、坏帧）记 stderr 继续。
				code := -32600
				message := "Invalid request: " + decodeErr.Error()
				var probe any
				if jsonErr := json.Unmarshal(trimmed, &probe); jsonErr != nil {
					code = -32700
					message = "Parse error: " + jsonErr.Error()
				}
				var envelope struct {
					ID json.RawMessage `json:"id"`
				}
				_ = json.Unmarshal(trimmed, &envelope)
				switch {
				case len(envelope.ID) != 0 && string(envelope.ID) != "null":
					if writeError := emitter.respond(envelope.ID, nil, NewError(code, message)); writeError != nil {
						fmt.Fprintf(server.errors, "[dbx-plugin-sdk-go] failed to write response: %s\n", writeError.Message)
					}
				case code == -32700:
					if writeError := emitter.respond(json.RawMessage("null"), nil, NewError(code, message)); writeError != nil {
						fmt.Fprintf(server.errors, "[dbx-plugin-sdk-go] failed to write response: %s\n", writeError.Message)
					}
				default:
					fmt.Fprintf(server.errors, "[dbx-plugin-sdk-go] %s\n", message)
				}
			} else if request.Method == "plugin/initialize" {
				if len(request.ID) == 0 {
					fmt.Fprintln(server.errors, "[dbx-plugin-sdk-go] plugin/initialize must be a request")
					continue
				}
				result, pluginError := server.initialize(request.Params)
				if writeError := emitter.respond(request.ID, result, pluginError); writeError != nil {
					return errors.New(writeError.Message)
				}
				continue
			} else {
				workers.Add(1)
				go func(request protocolRequest) {
					defer workers.Done()
					result, pluginError := server.dispatchSafe(
						RequestContext{RequestID: request.ID, Driver: request.Driver},
						request.Method,
						request.Params,
						emitter,
					)
					if len(request.ID) == 0 {
						if pluginError != nil {
							fmt.Fprintf(server.errors, "[dbx-plugin-sdk-go] %s\n", pluginError.Message)
						}
						return
					}
					if writeError := emitter.respond(request.ID, result, pluginError); writeError != nil {
						fmt.Fprintf(server.errors, "[dbx-plugin-sdk-go] failed to write response: %s\n", writeError.Message)
					}
				}(request)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return readErr
		}
	}
	workers.Wait()
	return nil
}

// readSDKLine 有界读一行：增量累积到 maxJSONBytes+1 即判超限（内容丢弃，
// 继续消费至行尾后返回 tooLong），拒绝报文与后续行的服务都不受影响。
// 取代 bufio.Scanner（评审 M-5：Scanner 超限即 ErrTooLong 终止 Serve）。
func readSDKLine(reader *bufio.Reader) (line []byte, tooLong bool, err error) {
	var buf []byte
	for {
		chunk, ferr := reader.ReadSlice('\n')
		switch {
		case tooLong:
			switch ferr {
			case nil, io.EOF:
				return nil, true, nil
			case bufio.ErrBufferFull:
				continue
			default:
				return nil, true, ferr
			}
		case len(buf)+len(chunk) > maxJSONBytes+1:
			switch ferr {
			case nil, io.EOF:
				return nil, true, nil
			case bufio.ErrBufferFull:
				tooLong = true
				buf = nil
				continue
			default:
				return nil, true, ferr
			}
		default:
			buf = append(buf, chunk...)
			switch ferr {
			case nil:
				return bytes.TrimRight(buf, "\r\n"), false, nil
			case io.EOF:
				return bytes.TrimRight(buf, "\r\n"), false, io.EOF
			case bufio.ErrBufferFull:
				continue
			default:
				return nil, false, ferr
			}
		}
	}
}

func (server *Server) initialize(params json.RawMessage) (any, *PluginError) {
	var request struct {
		Host struct {
			ProtocolVersions []int `json:"protocolVersions"`
		} `json:"host"`
	}
	if err := json.Unmarshal(params, &request); err != nil {
		return nil, NewError(-32602, "Invalid initialize parameters")
	}
	for _, version := range request.Host.ProtocolVersions {
		if version == ProtocolVersion {
			return map[string]any{
				"protocolVersion": ProtocolVersion,
				"capabilities":    server.metadata.Capabilities,
				"plugin": map[string]string{
					"id":      server.metadata.ID,
					"version": server.metadata.Version,
				},
			}, nil
		}
	}
	return nil, NewError(-32001, "DBX and plugin do not share a protocol version")
}

type protocolRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Driver  string          `json:"driver"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func decodeRequest(payload []byte) (protocolRequest, error) {
	var request protocolRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		return request, err
	}
	if request.JSONRPC != "2.0" {
		return request, errors.New("request does not declare jsonrpc 2.0")
	}
	if !validProtocolName(request.Method) {
		return request, errors.New("request method is invalid")
	}
	if len(request.Params) == 0 {
		request.Params = json.RawMessage("null")
	}
	return request, nil
}

func validProtocolName(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for index, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		if index > 0 && (character == '.' || character == '_' || character == ':' || character == '/' || character == '-') {
			continue
		}
		return false
	}
	return true
}
