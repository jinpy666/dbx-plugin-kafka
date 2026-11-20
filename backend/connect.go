package main

import (
	"encoding/json"
	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
	"io.dbx.kafka.plugin/internal/kafkaconn"
)

func (h *pluginHandler) connectCall(method string, params json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	var req kafkaconn.ConnectRequest
	if perr := decodeParams(params, &req); perr != nil {
		return nil, perr
	}
	result, err := h.svc.ConnectCall(getContext(), method, req)
	if err != nil {
		return nil, bizError(err)
	}
	return result, nil
}
