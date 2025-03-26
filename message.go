// Package bus implements a publish-subscribe pattern on top of go-que.
package bus

import (
	"bytes"
	"encoding/json"
	"net/textproto"

	"github.com/pkg/errors"
	"github.com/tnclong/go-que"
)

func (m *Message) ToRaw() json.RawMessage {
	var hdr Header
	if m.Header != nil {
		hdr = make(Header)
		for k, vv := range m.Header {
			canonicalKey := textproto.CanonicalMIMEHeaderKey(k)
			hdr[canonicalKey] = vv
		}
	}

	v := struct {
		*Message
		Header Header `json:"header,omitempty"`
	}{
		Message: m,
		Header:  hdr,
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		panic(err)
	}
	data := buf.Bytes()
	return json.RawMessage(data[:len(data)-1])
}

func ArgsFromMessageRaw(msgRaw json.RawMessage, pattern string) []byte {
	return que.Args(msgRaw, pattern)
}

func InboundFromArgs(args []byte) (*Inbound, error) {
	var msg Message
	var pattern string
	count, err := que.ParseArgs(args, &msg, &pattern)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse message")
	}
	if count != 2 {
		return nil, errors.Errorf("invalid args count: %d", count)
	}

	return &Inbound{
		Message: msg,
		Pattern: pattern,
	}, nil
}
