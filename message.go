// Package bus implements a publish-subscribe pattern on top of go-que.
package bus

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
	"github.com/qor5/go-que"
)

func (m *Message) ToRaw(sub Subscription) (json.RawMessage, error) {
	hdr := make(Header)
	if m.Header != nil {
		for k, vv := range m.Header {
			canonicalKey := http.CanonicalHeaderKey(k)
			hdr[canonicalKey] = vv
		}
	}
	hdr.Set(HeaderSubscriptionPattern, sub.Pattern())
	hdr.Set(HeaderSubscriptionIdentifier, sub.ID())

	v := struct {
		*Message
		Header Header `json:"header"`
	}{
		Message: m,
		Header:  hdr,
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, errors.Wrap(err, "failed to encode message to JSON")
	}
	data := buf.Bytes()
	return json.RawMessage(data[:len(data)-1]), nil
}

// InboundFromArgs creates an Inbound message from raw arguments.
// This is primarily used for testing and debugging purposes.
func InboundFromArgs(args []byte) (*Inbound, error) {
	var raw struct {
		Message
		Payload json.RawMessage `json:"payload"`
	}
	count, err := que.ParseArgs(args, &raw)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse message")
	}
	if count != 1 {
		return nil, errors.Errorf("invalid args count: %d", count)
	}
	raw.Message.Payload = raw.Payload
	return &Inbound{
		Message: raw.Message,
	}, nil
}

// InboundFromJob creates an Inbound message from a que.Job.
// This is the primary method used in production message processing.
func InboundFromJob(job que.Job) (*Inbound, error) {
	inbound, err := InboundFromArgs(job.Plan().Args)
	if err != nil {
		return nil, err
	}
	inbound.Job = job
	return inbound, nil
}
