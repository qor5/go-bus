package bus

import (
	"github.com/rs/xid"
)

var UniqueID = func(v string) func(msg *Outbound) string {
	return func(_ *Outbound) string {
		return v
	}
}

var FallbackUniqueID = func(msg *Outbound) string {
	return "_gobus_:" + xid.New().String()
}
