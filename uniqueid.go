package bus

import (
	"fmt"

	"github.com/rs/xid"
)

var UniqueID = func(v string) func(msg *Outbound) string {
	return func(msg *Outbound) string {
		return fmt.Sprintf("_gobus_:%s:%s", msg.Subject, v)
	}
}

var FallbackUniqueID = func(msg *Outbound) string {
	return "_gobus_:" + xid.New().String()
}

var PureUniqueID = func(v string) func(msg *Outbound) string {
	return func(msg *Outbound) string {
		return v
	}
}
