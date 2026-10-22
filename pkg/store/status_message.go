package store

import (
	"encoding/json"
	"strconv"
)

// ApplyingStatus records pending progress without formatting its object count.
func ApplyingStatus(objects int) StatusInfo {
	return StatusInfo{Status: StatusPending, Reason: StatusReason(ReasonReconciling), applyingObjects: objects + 1}
}

// String returns the visible status message.
func (i StatusInfo) String() string {
	if i.applyingObjects == 0 {
		return i.Message
	}
	var buffer [64]byte
	return string(appendApplying(buffer[:0], i.applyingObjects))
}

func sameMessage(left string, leftObjects int, right string, rightObjects int) bool {
	if leftObjects == rightObjects {
		return leftObjects != 0 || left == right
	}
	if leftObjects != 0 && rightObjects != 0 {
		return false
	}
	objects, literal := leftObjects, right
	if objects == 0 {
		objects, literal = rightObjects, left
	}
	var buffer [64]byte
	return string(appendApplying(buffer[:0], objects)) == literal
}

func appendApplying(buffer []byte, objects int) []byte {
	buffer = append(buffer, "applying "...)
	buffer = strconv.AppendInt(buffer, int64(objects-1), 10)
	return append(buffer, " objects"...)
}

// MarshalJSON encodes the visible status message with its readiness and reason.
func (i StatusInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Status  Status
		Reason  StatusReason
		Message string
	}{i.Status, i.Reason, i.String()})
}
