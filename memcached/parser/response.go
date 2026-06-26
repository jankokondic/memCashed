package parser

func EncodeResponse(body []byte) []byte {
	out := make([]byte, 4+len(body))

	size := uint32(len(body))

	out[0] = byte(size)
	out[1] = byte(size >> 8)
	out[2] = byte(size >> 16)
	out[3] = byte(size >> 24)

	copy(out[4:], body)

	return out
}
