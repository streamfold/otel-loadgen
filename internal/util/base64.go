package util

import "encoding/base64"

func B64MustDecode(s string) []byte {
	out, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return out
}
