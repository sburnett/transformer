package transformer

import (
	"bytes"
	"math"
	"testing"
)

func checkLess(t *testing.T, lesser, greater interface{}) {
	lesserBuffer := bytes.NewBuffer([]byte{})
	if err := Write(lesserBuffer, lesser); err != nil {
		t.Fatalf("Error encoding key %v: %v", lesser, err)
	}
	greaterBuffer := bytes.NewBuffer([]byte{})
	if err := Write(greaterBuffer, greater); err != nil {
		t.Fatalf("Error encoding key %v: %v", greater, err)
	}
	if bytes.Compare(lesserBuffer.Bytes(), greaterBuffer.Bytes()) >= 0 {
		t.Fatalf("Unexpected comparison result. %v is not less than %v.", lesser, greater)
	}
}

func checkNotLess(t *testing.T, lesser, greater interface{}) {
	lesserBuffer := bytes.NewBuffer([]byte{})
	if err := Write(lesserBuffer, lesser); err != nil {
		t.Fatalf("Error encoding key %v: %v", lesser, err)
	}
	greaterBuffer := bytes.NewBuffer([]byte{})
	if err := Write(greaterBuffer, greater); err != nil {
		t.Fatalf("Error encoding key %v: %v", greater, err)
	}
	if bytes.Compare(lesserBuffer.Bytes(), greaterBuffer.Bytes()) < 0 {
		t.Fatalf("Unexpected comparison result. %v is not less than %v.", lesser, greater)
	}
}

func TestEncodeBytes(t *testing.T) {
	checkLess(t, []byte{1}, []byte{2})
	checkLess(t, []byte{}, []byte{1})
}

func TestEncodeString(t *testing.T) {
	checkLess(t, "a", "b")
	checkLess(t, "", "a")
	checkLess(t, "a", "ab")
}

func TestEncodeUint8(t *testing.T) {
	checkLess(t, uint8(0), uint8(1))
	checkLess(t, uint8(math.MaxUint8-1), uint8(math.MaxUint8))
	checkLess(t, uint8(math.MaxInt8), uint8(math.MaxUint8))
	checkLess(t, uint8(math.MaxInt8-1), uint8(math.MaxInt8))
	checkLess(t, uint8(math.MaxInt8), uint8(math.MaxInt8+1))

	checkNotLess(t, uint8(0), uint8(0))
}

func TestEncodeInt8(t *testing.T) {
	checkLess(t, int8(0), int8(1))
	checkLess(t, int8(math.MaxInt8-1), int8(math.MaxInt8))
	checkLess(t, int8(math.MinInt8), int8(math.MinInt8+1))

	checkNotLess(t, int8(0), int8(0))
}

func TestEncodeUint16(t *testing.T) {
	checkLess(t, uint16(0), uint16(1))
	checkLess(t, uint16(math.MaxUint16-1), uint16(math.MaxUint16))
	checkLess(t, uint16(math.MaxInt16), uint16(math.MaxUint16))
	checkLess(t, uint16(math.MaxInt16-1), uint16(math.MaxInt16))
	checkLess(t, uint16(math.MaxInt16), uint16(math.MaxInt16+1))

	checkNotLess(t, uint16(0), uint16(0))
}

func TestEncodeInt16(t *testing.T) {
	checkLess(t, int16(0), int16(1))
	checkLess(t, int16(math.MaxInt16-1), int16(math.MaxInt16))
	checkLess(t, int16(math.MinInt16), int16(math.MinInt16+1))

	checkNotLess(t, int16(0), int16(0))
}

func TestEncodeUint32(t *testing.T) {
	checkLess(t, uint32(0), uint32(1))
	checkLess(t, uint32(math.MaxUint32-1), uint32(math.MaxUint32))
	checkLess(t, uint32(math.MaxInt32), uint32(math.MaxUint32))
	checkLess(t, uint32(math.MaxInt32-1), uint32(math.MaxInt32))
	checkLess(t, uint32(math.MaxInt32), uint32(math.MaxInt32+1))

	checkNotLess(t, uint32(0), uint32(0))
}

func TestEncodeInt32(t *testing.T) {
	checkLess(t, int32(0), int32(1))
	checkLess(t, int32(math.MaxInt32-1), int32(math.MaxInt32))
	checkLess(t, int32(math.MinInt32), int32(math.MinInt32+1))

	checkNotLess(t, int32(0), int32(0))
}

func TestEncodeUint64(t *testing.T) {
	checkLess(t, uint64(0), uint64(1))
	checkLess(t, uint64(math.MaxUint64-1), uint64(math.MaxUint64))
	checkLess(t, uint64(math.MaxInt64), uint64(math.MaxUint64))
	checkLess(t, uint64(math.MaxInt64-1), uint64(math.MaxInt64))
	checkLess(t, uint64(math.MaxInt64), uint64(math.MaxInt64+1))

	checkNotLess(t, uint64(0), uint64(0))
}

func TestEncodeInt64(t *testing.T) {
	checkLess(t, int64(0), int64(1))
	checkLess(t, int64(math.MaxInt64-1), int64(math.MaxInt64))
	checkLess(t, int64(math.MinInt64), int64(math.MinInt64+1))

	checkNotLess(t, int64(0), int64(0))
}

func TestDecodeBytes(t *testing.T) {
	checkDecode := func(value []byte) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue []byte
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if !bytes.Equal(value, decodedValue) {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode([]byte("hello"))
	checkDecode([]byte(""))
}

func TestDecodeString(t *testing.T) {
	checkDecode := func(value string) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue string
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode("hello")
	checkDecode("")
}

func TestDecodeUint8(t *testing.T) {
	checkDecode := func(value uint8) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue uint8
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode(0)
	checkDecode(math.MaxUint8)
}

func TestDecodeInt8(t *testing.T) {
	checkDecode := func(value int8) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue int8
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode(0)
	checkDecode(-1)
	checkDecode(2)
	checkDecode(-100)
	checkDecode(math.MaxInt8)
	checkDecode(math.MinInt8)
}

func TestDecodeUint16(t *testing.T) {
	checkDecode := func(value uint16) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue uint16
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode(0)
	checkDecode(math.MaxUint16)
}

func TestDecodeInt16(t *testing.T) {
	checkDecode := func(value int16) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue int16
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode(0)
	checkDecode(-1)
	checkDecode(2)
	checkDecode(-100)
	checkDecode(math.MaxInt16)
	checkDecode(math.MinInt16)
}

func TestDecodeUint32(t *testing.T) {
	checkDecode := func(value uint32) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue uint32
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode(0)
	checkDecode(math.MaxUint32)
}

func TestDecodeInt32(t *testing.T) {
	checkDecode := func(value int32) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue int32
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode(0)
	checkDecode(-1)
	checkDecode(2)
	checkDecode(-100)
	checkDecode(math.MaxInt32)
	checkDecode(math.MinInt32)
}

func TestDecodeUint64(t *testing.T) {
	checkDecode := func(value uint64) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue uint64
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode(0)
	checkDecode(math.MaxUint64)
}

func TestDecodeInt64(t *testing.T) {
	checkDecode := func(value int64) {
		buffer := bytes.NewBuffer([]byte{})
		if err := Write(buffer, value); err != nil {
			t.Fatalf("Encoder error: %v", err)
		}
		var decodedValue int64
		if err := Read(buffer, &decodedValue); err != nil {
			t.Fatalf("Decoder error: %v", err)
		}
		if value != decodedValue {
			t.Fatalf("Expected: %v, got %v", value, decodedValue)
		}
	}
	checkDecode(0)
	checkDecode(-1)
	checkDecode(2)
	checkDecode(-100)
	checkDecode(math.MaxInt64)
	checkDecode(math.MinInt64)
}
