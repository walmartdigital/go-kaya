package types

import (
	"encoding/json"
	"strconv"
)

// FlexInt ...
type FlexInt int

// FlexBool ...
type FlexBool bool

// UnmarshalJSON ...
func (fi *FlexInt) UnmarshalJSON(b []byte) error {
	if b[0] != '"' {
		return json.Unmarshal(b, (*int)(fi))
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*fi = FlexInt(i)
	return nil
}

// MarshalJSON ...
func (fi *FlexInt) MarshalJSON() ([]byte, error) {
	return []byte(string(*fi)), nil
}

// UnmarshalJSON ...
func (fi *FlexBool) UnmarshalJSON(b []byte) error {
	if b[0] != '"' {
		return json.Unmarshal(b, (*bool)(fi))
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	i, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	*fi = FlexBool(i)
	return nil
}

// MarshalJSON ...
func (fi *FlexBool) MarshalJSON() ([]byte, error) {
	if *fi {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}
