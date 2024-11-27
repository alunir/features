// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.20.0

package sqlc

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

type Resolution string

const (
	Resolution1Min  Resolution = "1Min"
	Resolution5Min  Resolution = "5Min"
	Resolution15Min Resolution = "15Min"
	Resolution30Min Resolution = "30Min"
	Resolution1H    Resolution = "1H"
	Resolution4H    Resolution = "4H"
	Resolution1D    Resolution = "1D"
)

func (e *Resolution) Scan(src interface{}) error {
	switch s := src.(type) {
	case []byte:
		*e = Resolution(s)
	case string:
		*e = Resolution(s)
	default:
		return fmt.Errorf("unsupported scan type for Resolution: %T", src)
	}
	return nil
}

type NullResolution struct {
	Resolution Resolution
	Valid      bool // Valid is true if Resolution is not NULL
}

// Scan implements the Scanner interface.
func (ns *NullResolution) Scan(value interface{}) error {
	if value == nil {
		ns.Resolution, ns.Valid = "", false
		return nil
	}
	ns.Valid = true
	return ns.Resolution.Scan(value)
}

// Value implements the driver Valuer interface.
func (ns NullResolution) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return string(ns.Resolution), nil
}

type Emd struct {
	Instrument int64
	Resolution Resolution
	Fdim       string
	Epoch      time.Time
	If0        sql.NullString
	If1        sql.NullString
	If2        sql.NullString
	If3        sql.NullString
	If4        sql.NullString
	If5        sql.NullString
	If6        sql.NullString
	If7        sql.NullString
	If8        sql.NullString
	If9        sql.NullString
	If10       sql.NullString
	If11       sql.NullString
	If12       sql.NullString
	If13       sql.NullString
	If14       sql.NullString
	If15       sql.NullString
	Ia0        sql.NullString
	Ia1        sql.NullString
	Ia2        sql.NullString
	Ia3        sql.NullString
	Ia4        sql.NullString
	Ia5        sql.NullString
	Ia6        sql.NullString
	Ia7        sql.NullString
	Ia8        sql.NullString
	Ia9        sql.NullString
	Ia10       sql.NullString
	Ia11       sql.NullString
	Ia12       sql.NullString
	Ia13       sql.NullString
	Ia14       sql.NullString
	Ia15       sql.NullString
	Ip0        sql.NullString
	Ip1        sql.NullString
	Ip2        sql.NullString
	Ip3        sql.NullString
	Ip4        sql.NullString
	Ip5        sql.NullString
	Ip6        sql.NullString
	Ip7        sql.NullString
	Ip8        sql.NullString
	Ip9        sql.NullString
	Ip10       sql.NullString
	Ip11       sql.NullString
	Ip12       sql.NullString
	Ip13       sql.NullString
	Ip14       sql.NullString
	Ip15       sql.NullString
}

type Exchange struct {
	ID   int64
	Name string
}

type Ffd struct {
	Instrument int64
	Resolution Resolution
	Fdim       string
	Epoch      time.Time
	Open       sql.NullString
	High       sql.NullString
	Low        sql.NullString
	Close      sql.NullString
	Volume     sql.NullString
	Trades     sql.NullString
}

type Instrument struct {
	ID       int64
	Name     string
	Exchange int64
}

type Ohlcvt struct {
	Instrument int64
	Epoch      time.Time
	Open       string
	High       string
	Low        string
	Close      string
	Volume     string
	Trades     int64
}

type PremiumIndex struct {
	Spotinstrument    int64
	Futuresinstrument int64
	Resolution        Resolution
	Epoch             time.Time
	Premiumindex      string
}
