package functions

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

func registerDateFunctions(r *FuncRegistry) {
	r.Register(&FuncDef{Name: "date", NumArgs: -1, IsDeterministic: true, ScalarFunc: dateFunc})
	r.Register(&FuncDef{Name: "time", NumArgs: -1, IsDeterministic: true, ScalarFunc: timeFunc})
	r.Register(&FuncDef{Name: "datetime", NumArgs: -1, IsDeterministic: true, ScalarFunc: datetimeFunc})
	r.Register(&FuncDef{Name: "julianday", NumArgs: -1, IsDeterministic: true, ScalarFunc: juliandayFunc})
	r.Register(&FuncDef{Name: "strftime", NumArgs: -1, IsDeterministic: true, ScalarFunc: strftimeFunc})
}

// dateTime holds parsed date/time components.
type dateTime struct {
	year  int
	month int
	day   int
	hour  int
	min   int
	sec   float64 // includes fractional seconds

	jd float64 // Julian Day number

	validYMD bool
	validHMS bool
	validJD  bool
}

// parseDateTimeArgs parses date/time arguments (a time string plus optional modifiers).
func parseDateTimeArgs(args []*vdbe.Mem) (*dateTime, bool) {
	if len(args) == 0 {
		return nil, false
	}

	if args[0].Type == vdbe.MemNull {
		return nil, false
	}

	timeStr := args[0].TextValue()
	d := parseTimeString(timeStr)
	if d == nil {
		return nil, false
	}

	// Ensure we have a Julian Day
	if !d.validJD {
		if d.validYMD {
			if !d.validHMS {
				d.hour, d.min, d.sec = 0, 0, 0
			}
			d.jd = computeJD(d)
			d.validJD = true
		} else {
			return nil, false
		}
	}

	// Apply modifiers
	for i := 1; i < len(args); i++ {
		if args[i].Type == vdbe.MemNull {
			return nil, false
		}
		mod := args[i].TextValue()
		if !applyModifier(d, mod) {
			return nil, false
		}
	}

	return d, true
}

// parseTimeString parses a SQLite time string into a dateTime.
func parseTimeString(s string) *dateTime {
	s = strings.TrimSpace(s)

	if s == "now" {
		now := time.Now().UTC()
		return &dateTime{
			year:  now.Year(),
			month: int(now.Month()),
			day:   now.Day(),
			hour:  now.Hour(),
			min:   now.Minute(),
			sec:   float64(now.Second()) + float64(now.Nanosecond())/1e9,
		}
	}

	// Try Julian day number (pure numeric, no dash or colon)
	if f, err := strconv.ParseFloat(s, 64); err == nil && !strings.Contains(s, "-") && !strings.Contains(s, ":") {
		return &dateTime{jd: f, validJD: true}
	}

	d := &dateTime{}

	// Try various ISO formats
	formats := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006-01-02",
		"15:04:05.999999999",
		"15:04:05",
		"15:04",
	}

	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			if strings.HasPrefix(f, "15:") {
				d.year = 2000
				d.month = 1
				d.day = 1
			} else {
				d.year = t.Year()
				d.month = int(t.Month())
				d.day = t.Day()
			}
			d.hour = t.Hour()
			d.min = t.Minute()
			d.sec = float64(t.Second()) + float64(t.Nanosecond())/1e9
			d.validYMD = true
			d.validHMS = true
			return d
		}
	}

	// Try YYYY-MM-DD HH:MM:SS with manual parsing
	if parts := strings.SplitN(s, " ", 2); len(parts) == 2 {
		if t, err := time.Parse("2006-01-02", parts[0]); err == nil {
			d.year = t.Year()
			d.month = int(t.Month())
			d.day = t.Day()
			d.validYMD = true
			timeParts := strings.SplitN(parts[1], ":", 3)
			if len(timeParts) >= 1 {
				if v, err := strconv.Atoi(timeParts[0]); err == nil && v >= 0 && v < 24 {
					d.hour = v
				}
			}
			if len(timeParts) >= 2 {
				if v, err := strconv.Atoi(timeParts[1]); err == nil && v >= 0 && v < 60 {
					d.min = v
				}
			}
			if len(timeParts) >= 3 {
				if v, err := strconv.ParseFloat(timeParts[2], 64); err == nil && v >= 0 && v < 60 {
					d.sec = v
				}
			}
			d.validHMS = true
			return d
		}
	}

	return nil
}

// computeJD computes Julian Day number from YMD/HMS.
func computeJD(d *dateTime) float64 {
	Y, M, D := d.year, d.month, d.day
	H, Mm, S := d.hour, d.min, d.sec

	if M <= 2 {
		Y--
		M += 12
	}
	A := Y / 100
	B := 2 - A + A/4

	jd := float64(int(365.25*float64(Y+4716))) + float64(int(30.6001*float64(M+1))) + float64(D) + float64(B) - 1524.5
	jd += float64(H)/24.0 + float64(Mm)/1440.0 + S/86400.0

	return jd
}

// jdToYMD converts a Julian Day number to year, month, day.
// Uses the standard algorithm from the Astronomical Algorithms book.
func jdToYMD(jd float64) (int, int, int) {
	// Shift to midnight (JD starts at noon)
	z := int(jd + 0.5)
	f := jd + 0.5 - float64(z)
	_ = f

	var alpha int
	if z < 2299161 {
		alpha = 0
	} else {
		alpha = int((float64(z) - 1867216.25) / 36524.25)
	}

	A := z + 1 + alpha - alpha/4
	B := A + 1524
	C := int((float64(B) - 122.1) / 365.25)
	D := int(365.25 * float64(C))
	E := int((float64(B) - float64(D)) / 30.6001)

	day := B - D - int(30.6001*float64(E))
	var month int
	if E < 14 {
		month = E - 1
	} else {
		month = E - 13
	}
	var year int
	if month > 2 {
		year = C - 4716
	} else {
		year = C - 4715
	}

	return year, month, day
}

// jdToHMS extracts hour, minute, second from a Julian Day.
func jdToHMS(jd float64) (int, int, float64) {
	// The fractional part of the Julian Day (time since noon)
	frac := jd - float64(int(jd)) + 0.5
	if frac >= 1.0 {
		frac -= 1.0
	}

	// Add a tiny epsilon to avoid floating-point truncation
	totalSeconds := frac*86400.0 + 0.5
	hour := int(totalSeconds / 3600.0)
	totalSeconds -= float64(hour) * 3600.0
	minute := int(totalSeconds / 60.0)
	second := totalSeconds - float64(minute)*60.0 - 0.5

	// Round second to avoid floating-point artifacts
	second = float64(int(second*1000+0.5)) / 1000.0

	return hour, minute, second
}

// applyModifier applies a date/time modifier.
func applyModifier(d *dateTime, mod string) bool {
	mod = strings.TrimSpace(mod)
	mod = strings.ToLower(mod)

	var sign float64 = 1
	rest := mod

	if strings.HasPrefix(rest, "+") {
		sign = 1
		rest = rest[1:]
	} else if strings.HasPrefix(rest, "-") {
		sign = -1
		rest = rest[1:]
	}

	// Parse number
	i := 0
	for i < len(rest) && (rest[i] >= '0' && rest[i] <= '9' || rest[i] == '.') {
		i++
	}
	if i == 0 {
		switch mod {
		case "utc":
			return true
		case "localtime":
			return true
		case "start of month":
			return applyStartOfMonth(d)
		case "start of year":
			return applyStartOfYear(d)
		case "start of day":
			return applyStartOfDay(d)
		default:
			return false
		}
	}

	num, err := strconv.ParseFloat(rest[:i], 64)
	if err != nil {
		return false
	}
	num *= sign
	unit := strings.TrimSpace(rest[i:])

	// Ensure we have JD
	if !d.validJD {
		d.jd = computeJD(d)
		d.validJD = true
	}

	switch unit {
	case "days", "day":
		d.jd += num
	case "hours", "hour":
		d.jd += num / 24.0
	case "minutes", "minute":
		d.jd += num / 1440.0
	case "seconds", "second":
		d.jd += num / 86400.0
	case "months", "month":
		return applyMonthModifier(d, num)
	case "years", "year":
		return applyMonthModifier(d, num*12)
	default:
		return false
	}

	d.validYMD = false
	d.validHMS = false
	return true
}

func applyStartOfMonth(d *dateTime) bool {
	if d.validYMD {
		d.day = 1
		d.hour, d.min, d.sec = 0, 0, 0
		d.validHMS = true
		d.jd = computeJD(d)
		d.validJD = true
	} else if d.validJD {
		y, m, _ := jdToYMD(d.jd)
		d.year, d.month, d.day = y, m, 1
		d.hour, d.min, d.sec = 0, 0, 0
		d.validYMD = true
		d.validHMS = true
		d.jd = computeJD(d)
		d.validJD = true
	}
	return true
}

func applyStartOfYear(d *dateTime) bool {
	if d.validYMD {
		d.month = 1
		d.day = 1
		d.hour, d.min, d.sec = 0, 0, 0
		d.validHMS = true
		d.jd = computeJD(d)
		d.validJD = true
	} else if d.validJD {
		y, _, _ := jdToYMD(d.jd)
		d.year, d.month, d.day = y, 1, 1
		d.hour, d.min, d.sec = 0, 0, 0
		d.validYMD = true
		d.validHMS = true
		d.jd = computeJD(d)
		d.validJD = true
	}
	return true
}

func applyStartOfDay(d *dateTime) bool {
	if !d.validJD {
		d.jd = computeJD(d)
		d.validJD = true
	}
	// Julian day starts at noon; floor to the nearest noon
	d.jd = float64(int(d.jd+0.5)) - 0.5
	d.validYMD = false
	d.validHMS = false
	return true
}

func applyMonthModifier(d *dateTime, months float64) bool {
	if d.validYMD {
		// Use cached YMD
	} else if d.validJD {
		d.year, d.month, d.day = jdToYMD(d.jd)
		d.validYMD = true
	}
	if d.validHMS {
		// Use cached HMS
	} else if d.validJD {
		d.hour, d.min, d.sec = jdToHMS(d.jd)
		d.validHMS = true
	}

	totalMonths := d.year*12 + d.month - 1 + int(months)
	d.year = totalMonths / 12
	d.month = totalMonths%12 + 1

	maxDay := daysInMonth(d.year, d.month)
	if d.day > maxDay {
		d.day = maxDay
	}

	d.jd = computeJD(d)
	d.validJD = true
	return true
}

func daysInMonth(year, month int) int {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if isLeapYear(year) {
			return 29
		}
		return 28
	default:
		return 31
	}
}

func isLeapYear(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

// --- Date function implementations ---

func dateFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) == 0 {
		return vdbe.NewMemNull()
	}
	d, ok := parseDateTimeArgs(args)
	if !ok {
		return vdbe.NewMemNull()
	}

	if d.validJD && !d.validYMD {
		d.year, d.month, d.day = jdToYMD(d.jd)
	}

	return vdbe.NewMemStr(fmt.Sprintf("%04d-%02d-%02d", d.year, d.month, d.day))
}

func timeFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) == 0 {
		return vdbe.NewMemNull()
	}
	d, ok := parseDateTimeArgs(args)
	if !ok {
		return vdbe.NewMemNull()
	}

	hour, min, sec := d.hour, d.min, d.sec
	if d.validJD && !d.validHMS {
		hour, min, sec = jdToHMS(d.jd)
	}
	secInt := int(sec)
	return vdbe.NewMemStr(fmt.Sprintf("%02d:%02d:%02d", hour, min, secInt))
}

func datetimeFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) == 0 {
		return vdbe.NewMemNull()
	}
	d, ok := parseDateTimeArgs(args)
	if !ok {
		return vdbe.NewMemNull()
	}

	if d.validJD && !d.validYMD {
		d.year, d.month, d.day = jdToYMD(d.jd)
	}
	hour, min, sec := d.hour, d.min, d.sec
	if d.validJD && !d.validHMS {
		hour, min, sec = jdToHMS(d.jd)
	}
	secInt := int(sec)
	return vdbe.NewMemStr(fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		d.year, d.month, d.day, hour, min, secInt))
}

func juliandayFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) == 0 {
		return vdbe.NewMemNull()
	}
	d, ok := parseDateTimeArgs(args)
	if !ok {
		return vdbe.NewMemNull()
	}
	if !d.validJD {
		d.jd = computeJD(d)
	}
	return vdbe.NewMemFloat(d.jd)
}

func strftimeFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) == 0 {
		return vdbe.NewMemNull()
	}
	if args[0].Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	fmtStr := args[0].TextValue()

	d, ok := parseDateTimeArgs(args[1:])
	if !ok {
		return vdbe.NewMemNull()
	}

	if !d.validJD {
		d.jd = computeJD(d)
	}
	if !d.validYMD {
		d.year, d.month, d.day = jdToYMD(d.jd)
	}
	if !d.validHMS {
		d.hour, d.min, d.sec = jdToHMS(d.jd)
	}

	return vdbe.NewMemStr(strftimeFormat(fmtStr, d))
}

func strftimeFormat(fmtStr string, d *dateTime) string {
	var buf strings.Builder
	i := 0
	for i < len(fmtStr) {
		if fmtStr[i] == '%' && i+1 < len(fmtStr) {
			i++
			switch fmtStr[i] {
			case 'd':
				buf.WriteString(fmt.Sprintf("%02d", d.day))
			case 'e':
				buf.WriteString(fmt.Sprintf("%2d", d.day))
			case 'f':
				s := math.Min(d.sec, 59.999)
				buf.WriteString(fmt.Sprintf("%06.3f", s))
			case 'F':
				buf.WriteString(fmt.Sprintf("%04d-%02d-%02d", d.year, d.month, d.day))
			case 'H':
				buf.WriteString(fmt.Sprintf("%02d", d.hour))
			case 'k':
				buf.WriteString(fmt.Sprintf("%2d", d.hour))
			case 'I':
				h := d.hour % 12
				if h == 0 {
					h = 12
				}
				buf.WriteString(fmt.Sprintf("%02d", h))
			case 'l':
				h := d.hour % 12
				if h == 0 {
					h = 12
				}
				buf.WriteString(fmt.Sprintf("%2d", h))
			case 'j':
				doy := dayOfYear(d.year, d.month, d.day)
				buf.WriteString(fmt.Sprintf("%03d", doy))
			case 'J':
				buf.WriteString(fmt.Sprintf("%.16g", d.jd))
			case 'm':
				buf.WriteString(fmt.Sprintf("%02d", d.month))
			case 'M':
				buf.WriteString(fmt.Sprintf("%02d", d.min))
			case 'p':
				if d.hour < 12 {
					buf.WriteString("AM")
				} else {
					buf.WriteString("PM")
				}
			case 'P':
				if d.hour < 12 {
					buf.WriteString("am")
				} else {
					buf.WriteString("pm")
				}
			case 'R':
				buf.WriteString(fmt.Sprintf("%02d:%02d", d.hour, d.min))
			case 's':
				unix := int64((d.jd-2440587.5)*86400.0 + 0.5)
				buf.WriteString(strconv.FormatInt(unix, 10))
			case 'S':
				buf.WriteString(fmt.Sprintf("%02d", int(d.sec)))
			case 'T':
				buf.WriteString(fmt.Sprintf("%02d:%02d:%02d", d.hour, d.min, int(d.sec)))
			case 'u':
				w := int((d.jd + 1.5)) % 7
				if w == 0 {
					w = 7
				}
				buf.WriteString(strconv.Itoa(w))
			case 'w':
				w := int((d.jd + 1.5)) % 7
				buf.WriteString(strconv.Itoa(w))
			case 'W':
				doy := dayOfYear(d.year, d.month, d.day)
				wday := dayOfWeek(d.year, d.month, d.day)
				week := (doy + 6 - wday) / 7
				buf.WriteString(fmt.Sprintf("%02d", week))
			case 'Y':
				buf.WriteString(fmt.Sprintf("%04d", d.year))
			case '%':
				buf.WriteByte('%')
			default:
				buf.WriteByte('%')
				buf.WriteByte(fmtStr[i])
			}
			i++
		} else {
			buf.WriteByte(fmtStr[i])
			i++
		}
	}
	return buf.String()
}

func dayOfYear(year, month, day int) int {
	days := day
	for m := 1; m < month; m++ {
		days += daysInMonth(year, m)
	}
	return days
}

// dayOfWeek returns 0=Sunday, 1=Monday, ..., 6=Saturday.
func dayOfWeek(year, month, day int) int {
	if month < 3 {
		month += 12
		year--
	}
	k := year % 100
	j := year / 100
	w := (day + (13*(month+1))/5 + k + k/4 + j/4 + 5*j) % 7
	w = (w + 6) % 7
	return w
}
