package dump

const (
	Zero        byte = 0
	NewString   byte = '\n'
	NewPage     byte = '\r'
	Esc         byte = '\\'
	Quote       byte = '\''
	DoubleQuote byte = '"'
	Z           byte = '\032'

	ZeroEsc        byte = '0'
	NewStringEsc   byte = 'n'
	NewPageEsc     byte = 'r'
	EscEsc         byte = '\\'
	QuoteEsc       byte = '\''
	DoubleQuoteEsc byte = '"'
	ZEsc           byte = 'Z'
)

func Escape(sql []byte) []byte {
	var (
		n    = len(sql)
		dest = make([]byte, 0, 2*n)
		esc  byte
	)

	for i := 0; i < n; i++ {
		esc = 0

		switch sql[i] {
		case Zero: /* Must be escaped for 'mysql' */
			esc = ZeroEsc
			break
		case NewString: /* Must be escaped for logs */
			esc = NewStringEsc
			break
		case NewPage:
			esc = NewPageEsc
			break
		case Esc:
			esc = EscEsc
			break
		case Quote:
			esc = QuoteEsc
			break
		case DoubleQuote: /* Better safe than sorry */
			esc = DoubleQuoteEsc
			break
		case Z: /* This gives problems on Win32 */
			esc = ZEsc
		}

		if esc != 0 {
			dest = append(dest, '\\', esc)
		} else {
			dest = append(dest, sql[i])
		}
	}

	return dest
}
