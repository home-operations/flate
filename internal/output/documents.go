// Key ordering and scalar layout are adapted for typed, buffered document emission
// from go.yaml.in/yaml/v2 (Copyright 2011-2016 Canonical Ltd., Apache-2.0)
// and its libyaml emitter (Copyright (c) 2006 Kirill Simonov, MIT).
// The applicable notices and licenses are in documents.NOTICE.

package output

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type documentEntry struct {
	name      string
	key       [64]byte
	size      int
	order     [32]byte
	orderSize int
	value     any
	text      string
	typed     bool
}

func (e *documentEntry) spelling() string {
	if e.size > 0 {
		return string(e.key[:e.size])
	}
	return e.name
}

type documentScratch struct {
	buffer  [4096]byte
	entries [32]documentEntry
}

type documentEmitter struct {
	writer  io.Writer
	scratch []documentScratch
	count   int
	used    int
	column  int
	mode    int
	err     error
	scalar  [128]byte
}

type documentPosition struct {
	indent int
	prefix byte
	depth  int
}

func (e Encoder) documentGroups(groups []DocumentGroup) error {
	var d documentEmitter
	d.mode = 0
	capacity := 0
	for _, group := range groups {
		for _, doc := range group.Documents {
			n, _ := d.value(doc, documentPosition{})
			capacity = max(capacity, n)
		}
	}
	d.scratch = make([]documentScratch, max(1, (capacity+31)/32))
	d.writer = e.Writer
	for _, group := range groups {
		if len(groups) != 1 {
			d.write("# Cluster: ")
			d.write(group.Cluster)
			d.write("\n")
		}
		for _, doc := range group.Documents {
			if d.err != nil {
				return d.err
			}
			d.mode = 1
			if _, err := d.value(doc, documentPosition{}); err != nil {
				return errors.Join(err, d.flush())
			}
			d.mode = 2
			d.write("---\n")
			if _, err := d.value(doc, documentPosition{}); err != nil {
				return err
			}
			if d.column != 0 {
				d.write("\n")
			}
			if d.err != nil {
				return d.err
			}
		}
	}
	return d.flush()
}

func (d *documentEmitter) flush() error {
	if d.err == nil && d.used > 0 {
		var n int
		n, d.err = d.writer.Write(d.scratch[0].buffer[:d.used])
		if n < d.used && d.err == nil {
			d.err = io.ErrShortWrite
		}
		d.used = 0
	}
	return d.err
}

func (d *documentEmitter) write(s string) {
	for len(s) > 0 && d.err == nil {
		if d.used == len(d.scratch[0].buffer) {
			if d.flush() != nil {
				return
			}
		}
		n := copy(d.scratch[0].buffer[d.used:], s)
		for i := range n {
			c := s[i]
			if c == '\n' {
				d.column = 0
			} else if c&0xc0 != 0x80 {
				d.column++
			}
		}
		d.used += n
		s = s[n:]
	}
}

func (d *documentEmitter) indent(n int) {
	if d.column != 0 {
		d.write("\n")
	}
	for range n {
		d.write(" ")
	}
}

func (d *documentEmitter) start(p documentPosition, block bool) {
	if d.mode != 2 {
		return
	}
	switch p.prefix {
	case ':':
		if !block {
			d.write(" ")
		}
	case '-', '?':
		d.write(" ")
	}
}

func (d *documentEmitter) entry(i int) *documentEntry {
	return &d.scratch[i/32].entries[i%32]
}

func (d *documentEmitter) value(value any, p documentPosition) (int, error) {
	if d.err != nil {
		return 0, d.err
	}
	if p.depth > 10000 {
		if d.mode == 0 {
			return 0, nil
		}
		return 0, &json.UnsupportedValueError{Str: "excessive nesting or cycle in document"}
	}
	switch v := value.(type) {
	case map[string]any:
		if v == nil {
			d.atom("null", p)
			return 0, nil
		}
		return d.mapping(v, nil, nil, p)
	case map[string]string:
		if v == nil {
			d.atom("null", p)
			return 0, nil
		}
		return d.mapping(nil, v, nil, p)
	case map[any]any:
		if v == nil {
			d.atom("null", p)
			return 0, nil
		}
		return d.mapping(nil, nil, v, p)
	case []any:
		if v == nil {
			d.atom("null", p)
			return 0, nil
		}
		if len(v) == 0 {
			d.atom("[]", p)
			return 0, nil
		}
		if p.prefix == ':' {
			p.indent -= 2
		}
		d.start(p, true)
		peak := 0
		for i, item := range v {
			if d.mode == 2 {
				if i > 0 || p.prefix != '-' && p.prefix != '?' {
					d.indent(p.indent)
				}
				d.write("-")
			}
			n, err := d.value(item, documentPosition{indent: p.indent + 2, prefix: '-', depth: p.depth + 1})
			if err != nil {
				return 0, err
			}
			peak = max(peak, n)
		}
		return peak, nil
	case nil:
		d.atom("null", p)
	case bool:
		if v {
			d.atom("true", p)
		} else {
			d.atom("false", p)
		}
	case int:
		if d.mode == 2 {
			d.atom(string(strconv.AppendInt(d.scalar[:0], int64(v), 10)), p)
		}
	case int64:
		if d.mode == 2 {
			d.atom(string(strconv.AppendInt(d.scalar[:0], v, 10)), p)
		}
	case uint64:
		if d.mode == 2 {
			d.atom(string(strconv.AppendUint(d.scalar[:0], v, 10)), p)
		}
	case float64:
		if d.mode == 0 {
			return 0, nil
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, &json.UnsupportedValueError{Value: reflect.ValueOf(v), Str: strconv.FormatFloat(v, 'g', -1, 64)}
		}
		if d.mode == 2 {
			d.atom(string(d.number(v)), p)
		}
	case string:
		if d.mode == 1 {
			return 0, validateDocumentString(v, false)
		}
		if d.mode == 2 {
			d.start(p, false)
			d.string(v, p.indent, false)
		}
	case time.Time:
		if d.mode == 0 {
			return 0, nil
		}
		text, err := v.AppendText(d.scalar[:0])
		if err != nil {
			return 0, &json.MarshalerError{Type: reflect.TypeFor[*time.Time](), Err: err}
		}
		if d.mode == 2 {
			d.start(p, false)
			d.string(string(text), p.indent, false)
		}
	default:
		if d.mode != 0 {
			return 0, &json.UnsupportedTypeError{Type: reflect.TypeOf(value)}
		}
	}
	return 0, d.err
}

func (d *documentEmitter) atom(s string, p documentPosition) {
	if d.mode != 2 {
		return
	}
	d.start(p, false)
	d.write(s)
}

func (d *documentEmitter) mapping(sm map[string]any, tm map[string]string, am map[any]any, p documentPosition) (int, error) {
	n := len(sm) + len(tm) + len(am)
	if n == 0 {
		d.atom("{}", p)
		return 0, nil
	}
	if d.mode != 2 {
		peak := 0
		next := documentPosition{depth: p.depth + 1}
		for k, v := range sm {
			if d.mode == 1 {
				if err := validateDocumentString(k, true); err != nil {
					return 0, err
				}
			}
			size, err := d.value(v, next)
			if err != nil {
				return 0, err
			}
			peak = max(peak, size)
		}
		for k, v := range tm {
			if d.mode == 1 {
				if err := validateDocumentString(k, true); err != nil {
					return 0, err
				}
				if err := validateDocumentString(v, false); err != nil {
					return 0, err
				}
			}
		}
		for k, v := range am {
			if d.mode == 1 {
				var entry documentEntry
				if err := entry.setKey(k); err != nil {
					return 0, err
				}
			}
			size, err := d.value(v, next)
			if err != nil {
				return 0, err
			}
			peak = max(peak, size)
		}
		return n + peak, nil
	}
	base := d.count
	d.count += n
	at := base
	for k, v := range sm {
		*d.entry(at) = documentEntry{name: k, value: v}
		at++
	}
	for k, v := range tm {
		*d.entry(at) = documentEntry{name: k, text: v, typed: true}
		at++
	}
	for k, v := range am {
		entry := d.entry(at)
		*entry = documentEntry{value: v}
		if err := entry.setKey(k); err != nil {
			return 0, err
		}
		at++
	}
	d.sort(base, n, false)
	survivors := 0
	for i := 0; i < n; {
		end := i + 1
		for end < n && normalizedCompare(d.entry(base+i).spelling(), d.entry(base+end).spelling()) == 0 {
			end++
		}
		// encoding/json/v2's interface-key sort has no equal-name tie-break, so equal names need a total order.
		*d.entry(base + survivors) = *d.entry(base + end - 1)
		survivors++
		i = end
	}
	d.sort(base, survivors, true)
	d.start(p, true)
	for i := range survivors {
		entry := d.entry(base + i)
		if i > 0 || p.prefix != '-' && p.prefix != '?' {
			d.indent(p.indent)
		}
		key := entry.spelling()
		explicit := normalizedLength(key) > 128
		runes := documentRunes{text: key}
		for {
			c, ok := runes.next()
			if !ok {
				break
			}
			if yamlBreak(c) {
				explicit = true
				break
			}
		}
		if explicit {
			d.write("? ")
		}
		d.string(key, p.indent+2, !explicit)
		child := documentPosition{indent: p.indent + 2, prefix: ':', depth: p.depth + 1}
		if explicit {
			d.indent(p.indent)
			child.prefix = '?'
		}
		d.write(":")
		if entry.typed {
			d.start(child, false)
			d.string(entry.text, child.indent, false)
		} else {
			if _, err := d.value(entry.value, child); err != nil {
				return 0, err
			}
		}
	}
	for i := range n {
		*d.entry(base + i) = documentEntry{}
	}
	d.count = base
	return n, nil
}

func (e *documentEntry) setKey(key any) error {
	var out []byte
	switch v := key.(type) {
	case string:
		e.name = v
		return validateDocumentString(v, true)
	case int:
		out = strconv.AppendInt(e.key[:0], int64(v), 10)
		e.order[0] = 3
		e.orderSize = len(binary.AppendVarint(e.order[:1], int64(v)))
	case int64:
		out = strconv.AppendInt(e.key[:0], v, 10)
		e.order[0] = 4
		e.orderSize = len(binary.AppendVarint(e.order[:1], v))
	case uint64:
		out = strconv.AppendUint(e.key[:0], v, 10)
		e.order[0] = 5
		e.orderSize = len(binary.AppendUvarint(e.order[:1], v))
	case time.Time:
		var err error
		out, err = v.AppendText(e.key[:0])
		if err != nil {
			return &json.MarshalerError{Type: reflect.TypeFor[*time.Time](), Err: err}
		}
		e.order[0] = 8
		payload, err := v.AppendBinary(e.order[2:2])
		if err != nil {
			return err
		}
		e.order[1] = byte(len(payload)) //nolint:gosec // time.Time.AppendBinary emits at most 16 bytes.
		e.orderSize = len(payload) + 2
	default:
		return &json.UnsupportedValueError{Str: "document member name must be a string, integer or timestamp"}
	}
	e.size = len(out)
	return nil
}

func (d *documentEmitter) less(i, j int, natural bool) bool {
	a, b := d.entry(i), d.entry(j)
	if natural {
		return naturalLess(a.spelling(), b.spelling())
	}
	if c := normalizedCompare(a.spelling(), b.spelling()); c != 0 {
		return c < 0
	}
	if a.size == 0 && b.size != 0 {
		return false
	}
	if a.size != 0 && b.size == 0 {
		return true
	}
	if a.size == 0 {
		return a.name < b.name
	}
	return bytes.Compare(a.order[:a.orderSize], b.order[:b.orderSize]) < 0
}

func (d *documentEmitter) sort(base, n int, natural bool) {
	down := func(root, size int) {
		for {
			child := root*2 + 1
			if child >= size {
				return
			}
			if child+1 < size && d.less(base+child, base+child+1, natural) {
				child++
			}
			if !d.less(base+root, base+child, natural) {
				return
			}
			a, b := d.entry(base+root), d.entry(base+child)
			*a, *b = *b, *a
			root = child
		}
	}
	for i := n/2 - 1; i >= 0; i-- {
		down(i, n)
	}
	for end := n - 1; end > 0; end-- {
		a, b := d.entry(base), d.entry(base+end)
		*a, *b = *b, *a
		down(0, end)
	}
}

type documentRunes struct {
	text   string
	at     int
	end    int
	rawEnd int
}

func (r *documentRunes) next() (rune, bool) {
	for r.at < len(r.text) {
		if r.at < r.rawEnd {
			r.at++
			return ' ', true
		}
		c, w := utf8.DecodeRuneInString(r.text[r.at:])
		if r.end > r.at {
			r.at += w
			if c == ' ' {
				continue
			}
			if c == 0x85 {
				c = '\n'
			}
			return c, true
		}
		r.end = 0
		if c == ' ' || c == 0x85 {
			finish, after, count := r.at, 0, 0
			for finish < len(r.text) {
				next, n := utf8.DecodeRuneInString(r.text[finish:])
				if next != ' ' && next != 0x85 {
					break
				}
				if next != ' ' {
					if count == 0 {
						after = finish + n
					}
					count++
				}
				finish += n
			}
			if count == 0 {
				r.rawEnd = finish
			}
			if count > 0 {
				if count == 1 {
					r.at = finish
					return ' ', true
				}
				r.end = finish
				r.at = after
				continue
			}
		}
		r.at += w
		return c, true
	}
	return 0, false
}

func normalizedCompare(a, b string) int {
	ar, br := documentRunes{text: a}, documentRunes{text: b}
	for {
		ac, aok := ar.next()
		bc, bok := br.next()
		if !aok {
			if bok {
				return -1
			}
			return 0
		}
		if !bok {
			return 1
		}
		if ac < bc {
			return -1
		}
		if ac > bc {
			return 1
		}
	}
}

func normalizedLength(s string) int {
	n := 0
	r := documentRunes{text: s}
	for {
		c, ok := r.next()
		if !ok {
			return n
		}
		n += utf8.RuneLen(c)
	}
}

func naturalLess(a, b string) bool {
	ar, br := documentRunes{text: a}, documentRunes{text: b}
	previousNonzero := false
	for {
		ac, aok := ar.next()
		bc, bok := br.next()
		if !aok || !bok {
			return !aok && bok
		}
		if ac == bc {
			if !unicode.IsDigit(ac) {
				previousNonzero = false
			} else if ac != '0' {
				previousNonzero = true
			}
			continue
		}
		al, bl := unicode.IsLetter(ac), unicode.IsLetter(bc)
		if al && bl {
			return ac < bc
		}
		if al || bl {
			return bl
		}
		an, bn := int64(0), int64(0)
		if (ac == '0' || bc == '0') && previousNonzero {
			an = 1
			bn = 1
		}
		ai, bi := 0, 0
		for c, ok := ac, true; ok && unicode.IsDigit(c); c, ok = ar.next() {
			an = an*10 + int64(c-'0')
			ai++
		}
		for c, ok := bc, true; ok && unicode.IsDigit(c); c, ok = br.next() {
			bn = bn*10 + int64(c-'0')
			bi++
		}
		if an != bn {
			return an < bn
		}
		if ai != bi {
			return ai < bi
		}
		return ac < bc
	}
}

func validateDocumentString(s string, key bool) error {
	for i, r := range s {
		if r == 0x85 {
			if key {
				return errors.New("document member name contains a YAML line break")
			}
			tail := s[i+2:]
			if len(tail) > 3 && (strings.HasPrefix(tail, "---") || strings.HasPrefix(tail, "...")) && (tail[3] == ' ' || strings.HasPrefix(tail[3:], "\u0085")) {
				return errors.New("document contains a YAML document indicator after a line break")
			}
		}
		if r >= 0x7f && r <= 0x9f && r != 0x85 || r == 0xfffe || r == 0xffff {
			return errors.New("document contains a YAML control character")
		}
	}
	return nil
}

func (d *documentEmitter) number(v float64) []byte {
	if v == 0 {
		return append(d.scalar[:0], '0')
	}
	format := byte('f')
	abs := math.Abs(v)
	if abs < 1e-6 || abs >= 1e21 {
		format = 'e'
	}
	text := strconv.AppendFloat(d.scalar[:0], v, format, -1, 64)
	if format == 'f' {
		s := string(text)
		if !strings.ContainsRune(s, '.') {
			if s[0] == '-' {
				magnitude := s[1:]
				if len(magnitude) < 19 || len(magnitude) == 19 && magnitude <= "9223372036854775808" {
					return text
				}
			} else if len(s) < 20 || len(s) == 20 && s <= "18446744073709551615" {
				return text
			}
		}
	}
	return strconv.AppendFloat(d.scalar[:0], v, 'g', -1, 64)
}

func yamlBreak(r rune) bool { return r == '\n' || r == '\r' || r == 0x85 || r == 0x2028 || r == 0x2029 }

func yamlPrintable(r rune) bool {
	return r == '\n' || r >= 0x20 && r <= 0x7e || r == 0x85 || r >= 0xa0 && r <= 0xd7ff || r >= 0xe000 && r <= 0xfffd && r != 0xfeff
}

func (d *documentEmitter) string(s string, indent int, simpleKey bool) {
	if d.err != nil {
		return
	}
	plain, block, single := true, true, true
	multiline, linefeed, previousSpace, previousBreak := false, false, false, false
	r := documentRunes{text: s}
	current, ok := r.next()
	for first := true; ok && d.err == nil; first = false {
		next, more := r.next()
		blankAfter := !more || next == ' ' || next == '\t'
		if first {
			if strings.ContainsRune("#,[\u005d{}&*!|>'\"%@`", current) || (current == '?' || current == ':' || current == '-') && blankAfter {
				plain = false
			}
			if strings.HasPrefix(s, "---") || strings.HasPrefix(s, "...") {
				plain = false
			}
			if current == ' ' || yamlBreak(current) {
				plain = false
			}
		} else if current == ':' && blankAfter || current == '#' && (previousSpace || previousBreak) {
			plain = false
		}
		if current == ' ' {
			if !more {
				plain = false
				block = false
			}
			if previousBreak {
				plain = false
				single = false
			}
		}
		if yamlBreak(current) {
			multiline = true
			plain = false
			if previousSpace {
				block = false
				single = false
			}
		}
		if current == '\n' {
			linefeed = true
		}
		if !yamlPrintable(current) {
			plain = false
			block = false
			single = false
		}
		previousSpace = current == ' '
		previousBreak = yamlBreak(current) || current == '\t'
		current, ok = next, more
	}
	style := byte('p')
	if yamlAmbiguous(s) {
		style = 'd'
	} else if !plain {
		style = 's'
	}
	if linefeed {
		style = 'l'
	}
	if style == 's' && !single {
		style = 'd'
	}
	if style == 'l' && (!block || simpleKey) {
		style = 'd'
	}
	if simpleKey && multiline {
		style = 'd'
	}
	if style == 'l' {
		d.literal(s, indent)
		return
	}
	switch style {
	case 's':
		d.write("'")
	case 'd':
		d.write("\"")
	}
	spaces, breaks := false, false
	r = documentRunes{text: s}
	current, ok = r.next()
	for first := true; ok && d.err == nil; first = false {
		next, more := r.next()
		if style == 'd' && (!yamlPrintable(current) || yamlBreak(current) || current == 0xfeff || current == '"' || current == '\\') {
			d.escape(current)
			spaces = false
		} else if current == ' ' {
			wrap := !simpleKey && !spaces && d.column > 80
			if style == 's' || style == 'd' {
				wrap = wrap && !first && more
			}
			if style == 'p' || style == 's' {
				wrap = wrap && next != ' '
			}
			if wrap {
				d.indent(indent)
				if style == 'd' && next == ' ' {
					d.write("\\")
				}
			} else {
				d.write(" ")
			}
			spaces = true
		} else if yamlBreak(current) {
			if !breaks && current == '\n' {
				d.write("\n")
			}
			if current == '\r' {
				d.write("\n")
			} else {
				d.rune(current)
			}
			breaks = true
		} else {
			if breaks {
				d.indent(indent)
			}
			if current == '\'' && style == 's' {
				d.write("'")
			}
			d.rune(current)
			spaces = false
			breaks = false
		}
		current, ok = next, more
	}
	switch style {
	case 's':
		d.write("'")
	case 'd':
		d.write("\"")
	}
}

func (d *documentEmitter) rune(r rune) {
	var data [4]byte
	n := utf8.EncodeRune(data[:], r)
	d.write(string(data[:n]))
	if yamlBreak(r) {
		d.column = 0
	}
}

func (d *documentEmitter) escape(r rune) {
	d.write("\\")
	switch r {
	case 0:
		d.write("0")
	case 7:
		d.write("a")
	case 8:
		d.write("b")
	case 9:
		d.write("t")
	case 10:
		d.write("n")
	case 11:
		d.write("v")
	case 12:
		d.write("f")
	case 13:
		d.write("r")
	case 27:
		d.write("e")
	case '"':
		d.write("\"")
	case '\\':
		d.write("\\")
	case 0x85:
		d.write("N")
	case 0xa0:
		d.write("_")
	case 0x2028:
		d.write("L")
	case 0x2029:
		d.write("P")
	default:
		width := 2
		if r > 0xffff {
			d.write("U")
			width = 8
		} else if r > 0xff {
			d.write("u")
			width = 4
		} else {
			d.write("x")
		}
		const hex = "0123456789ABCDEF"
		for shift := (width - 1) * 4; shift >= 0; shift -= 4 {
			d.write(hex[(r>>uint(shift))&15 : ((r>>uint(shift))&15)+1])
		}
	}
}

func (d *documentEmitter) literal(s string, indent int) {
	if utf8.ValidString(s) && !strings.ContainsAny(s, "\u0085\u2028\u2029") {
		d.write("|")
		if len(s) > 0 && (s[0] == ' ' || s[0] == '\n') {
			d.write("2")
		}
		if !strings.HasSuffix(s, "\n") {
			d.write("-")
		} else if len(s) == 1 || strings.HasSuffix(s, "\n\n") {
			d.write("+")
		}
		d.write("\n")
		for len(s) > 0 && d.err == nil {
			end := strings.IndexByte(s, '\n')
			if end < 0 {
				d.indent(indent)
				d.write(s)
				return
			}
			if end > 0 {
				d.indent(indent)
			}
			d.write(s[:end+1])
			s = s[end+1:]
		}
		return
	}
	d.write("|")
	r := documentRunes{text: s}
	first, _ := r.next()
	if first == ' ' || yamlBreak(first) {
		d.write("2")
	}
	last, previous, count := rune(0), rune(0), 0
	r = documentRunes{text: s}
	for d.err == nil {
		c, ok := r.next()
		if !ok {
			break
		}
		previous, last = last, c
		count++
	}
	if !yamlBreak(last) {
		d.write("-")
	} else if count == 1 || yamlBreak(previous) {
		d.write("+")
	}
	d.write("\n")
	breaks := true
	r = documentRunes{text: s}
	for d.err == nil {
		c, ok := r.next()
		if !ok {
			break
		}
		if yamlBreak(c) {
			if c == '\r' {
				d.write("\n")
			} else {
				d.rune(c)
			}
			breaks = true
		} else {
			if breaks {
				d.indent(indent)
			}
			d.rune(c)
			breaks = false
		}
	}
}

func yamlAmbiguous(s string) bool {
	switch s {
	case "", "~", "null", "Null", "NULL", "y", "Y", "yes", "Yes", "YES", "n", "N", "no", "No", "NO", "true", "True", "TRUE", "false", "False", "FALSE", "on", "On", "ON", "off", "Off", "OFF", ".nan", ".NaN", ".NAN", ".inf", ".Inf", ".INF", "+.inf", "+.Inf", "+.INF", "-.inf", "-.Inf", "-.INF", "<<":
		return true
	}
	if len(s) == 0 {
		return true
	}
	if s[0] != '+' && s[0] != '-' && s[0] != '.' && (s[0] < '0' || s[0] > '9') {
		return false
	}
	if yamlTimestamp(s) || yamlSexagesimal(s) {
		return true
	}
	return yamlNumeric(s)
}

func yamlNumeric(s string) bool {
	i := 0
	next := func() byte {
		for i < len(s) && s[i] == '_' {
			if s[0] == '.' && (i == 0 || i+1 == len(s) || s[i-1] < '0' || s[i-1] > '9' || s[i+1] < '0' || s[i+1] > '9') {
				break
			}
			i++
		}
		if i == len(s) {
			return 0
		}
		c := s[i]
		i++
		return c
	}
	c := next()
	negative, positive := c == '-', c == '+'
	if negative || positive {
		c = next()
	}
	if c == '0' {
		saved := i
		prefix := next()
		base := uint64(0)
		switch prefix {
		case 'x', 'X':
			base = 16
		case 'o', 'O':
			base = 8
		case 'b', 'B':
			base = 2
		}
		if base > 0 {
			n, count := uint64(0), 0
			for c = next(); c != 0; c = next() {
				if c < '0' || c > '9' && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
					return false
				}
				v := uint64(c - '0')
				if c >= 'a' && c <= 'f' {
					v = uint64(c-'a') + 10
				}
				if c >= 'A' && c <= 'F' {
					v = uint64(c-'A') + 10
				}
				if v >= base || n > (math.MaxUint64-v)/base {
					return false
				}
				n = n*base + v
				count++
			}
			if negative && n > uint64(1)<<63 || positive && n > math.MaxInt64 {
				return false
			}
			return count > 0
		}
		i = saved
	}
	digits, before, leading := 0, 0, 0
	dot, significant := false, false
	for {
		if c >= '0' && c <= '9' {
			digits++
			if !dot {
				before++
			}
			if c == '0' && !significant {
				leading++
			} else {
				significant = true
			}
		} else if c == '.' && !dot {
			dot = true
		} else {
			break
		}
		c = next()
	}
	exponent := 0
	if (c == 'e' || c == 'E') && digits > 0 {
		c = next()
		minus := c == '-'
		if c == '-' || c == '+' {
			c = next()
		}
		count := 0
		for c >= '0' && c <= '9' {
			exponent = min(100000, exponent*10+int(c-'0'))
			count++
			c = next()
		}
		if count == 0 {
			return false
		}
		if minus {
			exponent = -exponent
		}
	}
	if c != 0 || i < len(s) || digits == 0 {
		return false
	}
	if !significant {
		return true
	}
	power := before - leading - 1 + exponent
	if power != 308 {
		return power < 308
	}
	const overflow = "179769313486231580793728971405303415079934132710037826936173778980444968292764750946649017977587207096330286416692887910946555547851940402630657488671505820681908902000708383676273854845817711531764475730270069855571366959622842914819860834936475292719074168444365510704342711559699508093042880177904174497792"
	at := 0
	for at < len(s) && (s[at] == '+' || s[at] == '-' || s[at] == '0' || s[at] == '.' || s[at] == '_') {
		at++
	}
	for i := range len(overflow) {
		for at < len(s) && (s[at] == '.' || s[at] == '_') {
			at++
		}
		c := byte('0')
		if at < len(s) && s[at] != 'e' && s[at] != 'E' {
			c = s[at]
			at++
		}
		if c != overflow[i] {
			return c < overflow[i]
		}
	}
	return false
}

func yamlSexagesimal(s string) bool {
	i := 0
	if s[0] == '+' || s[0] == '-' {
		i++
	}
	begin := i
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '_') {
		i++
	}
	if i == begin || s[begin] < '0' || s[begin] > '9' {
		return false
	}
	groups := 0
	for i < len(s) && s[i] == ':' {
		i++
		if i == len(s) || s[i] < '0' || s[i] > '9' {
			return false
		}
		a := s[i]
		i++
		if i < len(s) && s[i] >= '0' && s[i] <= '9' {
			if a > '5' {
				return false
			}
			i++
		}
		groups++
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '_') {
			i++
		}
	}
	return groups > 0 && i == len(s)
}

func yamlTimestamp(s string) bool {
	if len(s) < 8 || s[4] != '-' {
		return false
	}
	i := 0
	number := func(limit int) int {
		n, count := 0, 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' && count < limit {
			n = n*10 + int(s[i]-'0')
			i++
			count++
		}
		if count == 0 {
			return -1
		}
		return n
	}
	year := number(4)
	if i != 4 {
		return false
	}
	i++
	month := number(2)
	if i >= len(s) || s[i] != '-' || month < 1 || month > 12 {
		return false
	}
	i++
	day := number(2)
	days := [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		days[1] = 29
	}
	if day < 1 || day > days[month-1] {
		return false
	}
	if i == len(s) {
		return true
	}
	sep := s[i]
	if sep != 'T' && sep != 't' && sep != ' ' {
		return false
	}
	i++
	hour := number(2)
	if hour < 0 || hour > 23 || i >= len(s) || s[i] != ':' {
		return false
	}
	i++
	minute := number(2)
	if minute < 0 || minute > 59 || i >= len(s) || s[i] != ':' {
		return false
	}
	i++
	second := number(2)
	if second < 0 || second > 59 {
		return false
	}
	if i < len(s) && (s[i] == '.' || s[i] == ',') {
		i++
		begin := i
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == begin {
			return false
		}
	}
	if sep == ' ' {
		return i == len(s)
	}
	if i == len(s) {
		return false
	}
	if s[i] == 'Z' {
		return i+1 == len(s)
	}
	if s[i] != '+' && s[i] != '-' {
		return false
	}
	i++
	h := number(2)
	if h < 0 || h > 24 || i >= len(s) || s[i] != ':' {
		return false
	}
	i++
	m := number(2)
	return m >= 0 && m <= 60 && i == len(s)
}
