package plainkv

import (
	"strconv"
	"testing"
)

func TestOpen(t *testing.T) {

	pkv := NewPlainKV("sample:password101@tcp(192.168.1.129)/kvdb", false)
	if err := pkv.Open(); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	if err := pkv.Set(`sample_key`, []byte(`Sample value`)); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	b, err := pkv.Get(`sample_key`)
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	t.Logf(`Retrieved from the database: %s`, b)

	err = pkv.Del(`sample_key`)
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	pkv.Close()
}

func TestOpenMime(t *testing.T) {

	pkv := NewPlainKV("sample:password101@tcp(192.168.1.129)/kvdb", false)
	if err := pkv.Open(); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	if err := pkv.Set(`sample_key`, []byte(`Sample value`)); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	pkv.SetMime(`sample_key`, `application/json`)

	b, err := pkv.Get(`sample_key`)
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	mime, err := pkv.GetMime(`sample_key`)
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	t.Logf(`Retrieved from the database: %s as %s`, b, mime)

	pkv.Close()
}

func TestOpenListKeys(t *testing.T) {

	pkv := NewPlainKV("sample:password101@tcp(192.168.1.129)/kvdb", false)
	if err := pkv.Open(); err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	strs, err := pkv.ListKeys("sample")
	if err != nil {
		t.Logf(`%s`, err)
		t.Fail()
	}

	for _, v := range strs {
		b, err := pkv.Get(v)
		if err != nil {
			t.Logf(`%s`, err)
			t.Fail()
		}

		t.Logf(`Retrieved from the database: %s`, b)
	}

	pkv.Close()
}

func BenchmarkPerformance(b *testing.B) {

	pkv := NewPlainKV("sample:password101@tcp(192.168.1.129)/kvdb", false)
	if err := pkv.Open(); err != nil {
		b.Logf(`%s`, err)
		b.Fail()
	}

	for i := 0; i < 100000; i++ {
		if err := pkv.Set(`sample_key`+strconv.Itoa(i), []byte(`Sample value `+strconv.Itoa(i))); err != nil {
			b.Logf(`%s`, err)
			b.Fail()
		}
	}

	pkv.Close()
}
