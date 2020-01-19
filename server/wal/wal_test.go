package wal

import (
	"os"
	"reflect"
	"testing"

	"github.com/rs/zerolog"

	"github.com/mrasu/ddb/server/structs"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	code := m.Run()
	os.Exit(code)
}

func TestWal_CurrentLsn(t *testing.T) {
	w := NewTestWal(&Memory{})
	if w.CurrentLsn() != 0 {
		t.Errorf("Invalid lsn: %d", w.CurrentLsn())
	}

	w.lsn = 10
	if w.CurrentLsn() != 10 {
		t.Errorf("Invalid lsn: %d", w.CurrentLsn())
	}
}

func TestWal_Write(t *testing.T) {
	w := NewTestWal(&Memory{})
	if w.CurrentLsn() != 0 {
		t.Errorf("Invalid lsn initialization: %d", w.CurrentLsn())
	}

	dcs := &structs.CreateDBChangeSet{Name: "hello"}
	err := w.Write(dcs)
	if err != nil {
		t.Error(err)
	}

	if w.CurrentLsn() != 1 {
		t.Errorf("Lsn didn't proceed: %d", w.CurrentLsn())
	}

	assertDBChangeSets(t, w, []*structs.CreateDBChangeSet{
		{Name: "hello"},
	})
}

func TestWal_Write_MultipleTimes(t *testing.T) {
	w := NewTestWal(&Memory{})
	if w.CurrentLsn() != 0 {
		t.Errorf("Invalid lsn initialization: %d", w.CurrentLsn())
	}

	origins := []structs.ChangeSet{
		&structs.CreateDBChangeSet{Name: "hello1"},
		&structs.CreateDBChangeSet{Name: "hello2"},
	}
	for _, cs := range origins {
		err := w.Write(cs)
		if err != nil {
			t.Error(err)
		}
	}

	if w.CurrentLsn() != 2 {
		t.Errorf("Lsn didn't proceed: %d", w.CurrentLsn())
	}

	assertDBChangeSets(t, w, []*structs.CreateDBChangeSet{
		{Name: "hello1"},
		{Name: "hello2"},
	})
}

func TestWal_WriteSlice(t *testing.T) {
	w := NewTestWal(&Memory{})
	if w.CurrentLsn() != 0 {
		t.Errorf("Invalid lsn initialization: %d", w.CurrentLsn())
	}

	origins := []structs.ChangeSet{
		&structs.CreateDBChangeSet{Name: "hello1"},
		&structs.CreateDBChangeSet{Name: "hello2"},
	}
	err := w.WriteSlice(origins)
	if err != nil {
		t.Error(err)
	}

	if w.CurrentLsn() != 2 {
		t.Errorf("Lsn didn't proceed: %d", w.CurrentLsn())
	}

	assertDBChangeSets(t, w, []*structs.CreateDBChangeSet{
		{Name: "hello1"},
		{Name: "hello2"},
	})
}

func TestWal_Read(t *testing.T) {
	var Rows []structs.ChangeSet
	for num := range structs.QueryTypeMap {
		cs, err := structs.ToChangeSet(num)
		if err != nil {
			t.Error(err)
		}
		Rows = append(Rows, cs)
	}
	w := NewTestWal(&Memory{})

	err := w.WriteSlice(Rows)
	if err != nil {
		t.Error(err)
	}

	css, err := w.Read()
	if err != nil {
		t.Error(err)
	}
	if len(css) != len(Rows) && len(Rows) > 0 {
		t.Errorf("Saving invalid wal size: %d", len(css))
	}

	for i, cs := range css {
		eCs := Rows[i]
		if reflect.TypeOf(cs) != reflect.TypeOf(eCs) {
			t.Errorf("Invalid type record: %T, %T", cs, eCs)
		}

		if cs.GetLsn() != int64(i) {
			t.Errorf("Invalid lsn: %v", cs)
		}
	}
}

func assertDBChangeSets(t *testing.T, w *Wal, eCss []*structs.CreateDBChangeSet) {
	css, err := w.Read()
	if err != nil {
		t.Error(err)
	}
	if len(css) != len(eCss) {
		t.Errorf("Saving invalid wal size: %d", len(css))
	}
	for i, eCs := range eCss {
		cs := css[i]
		dcs, ok := cs.(*structs.CreateDBChangeSet)
		if !ok {
			t.Errorf("Invalid ChangeSet type: %v", css[0])
		}
		if dcs.Name != eCs.Name {
			t.Errorf("Invalid ChangeSet content: %v", cs)
		}
		if dcs.Lsn != int64(i) {
			t.Errorf("Invalid lsn: %v", cs)
		}
	}
}
