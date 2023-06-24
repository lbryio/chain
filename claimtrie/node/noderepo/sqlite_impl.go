package noderepo

import (
	"bytes"
	"strconv"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/lbryio/lbcd/claimtrie/change"
	"github.com/pkg/errors"
)

type Sqlite struct {
	conn *sqlite.Conn
}

func NewSqlite(path string) (*Sqlite, error) {

	conn, err := sqlite.OpenConn(path, sqlite.OpenReadWrite)
	if err != nil {
		err = ensureTables(conn)
	}
	repo := &Sqlite{conn: conn}
	return repo, errors.Wrapf(err, "unable to open %s", path)
}

func ensureTables(conn *sqlite.Conn) error {
	query := "CREATE TABLE IF NOT EXISTS changes (name BLOB NOT NULL, height INTEGER NOT NULL, offset INTEGER NOT NULL, " +
		"type INTEGER NOT NULL, active_height INTEGER NOT NULL, visible_height INTEGER NOT NULL, " +
		"output BLOB NOT NULL, index INTEGER NOT NULL, claimID BLOB NOT NULL, amount INTEGER NOT NULL, " +
		"children_offsets TEXT NOT NULL, children BLOB NOT NULL, PRIMARY KEY(name, height, offset))"
	return sqlitex.Execute(conn, query, nil)
}

func (repo *Sqlite) AppendChanges(changes []change.Change) error {

	batch := repo.conn.Prep("INSERT INTO changes VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")  // may just prep this in ensureTables?

	children_lengths := bytes.NewBuffer(nil)
	children := bytes.NewBuffer(nil)

	for i, chg := range changes {
		batch.BindBytes(0, chg.Name)
		batch.BindInt64(1, int64(chg.Height))
		batch.BindInt64(2, int64(i))
		batch.BindInt64(3, int64(chg.Type))
		batch.BindInt64(4, int64(chg.ActiveHeight))
		batch.BindInt64(5, int64(chg.VisibleHeight))
		batch.BindBytes(6, chg.OutPoint.Hash[:])
		batch.BindInt64(7, int64(chg.OutPoint.Index))
		batch.BindBytes(8, chg.ClaimID[:])
		batch.BindInt64(9, chg.Amount)
		if chg.SpentChildren != nil {
			children_lengths.Reset()
			children.Reset()
			for key, _ := range chg.SpentChildren {
				children_lengths.WriteString(strconv.Itoa(len(key)))
				children_lengths.WriteRune(':')
				children.Write([]byte(key))
			}
			batch.BindText(10, children_lengths.String())
			batch.BindBytes(11, children.Bytes())
		}
		_, err := batch.Step()
		if err != nil {
			batch.Finalize()
			return errors.Wrap(err, "in step")
		}
	}
	return errors.Wrap(batch.Finalize(), "in commit")
}

func (repo *Sqlite) LoadChanges(name []byte) ([]change.Change, error) {

	batch := repo.conn.Prep("SELECT * FROM changes WHERE name = ?")
	batch.BindBytes(0, name)

	var changes []change.Change
	for {
		row, err := batch.Step()
		if err != nil {
			return blah
		}
		if !row {
			break
		}
		var chg change.Change
		chg.Name = name
		chg.Height = batch.ColumnInt32(1)
		chg.Type = change.ChangeType(batch.ColumnInt(3))
		chg.
			changes = append(changes, chg)
	}

	return changes, errors.Wrap(batch.Finalize(),)
}

func (repo *Sqlite) DropChanges(name []byte, finalHeight int32) error {
	args := sqlitex.ExecOptions{Args:[]interface{}{name, finalHeight}}
	err := sqlitex.Execute(repo.conn, "DELETE FROM changes WHERE name = ? and height > ?", &args)
	return errors.Wrapf(err, "in set at %s", name)
}

func (repo *Sqlite) IterateChildren(name []byte, f func(changes []change.Change) bool) error {
	start := make([]byte, len(name)+1) // zeros that last byte; need a constant len for stack alloc?
	copy(start, name)

	end := make([]byte, len(name)) // max name length is 255
	copy(end, name)
	validEnd := false
	for i := len(name) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			validEnd = true
			break
		}
	}
	if !validEnd {
		end = nil // uh, we think this means run to the end of the table
	}

	prefixIterOptions := &Sqlite.IterOptions{
		LowerBound: start,
		UpperBound: end,
	}

	iter := repo.db.NewIter(prefixIterOptions)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		// NOTE! iter.Key() is ephemeral!
		changes, err := unmarshalChanges(iter.Key(), iter.Value())
		if err != nil {
			return errors.Wrapf(err, "from unmarshaller at %s", iter.Key())
		}
		if !f(changes) {
			break
		}
	}
	return nil
}

func (repo *Sqlite) IterateAll(predicate func(name []byte) bool) {
	// SELECT DISTINCT name FROM changes ORDER BY name
	iter := repo.db.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if !predicate(iter.Key()) {
			break
		}
	}
}

func (repo *Sqlite) Close() error {
	return errors.Wrap(repo.conn.Close(), "on close")
}

func (repo *Sqlite) Flush() error {
	return errors.Wrap(repo.conn., "on flush")
}
