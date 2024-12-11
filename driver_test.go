package kuzu

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestDriver(t *testing.T) {
	ctx := nextContext()
	dir := filepath.Join(os.TempDir(), "kuzu")
	t.Log(dir)
	cc, err := sql.Open(Name, fmt.Sprintf("kuzu://%s", dir))
	if nil != err {
		t.Error(err)
		return
	}
	defer func() {
		if err = os.RemoveAll(dir); nil != err {
			t.Error(err)
		}
	}()
	exps := []string{
		"CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))",
		"CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))",
		"CREATE REL TABLE Follows(FROM User TO User, since INT64)",
		"CREATE REL TABLE LivesIn(FROM User TO City)",
		"COPY User FROM \"dataset/demo-db/user.csv\"",
		"COPY City FROM \"dataset/demo-db/city.csv\"",
		"COPY Follows FROM \"dataset/demo-db/follows.csv\"",
		"COPY LivesIn FROM \"dataset/demo-db/lives-in.csv\"",
	}

	for _, expr := range exps {
		r, err := cc.ExecContext(ctx, expr)
		if nil != err {
			t.Error(err)
			return
		}
		t.Log(r.RowsAffected())
	}

	doQuery := func(query string, args map[string]any) ([]map[string]string, error) {
		var namedArgs []any
		for n, v := range args {
			namedArgs = append(namedArgs, sql.Named(n, v))
		}
		rows, err := cc.QueryContext(ctx, query, namedArgs...)
		if nil != err {
			return nil, err
		}
		defer closeQuiet(rows)
		cols, err := rows.Columns()
		if nil != err {
			return nil, err
		}
		var datum []map[string]string
		for rows.Next() {
			set := make([]any, len(cols))
			for idx, _ := range set {
				set[idx] = new(sql.NullString)
			}
			if err = rows.Scan(set...); nil != err {
				return nil, err
			}
			data := make(map[string]string, len(cols))
			for idx, v := range set {
				if ns, ok := v.(*sql.NullString); ok && ns.Valid {
					data[cols[idx]] = ns.String
				} else {
					data[cols[idx]] = ""
				}
			}
			datum = append(datum, data)
		}
		return datum, nil
	}

	queries := map[string]map[string]any{
		"MATCH (a:User)-[e:Follows]->(b:User) RETURN a.name, e.since, b.name": {},
		"MATCH (a:User) WHERE a.name = $name RETURN a.age":                    {"name": "Adam"},
	}
	for query, args := range queries {
		rs, err := doQuery(query, args)
		if nil != err {
			t.Error(err)
			return
		}
		t.Log(rs)
	}
}
