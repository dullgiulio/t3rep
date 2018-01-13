package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type conf struct {
	Query     string
	Fields    int
	Months    int
	Directory string
	Systems   map[string]string
}

func newConf() *conf {
	return &conf{
		Systems: make(map[string]string),
	}
}

func (c *conf) load(fname string) error {
	fh, err := os.Open(fname)
	if err != nil {
		return fmt.Errorf("cannot open configuration file: %v", err)
	}
	defer fh.Close()
	dec := json.NewDecoder(fh)
	if err := dec.Decode(c); err != nil {
		return fmt.Errorf("cannot decode JSON configuration: %v", err)
	}
	return nil
}

func formatCSV(buf *bytes.Buffer, fields []string) []byte {
	buf.Reset()
	for i, f := range fields {
		f = strings.Replace(f, "\"", "\\\"", -1) // quote
		if i > 0 {
			fmt.Fprintf(buf, ";\"%s\"", f)
		} else {
			fmt.Fprintf(buf, "\"%s\"", f)
		}
	}
	buf.Write([]byte("\r\n"))
	return buf.Bytes()
}

func extract(w io.Writer, rows *sql.Rows, nassoc int) error {
	assoc := make([]string, nassoc)
	params := make([]interface{}, nassoc)
	for i := 0; i < nassoc; i++ {
		params[i] = interface{}(&assoc[i])
	}
	var buf bytes.Buffer
	for rows.Next() {
		if err := rows.Scan(params...); err != nil {
			return fmt.Errorf("cannot scan query: %v", err)
		}
		line := formatCSV(&buf, assoc)
		if _, err := w.Write(line); err != nil {
			return fmt.Errorf("cannot write line: %v", err)
		}
	}
	return nil
}

type reporter struct {
	db     *sql.DB
	query  string
	nassoc int
}

func newReporter(db *sql.DB, query string, nassoc int) *reporter {
	return &reporter{
		db:     db,
		query:  query,
		nassoc: nassoc,
	}
}

func (r *reporter) write(w io.Writer, rep *report) error {
	rows, err := rep.rows(r.db, r.query)
	if err != nil {
		return fmt.Errorf("cannot query: %v", err)
	}
	return extract(w, rows, r.nassoc)
}

func (r *reporter) generate(l *logs, rep *report) {
	l.info.Printf("%s: generating report", rep)
	if rep.exists() {
		l.info.Printf("%s: exists", rep)
		return
	}
	f, err := os.Create(rep.fname)
	if err != nil {
		l.err.Printf("%s: cannot create file", rep)
		return
	}
	if err := r.write(f, rep); err != nil {
		l.err.Printf("%s: cannot generate report: %v", rep, err)
		f.Close() // ignore error, we remove this file
		if err = os.Remove(rep.fname); err != nil {
			l.err.Printf("%s: cannot removed partial report file, remove it manually: %v", rep, err)
		}
		return
	}
	if err = f.Close(); err != nil {
		l.err.Fatalf("%s: cannot close file: %v", rep, err)
		if err = os.Remove(rep.fname); err != nil {
			l.err.Printf("%s: cannot removed partial report file, remove it manually: %v", rep, err)
		}
	}
}

type report struct {
	name  string
	fname string
	start time.Time
	end   time.Time
}

func makeReport(name, dir string, start time.Time) report {
	return report{
		name:  name,
		start: start,
		end:   start.AddDate(0, 1, -1).Add(23*time.Hour + 59*time.Minute + 59*time.Second),
		fname: filepath.Join(dir, fmt.Sprintf("%s-%d-%02d.csv", name, start.Year(), start.Month())),
	}
}

func (r *report) rows(db *sql.DB, query string) (*sql.Rows, error) {
	return db.Query(query, r.start.Format("2006-01-02 15:04:05"), r.end.Format("2006-01-02 15:04:05"))
}

func (r *report) exists() bool {
	_, err := os.Stat(r.fname)
	return !os.IsNotExist(err)
}

func (r *report) String() string {
	return r.fname
}

type task struct {
	t      time.Time
	name   string
	dir    string
	dsn    string
	query  string
	months int
	fields int
}

func newTask(t time.Time, name, dsn string, cf *conf) *task {
	return &task{
		t:      t,
		name:   name,
		dsn:    dsn,
		dir:    cf.Directory,
		query:  cf.Query,
		months: cf.Months,
		fields: cf.Fields,
	}
}

func (t *task) exec(l *logs) error {
	db, err := sql.Open("mysql", t.dsn)
	if err != nil {
		return fmt.Errorf("%s: cannot connect to database: %v", t.name, err)
	}
	defer db.Close()
	reporter := newReporter(db, t.query, t.fields)
	reps := t.reports()
	for i := range reps {
		reporter.generate(l, &reps[i])
	}
	return nil
}

func (t *task) reports() []report {
	last := time.Date(t.t.Year(), t.t.Month(), 1, 0, 0, 0, 0, time.Local)
	reports := make([]report, t.months)
	for i := 0; i < t.months; i++ {
		last = last.AddDate(0, -1, 0) // remove one month
		reports[i] = makeReport(t.name, t.dir, last)
	}
	return reports
}

type logs struct {
	err  *log.Logger
	info *log.Logger
}

func work(t *task, logs *logs) error {
	if err := t.exec(logs); err != nil {
		return fmt.Errorf("creating report: %v", err)
	}
	return nil
}

func run(sem chan struct{}, cf *conf, logs *logs, now time.Time) {
	errch := make(chan error, len(cf.Systems))
	for name, dsn := range cf.Systems {
		go func(name, dsn string) {
			sem <- struct{}{}
			t := newTask(now, name, dsn, cf)
			errch <- work(t, logs)
			<-sem
		}(name, dsn)
	}
	for i := 0; i < len(cf.Systems); i++ {
		if err := <-errch; err != nil {
			logs.err.Printf("fatal: %v", err)
		}
	}
}

func main() {
	parallel := flag.Int("parallel", 0, "Number of concurrent report creations")
	verbose := flag.Bool("verbose", false, "Show information messages for debugging")
	flag.Parse()
	logs := &logs{
		err:  log.New(os.Stderr, "ERROR - ", log.LstdFlags),
		info: log.New(ioutil.Discard, "INFO  - ", log.LstdFlags),
	}
	if *verbose {
		logs.info = log.New(os.Stdout, "INFO  - ", log.LstdFlags)
	}
	cfile := flag.Arg(0)
	if cfile == "" {
		logs.err.Fatal("usage: t3rep CONFFILE")
	}
	cf := newConf()
	if err := cf.load(cfile); err != nil {
		logs.err.Fatalf("fatal: cannot start: %v", err)
	}
	if *parallel > 0 {
		*parallel = 1
	}
	sem := make(chan struct{}, *parallel)
	run(sem, cf, logs, time.Now())
}
