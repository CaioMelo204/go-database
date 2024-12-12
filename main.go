package main

import (
	"encoding/json"
	"fmt"
	"github.com/jcelliott/lumber"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

const Version = "1.0.1"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	driver := &Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Database already exists", dir)
		return driver, nil
	}

	opts.Logger.Debug("Creating database directory", dir)

	return driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection, resourse string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("collection is required")
	}
	if resourse == "" {
		return fmt.Errorf("resourse is required")
	}
	mutex := d.getOrCreateNewMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resourse+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.Marshal(v)

	if err != nil {
		return err
	}

	b = append(b, byte('\n'))

	if err = ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("collection is required")
	}

	if resource == "" {
		return fmt.Errorf("resource is required")
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := os.Stat(record); os.IsNotExist(err) {
		return err
	}

	b, err := ioutil.ReadFile(record + ".json")

	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection is required")
	}

	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var records []string

	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(d.dir, collection, resource)
	mutex := d.getOrCreateNewMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	switch fi, err := stat(path); {
	case fi == nil, err != nil:
		return fmt.Errorf("%s does not exist", path)
	case fi.Mode().IsDir():
		return os.RemoveAll(path)
	case fi.Mode().IsRegular():
		return os.RemoveAll(path + ".json")
	}
	return nil
}

func stat(path string) (f os.FileInfo, err error) {
	if f, err = os.Stat(path); os.IsNotExist(err) {
		f, err = os.Stat(path + ".json")
	}
	return
}

func (d *Driver) getOrCreateNewMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

type Address struct {
	City    string
	State   string
	Country string
	Code    json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./db"

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println(err)
	}

	employees := []User{
		{"John", "23", "23344333", "Myrl Tech", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Paul", "25", "23344333", "Google", Address{"san francisco", "california", "USA", "410013"}},
		{"Robert", "27", "23344333", "Microsoft", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Vince", "29", "23344333", "Facebook", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Neo", "31", "23344333", "Remote-Teams", Address{"bangalore", "karnataka", "india", "410013"}},
		{"Albert", "32", "23344333", "Dominate", Address{"bangalore", "karnataka", "india", "410013"}},
	}

	for _, user := range employees {
		db.Write("user", user.Name, User{
			Name:    user.Name,
			Age:     user.Age,
			Contact: user.Contact,
			Company: user.Company,
			Address: Address{
				City:    user.Address.City,
				State:   user.Address.State,
				Country: user.Address.Country,
				Code:    user.Address.Code,
			},
		})
	}

	records, err := db.ReadAll("user")
	if err != nil {
		fmt.Println(err)
	}

	for _, record := range records {
		fmt.Println(record)
	}

	allusers := []User{}

	for _, f := range records {
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			fmt.Println(err)
		}
		allusers = append(allusers, employeeFound)
	}

	for _, employee := range allusers {
		fmt.Println(employee)
	}

	//if err = db.Delete("user", "john"); err != nil {
	//	fmt.Println(err)
	//}
	//
	if err = db.Delete("user", ""); err != nil {
		fmt.Println(err)
	}
}

//TIP See GoLand help at <a href="https://www.jetbrains.com/help/go/">jetbrains.com/help/go/</a>.
// Also, you can try interactive lessons for GoLand by selecting 'Help | Learn IDE Features' from the main menu.
