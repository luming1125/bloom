package bloom

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
)

//FilterWrapper 包装器
type FilterWrapper struct {
	*Filter
	fileName   string
	filterName string
}

//Manager 管理器
type Manager struct {
	cfg      config
	loadReqs chan *loadRequest
	makeReqs chan *makeRequest
}

type loadRequest struct {
	filterName string
	response   chan<- result
}

type makeRequest struct {
	n        uint32
	k        uint
	name     string
	response chan<- result
}

type config struct {
	dir    string
	suffix string
}

type entry struct {
	res   *result
	ready chan struct{}
}

type result struct {
	fw  *FilterWrapper
	err error
}

//GetManager 获取Manager
func GetManager(dir string) *Manager {
	cfg := config{dir: dir, suffix: ".bf"}

	m := &Manager{cfg: cfg, loadReqs: make(chan *loadRequest), makeReqs: make(chan *makeRequest)}

	go m.server()

	return m
}

//CreateBloomFromFile 使用文件创建过滤器，文件一行一条数据，以'\n'结尾
func (manager *Manager) CreateBloomFromFile(srcFile string, name string) (string, error) {
	f, err := os.Open(srcFile)
	defer f.Close()
	//文件不存在直接返回
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(f)
	count := uint32(0)
	for scanner.Scan() {
		count++
	}

	if count <= 0 {
		return "", fmt.Errorf("file %s is empty", srcFile)
	}

	fw, err := manager.Make(count, 8, name)
	if err != nil {
		return "", err
	}

	f.Seek(0, 0)
	scanner = bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		fw.Add(line)
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	err = fw.Flush()
	if err != nil {
		return "", err
	}

	return fw.filterName, nil
}

//Check 检查是否在过滤器内
func (manager *Manager) Check(name string, data string) (bool, error) {
	fw, err := manager.Load(name)
	if err != nil {
		return false, err
	}

	return fw.Check(data), nil
}

//Load 从文件读取filter
func (manager *Manager) Load(name string) (*FilterWrapper, error) {
	resp := make(chan result)
	manager.loadReqs <- &loadRequest{filterName: name, response: resp}

	res := <-resp
	return res.fw, res.err
}

//Make 创建新的bloomfilter
func (manager *Manager) Make(n uint32, k uint, name string) (*FilterWrapper, error) {
	resp := make(chan result)
	manager.makeReqs <- &makeRequest{n: n, k: k, name: name, response: resp}

	res := <-resp

	return res.fw, res.err
}

func (manager *Manager) fullPath(name string) string {
	return manager.cfg.dir + name + manager.cfg.suffix
}

func (manager *Manager) server() {
	cache := make(map[string]*entry)
	for {
		select {
		case lr := <-manager.loadReqs:
			e := cache[lr.filterName]
			if e == nil {
				e = &entry{ready: make(chan struct{})}
				cache[lr.filterName] = e
				go e.load(manager, lr.filterName)
			}

			go e.deliver(lr.response)
		case mr := <-manager.makeReqs:
			e := cache[mr.name]
			if e == nil {
				e = &entry{ready: make(chan struct{})}
				cache[mr.name] = e
				go e.make(manager, mr.name, mr.n, mr.k)
			}

			go e.deliver(mr.response)
		default:
			break
		}
	}
}

func (e *entry) load(manager *Manager, name string) {
	path := manager.fullPath(name)

	raw, err := ioutil.ReadFile(path)
	if err != nil {
		e.res = &result{nil, err}
	}

	buffer := bytes.NewBuffer(raw)
	decoder := gob.NewDecoder(buffer)

	var filter Filter
	err = decoder.Decode(&filter)
	if err != nil {
		e.res = &result{nil, err}
	}

	fw := &FilterWrapper{Filter: &filter, fileName: path, filterName: name}

	e.res = &result{fw, nil}

	close(e.ready)
}

func (e *entry) make(manager *Manager, name string, n uint32, k uint) {
	path := manager.fullPath(name)

	filter := New(n, k)
	fw := &FilterWrapper{Filter: filter, fileName: path, filterName: name}

	e.res = &result{fw, nil}

	close(e.ready)
}

func (e *entry) deliver(resp chan<- result) {
	<-e.ready
	resp <- *(e.res)
}

//Flush 写入文件
func (fw *FilterWrapper) Flush() error {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(*(fw.Filter))
	if err != nil {
		return err
	}

	return ioutil.WriteFile(fw.fileName, buffer.Bytes(), 0600)
}
