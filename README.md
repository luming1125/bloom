# bloom

## 特点

- 基于二进制文件的布隆过滤器
- 并发安全

## 使用

- 基于白名单文件生成布隆过滤器
```go
m := bloom.GetManager("/your/path/") //文件生成路径
filter, err := m.CreateBloomFromFile("/file/path/name.txt", "fist-test")//传入白名单文件和过滤器名称
fmt.Println(filter) //打印过滤器名称
```
- 查看是否在布隆过滤器内
```go
m := bloom.GetManager("/your/path/")
filter := "first-test"
result, err := m.Check(filter, "test-data")
```


