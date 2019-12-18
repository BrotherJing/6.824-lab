### Task1 Map/Reduce

Map: 读input，调用map方法，然后根据key hash到N个文件，N是reducer的数量。

Reduce: 从M个map读到自己hash的那个文件，合并，对每个key下面的value list调用reduce方法，然后排序，写output。

### Task2 Word count

Use `strings.FieldsFunc` to split string into word list.

### Task3 Parallel

Channel, 多路复用, sync.WaitGroup

#### Concurrency in go

Shared values are passed around on channels.

channel可以有多个线程同时监听。只会被一个拿到

```go
for r := range channel {
}
```

多路复用

如果所有channel都block，走default
```go
for {
  select {
    case r = <-channel1:
    default:
  }
}
```

### Task4 Handle failure

Mutex, break for loop.

task作为一个channel，失败的task再写回channel。mutex维护一个counter，记录成功的task数量，全部完成退出循环。
