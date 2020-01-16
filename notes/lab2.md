### Part 2A Leader election and heartbeat

定时触发election，还需要能重置。官方说用time.Sleep()，
其实可以用select，一个case是time.After()，再用一个channel用来重置。

所有接收rpc request或response都要检查term，如果消息里的term大于自己的term，变成follower。

Mutex不可重入，不能写嵌套的Lock, Unlock。

发RequestVote或heartbeat，每个rpc一个goroutine。

### Part 2B

需要往 applyCh 发已经commit的log。因为会阻塞，所以需要一个单独的goroutine来发。因为需要保证顺序，
所以只能有一个goroutine。官方推荐用sync.Cond。具体做法是，发apply的goroutine等待一个条件（lastApplied < commitIndex），
更新commitIndex的线程发Signal唤醒它。
