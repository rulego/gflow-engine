package service

import (
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// execGate 实例级可重入门闩：同一实例的 ExecuteNext（状态判定 + 链驱动）串行化，
// 消除"并发 approve 的事务提交后同时进入 ExecuteNext → 双方都判定 fork-join 收齐
// → 双 multi-restore → end 节点/副作用重复执行"的竞态窗口。
//
// 为什么可重入：链内节点（aiAgent 回跳 / userTask reject 路径）会在 OnMsg 的同一
// goroutine 里对同一实例再调 ExecuteNext。普通互斥锁会自死锁；通过 goroutine ID
// 识别同 goroutine 重入直接放行（外层驱动已持有门闩，本次执行天然被串行化覆盖）。
// 注：跨进程部署由 WithInstanceTx 行锁 + end 节点 TryLock 去重兜底，此处只管进程内。
type execGate struct {
	mu      sync.Mutex
	cond    *sync.Cond
	owner   uint64    // 持有者 goroutine ID；0 = 空闲
	depth   int       // 持有者重入计数
	waiters int       // 等待中的其他 goroutine 数（用于空闲回收）
	key     string    // 所属 instanceID（空闲回收时从 map 删除用）
	gates   *sync.Map // 所属 RuntimeServiceImpl 的门闩表（空闲回收用）
}

// goroutineID 从 runtime.Stack 解析当前 goroutine ID。
// 仅用于可重入锁的持有者识别（不做并发原语语义），解析失败返回 0（视为不可重入）。
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return 0
	}
	id, _ := strconv.ParseUint(fields[1], 10, 64)
	return id
}

// acquireExecGate 获取实例门闩，返回 (释放函数, 是否为同 goroutine 重入)。
// 门闩表挂在 RuntimeServiceImpl 上：多引擎共存时互不干扰。
func (s *RuntimeServiceImpl) acquireExecGate(instanceID string) (release func(), reentrant bool) {
	for {
		gAny, _ := s.execGates.LoadOrStore(instanceID, &execGate{key: instanceID, gates: &s.execGates})
		g := gAny.(*execGate)
		gid := goroutineID()

		g.mu.Lock()
		if g.cond == nil {
			g.cond = sync.NewCond(&g.mu)
		}
		if g.owner == 0 {
			// 释放方在 waiters==0 时会把门闩从 map 回收。若本 goroutine 在
			// LoadOrStore 之后、Lock 之前恰好撞上一次释放+回收，这里拿到的就是
			// 孤儿门闩：直接持有会让后到者 LoadOrStore 建新门闩，双持有击穿
			// 串行化。校验 map 里的现行条目仍是本门闩，不是就重取。
			if cur, ok := s.execGates.Load(instanceID); !ok || cur != g {
				g.mu.Unlock()
				continue
			}
			g.owner, g.depth = gid, 1
			g.mu.Unlock()
			return g.releaseFunc(), false
		}
		if g.owner == gid {
			g.depth++
			g.mu.Unlock()
			return g.releaseFunc(), true
		}
		g.waiters++
		for g.owner != 0 {
			g.cond.Wait()
		}
		g.waiters--
		// 等待路径无需孤儿校验：回收仅发生在 waiters==0 时，本 goroutine
		// 作为等待者存在期间门闩不可能被删除。
		g.owner, g.depth = gid, 1
		g.mu.Unlock()
		return g.releaseFunc(), false
	}
}

// releaseFunc 返回与门闩状态绑定的释放函数（幂等）。
// 释放到最外层时若无等待者则回收 map 条目，避免长期运行下条目无限增长。
func (g *execGate) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.depth--
			if g.depth <= 0 {
				g.depth = 0
				g.owner = 0
				if g.waiters == 0 && g.key != "" && g.gates != nil {
					g.gates.Delete(g.key)
				}
			}
			g.cond.Broadcast()
			g.mu.Unlock()
		})
	}
}
