package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"strings"
	"time"
)

// 节点结构体定义
type node struct {
	connect bool
	address string // 节点地址
}

// State 定义
type State int

// Raft节点三种状态：Follower、Candidate、Leader
const (
	Follower State = iota + 1
	Candidate
	Leader
)

// Raft Node
type Raft struct {

	// 当前节点id
	me int

	// 除当前节点外其他节点信息
	nodes map[int]*node

	// 当前节点状态
	state State

	// 当前任期
	currentTerm int

	// 当前任期投票给了谁
	// 未投票设置为-1
	votedFor int

	// 当前任期获取的投票数量
	voteCount int

	// heartbeat channel
	heartbeatC chan bool

	// to leader channel
	toLeaderC chan bool
}

// 新建节点
func newNode(address string) *node {
	node := &node{}
	node.address = address
	return node
}

// 创建 Raft
func (rf *Raft) start() {

	// 初始化 Raft 节点
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatC = make(chan bool)
	rf.toLeaderC = make(chan bool)

	// 节点状态变更以及 RPC 处理
	// ...

	// 开启 goroutine
	go func() {

		rand.Seed(time.Now().UnixNano())

		// 不间断处理节点行为和RPC
		for {
			switch rf.state {

			// 如果是 follower 状态
			case Follower:
				select {
				// 从 heartbeatC 管道成功接收到 heartbeat
				case <-rf.heartbeatC:
					log.Printf("follower-%d recived heartbeat\n", rf.me)
					// heartbeat 超时，follower 变成 candidate 状态
				case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
					log.Printf("follower-%d timeout\n", rf.me)
					rf.state = Candidate
				}

				// 其他状态处理
				// ...
			}
		}()
	}

	go func() {

		rand.Seed(time.Now().UnixNano())

		for {
			switch rf.state {
			// ...

			// Candidate 状态
			case Candidate:
				fmt.Printf("Node: %d, I'm candidate\n", rf.me)
				// 当前 term + 1
				rf.currentTerm++
				// 为自己投票
				rf.votedFor = rf.me
				rf.voteCount = 1

				// 向其他节点广播投票申请
				go rf.broadcastRequestVote()

				select {
				// 选举超时，状态转化为 follower
				case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
					rf.state = Follower
				// 选举成功
				case <-rf.toLeaderC:
					fmt.Printf("Node: %d, I'm leader\n", rf.me)
					rf.state = Leader
				}

				// ...
			}
		}()
	}
}

// request
type VoteArgs struct {
	// 当前任期号
	Term        int
	// 候选人id
	CandidateID int
}

// response
type VoteReply struct {
	// 当前任期号
	// 如果其他节点大于当前Candidate节点term
	// 以便Candiate去更新自己的任期号
	Term int
	//候选人赢得此张选票时为真
	VoteGranted bool
}

func (rf *Raft) broadcastRequestVote() {

	// 设置 request
	var args = VoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	// rf.nodes 保存了其他 node
	// 遍历 nodes 并发送投票申请
	for i := range rf.nodes {
		go func(i int) {
			var reply VoteReply
			// 发送申请到某个节点
			rf.sendRequestVote(i, args, &reply)
		}(i)
	}
}

// 发送申请到某个节点
// serverID - server 唯一标识
// args - request 内容
// reply - follower response
func (rf *Raft) sendRequestVote(serverID int, args VoteArgs, reply *VoteReply) {
	// 创建 client
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer client.Close()

	// 调用follower 节点的 RequestVote 方法
	client.Call("Raft.RequestVote", args, reply)

	// 如果candiate节点term小于follower节点
	// 当前candidate节点无效，candidate节点
	// 转变为follower节点
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return
	}

	// 成功获得投票
	if reply.VoteGranted {
		// 票数 + 1
		rf.voteCount++
		// 获取票数大于集群的一半（N/2+1）
		if rf.voteCount > len(rf.nodes)/2+1 {
			// 发送通知给 toLeaderC channel
			rf.toLeaderC <- true
		}
	}
}

// Follower 处理投票申请
func (rf *Raft) RequestVote(args VoteArgs, reply *VoteReply) error {

	// 如果Candidate节点term小于follower
	// 说明Candidate节点过时，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// follower 未给其他节点投票，投票成功
	if rf.votedFor == -1 {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return nil
	}

	// 其他情况
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return nil
}

func (rf *Raft) start() {

	// ...

	go func() {

		rand.Seed(time.Now().UnixNano())

		for {
			switch rf.state {
			// ...
			case Leader:
				// 每隔100ms向其他节点发送一次heartbeat
				rf.broadcastHeartbeat()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

// request
type HeartbeatArgs struct {
	// 当前leader term
	Term     int
	// 当前 leader id
	LeaderID int
}

type HeartbeatReply struct {
	// 当前 follower term
	Term int
}

// 广播 heartbeat
func (rf *Raft) broadcastHeartbeat() {
	// 遍历所有节点
	for i := range rf.nodes {
		// request 参数
		args := HeartbeatArgs{
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}

		go func(i int, args HeartbeatArgs) {
			var reply HeartbeatReply
			// 向某一个节点发送 heartbeat
			rf.sendHeartbeat(i, args, &reply)
		}(i, args)
	}
}

// 向某一个节点发送 heartbeat
func (rf *Raft) sendHeartbeat(serverID int, args HeartbeatArgs, reply *HeartbeatReply) {
	// 创建 RPC client
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	defer client.Close()
	// 调用 follower 节点 Heartbeat 方法
	client.Call("Raft.Heartbeat", args, reply)

	// 如果follower节点term大于leader节点term
	// leader节点过时，leader转变为follower状态
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
	}
}

// Heartbeat rpc method
func (rf *Raft) Heartbeat(args HeartbeatArgs, reply *HeartbeatReply) error {

	// 如果leader节点term小于follower节点，不做处理并返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	// 如果leader节点term大于follower节点
	// 说明 follower 过时，重置follower节点term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// 将当前folower节点term返回给leader
	reply.Term = rf.currentTerm

	// 心跳成功，将消息发给 heartbeatC 通道
	rf.heartbeatC <- true

	return nil
}

func main() {

	// 参数定义和默认值
	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9091", "comma sep")
	id := flag.Int("id", 1, "node ID")

	// 参数解析
	flag.Parse()
	clusters := strings.Split(*cluster, ",")
	ns := make(map[int]*node)
	for k, v := range clusters {
		ns[k] = newNode(v)
	}

	// 创建节点
	raft := &Raft{}
	raft.me = *id
	raft.nodes = ns

	// 监听rpc
	raft.rpc(*port)
	// 开启 raft
	raft.start()

	select {}

}