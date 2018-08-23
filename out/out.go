package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lorock/proxy/msg"
	"github.com/lorock/proxy/utils"
)

var (
	gConfig *Config = nil
)

type connection struct {
	c            *connectionMng
	svr          *net.TCPConn
	cli          *net.TCPConn
	shutdown     *utils.Shutdown
	loopShutdown *utils.Shutdown
	network      *Network
}

func (AbaoSTR *connection) loop() {
	defer AbaoSTR.Shutdown()

	if AbaoSTR.cli == nil || AbaoSTR.svr != nil {
		panic("invalid connection")
	}
	defer AbaoSTR.loopShutdown.Complete()
	defer AbaoSTR.cli.Close()

	// 等待就绪
	m, tp, err := msg.ReadMsg(AbaoSTR.cli)
	if err != nil {
		fmt.Printf("read message error [%s]\n", err.Error())
		return
	}

	if tp != "DataActiveRequest" {
		fmt.Printf("invalid out resp type\n")
		return
	}

	req, ok := m.(*msg.DataActiveRequest)
	if !ok {
		fmt.Print("invalid OutRequest type\n")
		return
	}
	if req.Type != AbaoSTR.network.Topic {
		fmt.Printf("invalid Topic %s\n", req.Type)
		return
	}

	initMsg := &msg.Response{
		Magic:   req.Magic,
		Request: tp,
		Message: "",
	}

	// 收到请求之后，先连接服务器，确定之后再说
	svrConn, err := net.Dial(
		"tcp",
		fmt.Sprintf("%s:%d", AbaoSTR.network.BackendHost, AbaoSTR.network.BackendPort),
	)
	if err != nil {
		fmt.Println("Error connecting:", err)
		initMsg.Message = err.Error()
		msg.WriteMsg(AbaoSTR.cli, initMsg)
		return
	}
	defer svrConn.Close()
	tcpConn, _ := svrConn.(*net.TCPConn)
	AbaoSTR.svr = tcpConn

	// 回复
	msg.WriteMsg(AbaoSTR.cli, initMsg)

	// 开始数据交换
	utils.Join(AbaoSTR.cli, AbaoSTR.svr, AbaoSTR)
}

func (AbaoSTR connection) Shutdown() {
	AbaoSTR.shutdown.Begin()
}

func (AbaoSTR *connection) Run() {
	// 注册
	AbaoSTR.c.Add(AbaoSTR)
	defer AbaoSTR.c.Del(AbaoSTR)

	go AbaoSTR.loop()

	// 等待关闭
	AbaoSTR.shutdown.WaitBegin()
	fmt.Printf("start to shutdown connection %p\n", AbaoSTR)

	// 关闭socket
	AbaoSTR.cli.CloseRead()
	AbaoSTR.cli.CloseWrite()
	if AbaoSTR.svr != nil {
		AbaoSTR.svr.CloseRead()
		AbaoSTR.svr.CloseWrite()
	}

	// 等待loop结束
	AbaoSTR.loopShutdown.WaitComplete()
	// 成功结束
	AbaoSTR.shutdown.Complete()
}

///////////////////////////////////////////////////////////////////////////

type connectionMng struct {
	sessions map[*connection]int
	sync.Mutex
}

func NewControl() *connectionMng {
	return &connectionMng{
		sessions: make(map[*connection]int),
	}
}

func (AbaoSTR *connectionMng) NewConnection(cli *net.TCPConn, network *Network) {
	s := &connection{
		c:            AbaoSTR,
		cli:          cli,
		shutdown:     utils.NewShutdown(true),
		loopShutdown: utils.NewShutdown(false),
		network:      network,
	}
	go s.Run()
}

func (AbaoSTR *connectionMng) Add(s *connection) {
	fmt.Printf("add connection %p\n", s)
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	if _, ok := AbaoSTR.sessions[s]; !ok {
		AbaoSTR.sessions[s] = 0
	} else {
		panic("repeat add\n")
	}
}

func (AbaoSTR *connectionMng) Del(s *connection) {
	fmt.Printf("del connection %p\n", s)
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	if _, ok := AbaoSTR.sessions[s]; ok {
		delete(AbaoSTR.sessions, s)
	} else {
		panic("empty del\n")
	}
}

func (AbaoSTR connectionMng) Shutdown() {
	fmt.Printf("start to shutdown connectionMng\n")
	AbaoSTR.Lock()

	// 尝试关闭
	for k, _ := range AbaoSTR.sessions {
		k.Shutdown()
	}
	AbaoSTR.Unlock()
}

func (AbaoSTR connectionMng) WaitComplele() {
	for {
		time.Sleep(time.Millisecond * 100)
		AbaoSTR.Lock()

		if len(AbaoSTR.sessions) == 0 {
			AbaoSTR.Unlock()
			break
		}

		AbaoSTR.Unlock()
	}
}

/////////////////////////////////////////////////////////////////////////////
type sessionGroup struct {
	c   *connectionMng
	mng *net.TCPConn

	shutdown          *utils.Shutdown
	heartbeatShutdown *utils.Shutdown
	beatCh            chan int
	lastPing          time.Time

	abortShutdown *utils.Shutdown
	breakFlag     bool
}

func NewSessionGroup() *sessionGroup {
	return &sessionGroup{
		abortShutdown: utils.NewShutdown(true),
		breakFlag:     false,
	}
}

func (AbaoSTR sessionGroup) Shutdown() {
	AbaoSTR.abortShutdown.Begin()
}

func (AbaoSTR sessionGroup) ShutdownAndRetry() {
	AbaoSTR.shutdown.Begin()
}

func (AbaoSTR *sessionGroup) Run(network *Network) {
	defer AbaoSTR.abortShutdown.Complete()

	go func() {
		AbaoSTR.abortShutdown.WaitBegin()
		AbaoSTR.breakFlag = true
		AbaoSTR.ShutdownAndRetry()
	}()

	for !AbaoSTR.breakFlag {
		conn, err := net.Dial(
			"tcp",
			fmt.Sprintf("%s:%d", network.ServerHost, network.ServerPort),
		)
		if err != nil {
			fmt.Println("Error connecting:", err)
			time.Sleep(time.Millisecond * time.Duration(gConfig.RetryInterval))
			continue
		}

		AbaoSTR.lastPing = time.Now()
		AbaoSTR.beatCh = make(chan int)
		AbaoSTR.heartbeatShutdown = utils.NewShutdown(false)
		AbaoSTR.shutdown = utils.NewShutdown(true)
		AbaoSTR.c = NewControl()
		tcpConn, _ := conn.(*net.TCPConn)
		AbaoSTR.mng = tcpConn

		go AbaoSTR.loop(network)
		go AbaoSTR.heartbeat()
		// 监控退出
		AbaoSTR.manager()

		if AbaoSTR.breakFlag {
			break
		}
		time.Sleep(time.Millisecond * time.Duration(gConfig.RetryInterval))
		fmt.Printf("retry connect in %p\n", AbaoSTR)
	}
}

func (AbaoSTR *sessionGroup) manager() {
	AbaoSTR.shutdown.WaitBegin()
	fmt.Printf("start to shutdown sessionGroup %p\n", AbaoSTR)

	AbaoSTR.mng.CloseRead()
	AbaoSTR.mng.CloseWrite()

	// 关闭数据连接
	AbaoSTR.c.Shutdown()
	// 等待结束
	AbaoSTR.c.WaitComplele()
	// 关闭心跳 （务必在c的后面）
	close(AbaoSTR.beatCh)
	// 等待心跳结束
	AbaoSTR.heartbeatShutdown.WaitComplete()

	AbaoSTR.mng.Close()
	// 回收
	AbaoSTR.shutdown.Complete()
}

func (AbaoSTR *sessionGroup) loop(network *Network) {
	defer AbaoSTR.ShutdownAndRetry()

	// 请求注册
	uniqKey := utils.Magic()
	initMsg := &msg.OutRequest{
		Magic:   uniqKey,
		Version: utils.Version,
		Type:    network.Topic,
	}
	msg.WriteMsg(AbaoSTR.mng, initMsg)

	// 确认回包
	if err := msg.CheckResponse(AbaoSTR.mng, uniqKey, "OutRequest"); err != nil {
		fmt.Println(err.Error())
		return
	}

	// 等待数据连接请求和心跳
	for {
		// 等待消息
		d, tp, err := msg.ReadMsg(AbaoSTR.mng)
		if err != nil {
			fmt.Printf("read message error [%s]\n", err.Error())
			break
		}
		if tp == "Pong" {
			_, ok := d.(*msg.Pong)
			if !ok {
				fmt.Printf("invalid out resp type\n")
				break
			}
			AbaoSTR.beatCh <- 1
			continue
		}

		resp, ok := d.(*msg.NewDataRequest)
		if !ok {
			fmt.Printf("invalid out resp type\n")
			break
		}

		if resp.Type != network.Topic {
			fmt.Printf("invalid Topic %s\n", resp.Type)
			continue
		}
		fmt.Printf("recv NewDataRequest\n")

		go func() {
			newConn, err := net.Dial(
				"tcp",
				fmt.Sprintf("%s:%d", network.ServerHost, network.ServerPort),
			)
			if err != nil {
				fmt.Println("Error connecting:", err)
				return
			}

			uniqKey := utils.Magic()
			initMsg := &msg.OutDataRequest{
				Magic:   uniqKey,
				Type:    network.Topic,
				Version: utils.Version,
			}
			msg.WriteMsg(newConn, initMsg)

			tcpConn, _ := newConn.(*net.TCPConn)
			if err := msg.CheckResponse(tcpConn, uniqKey, "OutDataRequest"); err != nil {
				fmt.Println(err.Error())
				tcpConn.Close()
				return
			}

			AbaoSTR.c.NewConnection(tcpConn, network)
		}()
	}
}

func (AbaoSTR *sessionGroup) heartbeat() {
	defer AbaoSTR.ShutdownAndRetry()
	defer AbaoSTR.heartbeatShutdown.Complete()

	flag := false
	for {
		if flag {
			break
		}

		select {
		case _, ok := <-AbaoSTR.beatCh:
			// 收到心跳回报
			if !ok {
				flag = true
			}
			AbaoSTR.lastPing = time.Now()
			break
		case <-time.After(time.Millisecond * time.Duration(gConfig.HeartbeatInterval)):
			// 检查心跳
			if time.Since(AbaoSTR.lastPing) > time.Millisecond*time.Duration(gConfig.HeartbeatTimeout) {
				fmt.Println("Lost heartbeat")
				flag = true
			}

			// 发送心跳
			msg.WriteMsg(AbaoSTR.mng, &msg.Ping{})
			break
		}

	}
}

func checkConfig() {
	if jsonBytes, err := json.MarshalIndent(gConfig, "", "    "); err != nil {
		panic(err)
	} else {
		fmt.Println(string(jsonBytes))
	}
	if len(gConfig.Networks) < 1 || len(gConfig.Networks) > 16 {
		panic("invalid network config,network count in(1,16")
	}

	// 不同host/port 的subject可以相同
	//	topics := make(map[string]int)
	//	for _, v := range gConfig.Networks {
	//		topics[v.Topic] = 0
	//	}
	//	if len(topics) != len(gConfig.Networks) {
	//		panic("topic can not duplicate")
	//	}
}

func main() {
	confPath := flag.String("config", "", "配置文件")
	flag.Parse()
	if confPath == nil || *confPath == "" {
		gConfig = defaultConfig()
	} else {
		gConfig = parseConfig(*confPath)
	}
	checkConfig()

	fmt.Println("Starting the server ...")
	utils.RandomSeed()

	// 信号 和谐的退出
	sessions := make([]*sessionGroup, 0)
	exitMng := utils.NewExitManager()
	go exitMng.Run(func() {
		for _, v := range sessions {
			v.Shutdown()
		}
	})

	var wait sync.WaitGroup
	wait.Add(len(gConfig.Networks))
	for _, v := range gConfig.Networks {
		s := NewSessionGroup()
		network := v
		sessions = append(sessions, s)
		go func() {
			defer wait.Done()
			s.Run(network)
		}()
	}
	wait.Wait()
}
