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

type session struct {
	c            *control
	svr          *net.TCPConn
	cli          *net.TCPConn
	shutdown     *utils.Shutdown
	loopShutdown *utils.Shutdown
	network      *Network
}

func (AbaoSTR *session) loop() {
	defer AbaoSTR.Shutdown()

	if AbaoSTR.cli == nil || AbaoSTR.svr != nil {
		panic("invalid session")
	}
	defer AbaoSTR.loopShutdown.Complete()
	defer AbaoSTR.cli.Close()

	// 收到请求之后，先连接服务器，确定之后再说
	svrConn, err := net.Dial(
		"tcp",
		fmt.Sprintf("%s:%d", AbaoSTR.network.ServerHost, AbaoSTR.network.ServerPort),
	)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer svrConn.Close()
	tcpConn, _ := svrConn.(*net.TCPConn)
	AbaoSTR.svr = tcpConn

	// 服务器发送请求
	uniqKey := utils.Magic()
	initMsg := &msg.InRequest{
		Magic:   uniqKey,
		Version: utils.Version,
		Type:    AbaoSTR.network.Topic,
	}
	msg.WriteMsg(tcpConn, initMsg)

	// 等待响应
	if err := msg.CheckResponse(tcpConn, uniqKey, "InRequest"); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("ok,start session data exchange %p\n", AbaoSTR)

	// 开始数据交换
	utils.Join(AbaoSTR.cli, AbaoSTR.svr, AbaoSTR)

}

func (AbaoSTR session) Shutdown() {
	AbaoSTR.shutdown.Begin()
}

func (AbaoSTR *session) Run() {
	// 成功结束
	defer AbaoSTR.shutdown.Complete()

	AbaoSTR.c.Add(AbaoSTR)
	defer AbaoSTR.c.Del(AbaoSTR)

	go AbaoSTR.loop()
	// 连接服务器
	AbaoSTR.shutdown.WaitBegin()
	fmt.Printf("start to shutdown session %p\n", AbaoSTR)

	// 关闭socket
	AbaoSTR.cli.CloseRead()
	AbaoSTR.cli.CloseWrite()
	if AbaoSTR.svr != nil {
		AbaoSTR.svr.CloseRead()
		AbaoSTR.svr.CloseWrite()
	}

	// 等待loop结束
	AbaoSTR.loopShutdown.WaitComplete()
}

///////////////////////////////////////////////////////////////////////////

type control struct {
	sessions map[*session]int
	sync.Mutex
}

// NewControl NewControl
func NewControl() *control {
	return &control{
		sessions: make(map[*session]int),
	}
}

func (AbaoSTR *control) NewSession(cli *net.TCPConn, network *Network) {
	s := &session{
		c:            AbaoSTR,
		cli:          cli,
		shutdown:     utils.NewShutdown(true),
		loopShutdown: utils.NewShutdown(false),
		network:      network,
	}
	go s.Run()
}

func (AbaoSTR *control) Add(s *session) {
	fmt.Printf("add session %p\n", s)
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	if _, ok := AbaoSTR.sessions[s]; !ok {
		AbaoSTR.sessions[s] = 0
	} else {
		panic("repeat add\n")
	}
}

func (AbaoSTR *control) Del(s *session) {
	fmt.Printf("del session %p\n", s)
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	if _, ok := AbaoSTR.sessions[s]; ok {
		delete(AbaoSTR.sessions, s)
	} else {
		panic("empty del\n")
	}
}

func (AbaoSTR control) Shutdown() {
	fmt.Println("start to shutdown control\n")
	AbaoSTR.Lock()

	// 尝试关闭
	for k, _ := range AbaoSTR.sessions {
		k.Shutdown()
	}
	AbaoSTR.Unlock()
}

// WaitComplele WaitComplele
func (AbaoSTR control) WaitComplele() {
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

// checkConfig checkConfig
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

	listenrs := make(map[net.Listener]*Network)
	c := NewControl()

	// 信号 和谐的退出
	exitMng := utils.NewExitManager()
	go exitMng.Run(func() {
		for k, _ := range listenrs {
			k.Close()
		}
		c.Shutdown()
	})

	for _, v := range gConfig.Networks {
		network := v

		// tcp服务器
		listener, err := net.Listen(
			"tcp",
			fmt.Sprintf("%s:%d", network.Bind, network.Port),
		)
		if err != nil {
			fmt.Println("Error listening", err.Error())
			return
		}
		listenrs[listener] = network
	}

	var wait sync.WaitGroup
	wait.Add(len(gConfig.Networks))
	for k, v := range listenrs {
		listener := k
		network := v
		go func() {
			defer wait.Done()
			for {
				conn, err := listener.Accept()
				if conn == nil {
					fmt.Println("listener accept ended")
					break
				}
				if err != nil {
					fmt.Println("Error accepting", err.Error())
					break
				}

				tcpConn, _ := conn.(*net.TCPConn)
				c.NewSession(tcpConn, network)
			}
		}()
	}

	// 等待结束
	wait.Wait()
	c.WaitComplele()
}
