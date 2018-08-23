package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/lorock/proxy/msg"
	"github.com/lorock/proxy/utils"
)

var (
	gConfig *Config = nil
)

type tunnel struct {
	r   *ControlRegistry
	m   *msg.OutRequest // 控制连接的请求包
	mng *net.TCPConn    // 控制连接

	proxyLock sync.Mutex
	proxies   map[*proxy]int // 已经正在工作的转发proxy

	frees chan *net.TCPConn  // 空闲数据链接
	out   chan (msg.Message) // 接收控制指令的ch

	shutdown     *utils.Shutdown // 关于tunnel自己的控制器
	loopShutdown *utils.Shutdown // 关于loop go routine的控制器
	readShutdown *utils.Shutdown // 关于read go routine的控制器
}

func (AbaoSTR tunnel) Type() string {
	return AbaoSTR.m.Type
}

func (AbaoSTR tunnel) RequestNewTunnel() {
	AbaoSTR.out <- &msg.NewDataRequest{
		Magic: utils.Magic(),
		Type:  AbaoSTR.m.Type,
	}
}

func (AbaoSTR *tunnel) GetFreeTunnel() (conn *net.TCPConn, err error) {
	var ok bool
	select {
	case conn, ok = <-AbaoSTR.frees:
		if !ok {
			err = fmt.Errorf("No proxy connections available, control is closing")
			return
		}
	default:
		fmt.Printf("No free tunnel in pool, requesting tunnel from control\n")
		AbaoSTR.RequestNewTunnel()

		select {
		case conn, ok = <-AbaoSTR.frees:
			if !ok {
				err = fmt.Errorf("No proxy connections available, control is closing")
				return
			}
		case <-time.After(time.Duration(utils.FreeTunnelTimeout) * time.Millisecond):
			err = fmt.Errorf("Timeout trying to get proxy connection")
			return
		}
	}
	fmt.Printf("get a empty data conn %p from [%s]\n", conn, AbaoSTR.m.Type)
	// 被弄走一个，提前再申请一个
	AbaoSTR.RequestNewTunnel()
	return
}

func (AbaoSTR *tunnel) RegisterDataConn(conn *net.TCPConn) error {
	select {
	case AbaoSTR.frees <- conn:
		fmt.Printf("registered data conn %p to [%s]\n", conn, AbaoSTR.m.Type)
		return nil
	default:
		fmt.Printf("frees buffer is full, discarding.")
		return fmt.Errorf("frees buffer is full, discarding.")
	}
}

func (AbaoSTR *tunnel) Add(s *proxy) {
	fmt.Printf("add proxy to tunnel %p\n", s)
	AbaoSTR.proxyLock.Lock()
	defer AbaoSTR.proxyLock.Unlock()

	if _, ok := AbaoSTR.proxies[s]; !ok {
		AbaoSTR.proxies[s] = 0
	} else {
		panic("repeat add to tunnel")
	}
}

func (AbaoSTR *tunnel) Del(s *proxy) {
	fmt.Printf("del proxy from tunnel %p\n", s)
	AbaoSTR.proxyLock.Lock()
	defer AbaoSTR.proxyLock.Unlock()

	if _, ok := AbaoSTR.proxies[s]; ok {
		delete(AbaoSTR.proxies, s)
	} else {
		panic("empty del from tunnel")
	}
}

func (AbaoSTR *tunnel) loop() {
	if AbaoSTR.mng == nil {
		panic("invalid tunnel")
	}
	defer AbaoSTR.loopShutdown.Complete()
	defer AbaoSTR.Shutdown()

	// write messages to the control channel
	for m := range AbaoSTR.out {
		if err := msg.WriteMsg(AbaoSTR.mng, m); err != nil {
			fmt.Println("write error ", err)
			break
		}
	}
}

func (AbaoSTR tunnel) read() {
	if AbaoSTR.mng == nil {
		panic("invalid tunnel")
	}
	defer AbaoSTR.readShutdown.Complete()
	defer AbaoSTR.Shutdown()

	for {
		_, _, err := msg.ReadMsg(AbaoSTR.mng)

		d, tp, err := msg.ReadMsg(AbaoSTR.mng)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("tunnel %p read end\n", &AbaoSTR)
			} else {
				fmt.Println("Error to read message because of ", err)
			}
			break
		}
		if tp == "Ping" {
			// 自动回个心跳
			_, ok := d.(*msg.Ping)
			if !ok {
				fmt.Printf("invalid out resp type\n")
				break
			}
			msg.WriteMsg(AbaoSTR.mng, &msg.Pong{})
		}
	}
}

func (AbaoSTR tunnel) Shutdown() {
	AbaoSTR.shutdown.Begin()
}

func (AbaoSTR *tunnel) Run() {

	AbaoSTR.r.Add(AbaoSTR)
	defer AbaoSTR.r.Del(AbaoSTR)

	go AbaoSTR.loop()
	go AbaoSTR.read()

	// 事先请求一个data通道
	AbaoSTR.RequestNewTunnel()

	// 等待结束指令
	AbaoSTR.shutdown.WaitBegin()
	fmt.Printf("start to shutdown tunnel %p\n", AbaoSTR)

	// 关闭接收控制指令的ch
	close(AbaoSTR.out)

	// 关闭socket
	AbaoSTR.mng.CloseRead()
	AbaoSTR.mng.CloseWrite()
	AbaoSTR.mng.Close()

	// 关闭空闲的连接
	close(AbaoSTR.frees)
	for p := range AbaoSTR.frees {
		fmt.Printf("close free connection %p from tunnel %p\n", p, AbaoSTR)
		p.Close()
	}

	// 关闭关联的proxy
	AbaoSTR.proxyLock.Lock()
	for t, _ := range AbaoSTR.proxies {
		fmt.Printf("start to shutdown proxy %p from tunnel %p\n", t, AbaoSTR)
		t.Shutdown()
	}
	AbaoSTR.proxyLock.Unlock()

	// 等待go routine结束
	AbaoSTR.loopShutdown.WaitComplete()
	AbaoSTR.readShutdown.WaitComplete()
	AbaoSTR.shutdown.Complete()
}

////////////////////////////////////////////////////////////////////////////

type ControlRegistry struct {
	tunnels map[string]*tunnel
	sync.RWMutex
}

func NewControlRegistry() *ControlRegistry {
	return &ControlRegistry{
		tunnels: make(map[string]*tunnel),
	}
}

func (AbaoSTR *ControlRegistry) NewTunnel(mng *net.TCPConn, m *msg.OutRequest) {
	t := &tunnel{
		r:            AbaoSTR,
		mng:          mng,
		m:            m,
		proxies:      make(map[*proxy]int),
		frees:        make(chan *net.TCPConn, utils.TunnelBufLen),
		shutdown:     utils.NewShutdown(true),
		loopShutdown: utils.NewShutdown(false),
		readShutdown: utils.NewShutdown(false),
		out:          make(chan msg.Message),
	}
	go t.Run()
}

func (AbaoSTR *ControlRegistry) Add(s *tunnel) {
	fmt.Printf("add tunnel %p named [%s]\n", s, s.m.Type)
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	tp := s.Type()
	if t, ok := AbaoSTR.tunnels[tp]; ok {
		// 关闭旧的
		t.Shutdown()
	}
	AbaoSTR.tunnels[tp] = s
}

func (AbaoSTR *ControlRegistry) Del(s *tunnel) {
	fmt.Printf("del tunnel %p\n", s)
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	tp := s.Type()
	if v, ok := AbaoSTR.tunnels[tp]; ok {
		if v == s {
			fmt.Printf("delete tunnel %p from ControlRegistry\n", s)
			delete(AbaoSTR.tunnels, tp)
		} else {
			fmt.Printf("delete different tunnel todo(%p|%p)current ControlRegistry\n", s, v)
		}
	} else {
		panic("empty del\n")
	}
}

func (AbaoSTR *ControlRegistry) Get(tp string) *tunnel {
	AbaoSTR.RLock()
	defer AbaoSTR.RUnlock()
	return AbaoSTR.tunnels[tp]
}

func (AbaoSTR ControlRegistry) Shutdown() {
	fmt.Println("start to shutdown ControlRegistry")
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	// 尝试关闭
	for _, v := range AbaoSTR.tunnels {
		v.Shutdown()
	}

}

func (AbaoSTR ControlRegistry) WaitComplele() {
	for {
		time.Sleep(time.Millisecond * 100)
		AbaoSTR.Lock()

		if len(AbaoSTR.tunnels) == 0 {
			AbaoSTR.Unlock()
			break
		}

		AbaoSTR.Unlock()
	}
}

////////////////////////////////////////////////////////////////////////////

type proxy struct {
	r            *ProxyRegistry
	m            *msg.InRequest
	cli          *net.TCPConn
	svr          *net.TCPConn
	shutdown     *utils.Shutdown
	loopShutdown *utils.Shutdown
}

func (AbaoSTR *proxy) loop(c *ControlRegistry) {
	if AbaoSTR.cli == nil || AbaoSTR.svr != nil {
		panic("invalid proxy")
	}
	defer AbaoSTR.loopShutdown.Complete()
	defer AbaoSTR.cli.Close()
	defer AbaoSTR.Shutdown()

	// 回复报文
	initMsg := &msg.Response{
		Magic:   AbaoSTR.m.Magic,
		Request: "InRequest",
		Message: "",
	}

	if AbaoSTR.m.Version != utils.Version {
		initMsg.Message = fmt.Sprintf("invalid version [%s|%s]", AbaoSTR.m.Version, utils.Version)
		msg.WriteMsg(AbaoSTR.cli, initMsg)
		return
	}

	// 找到mng tunnel
	t := c.Get(AbaoSTR.m.Type)
	if t == nil {
		initMsg.Message = fmt.Sprintf("can not find tunnel [%s]", AbaoSTR.m.Type)
		msg.WriteMsg(AbaoSTR.cli, initMsg)
		return
	}
	t.Add(AbaoSTR)
	defer t.Del(AbaoSTR)

	// 获得空闲的连接
	newConn, err := t.GetFreeTunnel()
	if err != nil || newConn == nil {
		fmt.Println(err.Error())
		initMsg.Message = err.Error()
		msg.WriteMsg(AbaoSTR.cli, initMsg)
		return
	}
	AbaoSTR.svr = newConn
	defer AbaoSTR.svr.Close()

	// 上游服务器发送请求，用于激活data传输
	uniqKey := utils.Magic()
	msg.WriteMsg(newConn, &msg.DataActiveRequest{
		Magic: uniqKey,
		Type:  AbaoSTR.m.Type,
	})

	// 等待响应
	if err := msg.CheckResponse(AbaoSTR.svr, uniqKey, "DataActiveRequest"); err != nil {
		fmt.Println(err.Error())
		initMsg.Message = err.Error()
		msg.WriteMsg(AbaoSTR.cli, initMsg)
		return
	}

	msg.WriteMsg(AbaoSTR.cli, initMsg)
	fmt.Printf("ok,active proxy data exchange %p\n", AbaoSTR)

	// 开始数据交换
	utils.Join(AbaoSTR.cli, AbaoSTR.svr, AbaoSTR)
}

func (AbaoSTR proxy) Shutdown() {
	AbaoSTR.shutdown.Begin()
}

func (AbaoSTR *proxy) Run(c *ControlRegistry) {
	defer AbaoSTR.shutdown.Complete()

	AbaoSTR.r.Add(AbaoSTR)
	defer AbaoSTR.r.Del(AbaoSTR)

	go AbaoSTR.loop(c)
	// 连接服务器
	AbaoSTR.shutdown.WaitBegin()
	fmt.Printf("start to shutdown proxy %p\n", AbaoSTR)

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

////////////////////////////////////////////////////////////////////////////

type ProxyRegistry struct {
	proxies map[*proxy]int
	sync.Mutex
}

func NewProxyRegistry() *ProxyRegistry {
	return &ProxyRegistry{
		proxies: make(map[*proxy]int),
	}
}

func (AbaoSTR *ProxyRegistry) NewProxy(
	cli *net.TCPConn,
	m *msg.InRequest,
	c *ControlRegistry) {
	p := &proxy{
		r:            AbaoSTR,
		cli:          cli,
		m:            m,
		shutdown:     utils.NewShutdown(true),
		loopShutdown: utils.NewShutdown(false),
	}
	go p.Run(c)
}

func (AbaoSTR *ProxyRegistry) Add(s *proxy) {
	fmt.Printf("add proxy %p\n", s)
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	if _, ok := AbaoSTR.proxies[s]; !ok {
		AbaoSTR.proxies[s] = 0
	} else {
		panic("repeat add\n")
	}
}

func (AbaoSTR *ProxyRegistry) Del(s *proxy) {
	fmt.Printf("del proxy %p\n", s)
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	if _, ok := AbaoSTR.proxies[s]; ok {
		delete(AbaoSTR.proxies, s)
	} else {
		panic("empty del\n")
	}
}

func (AbaoSTR ProxyRegistry) Shutdown() {
	fmt.Println("start to shutdown ProxyRegistry")
	AbaoSTR.Lock()
	defer AbaoSTR.Unlock()

	// 尝试关闭
	for k, _ := range AbaoSTR.proxies {
		k.Shutdown()
	}

}

func (AbaoSTR ProxyRegistry) WaitComplele() {
	for {
		time.Sleep(time.Millisecond * 100)
		AbaoSTR.Lock()

		if len(AbaoSTR.proxies) == 0 {
			AbaoSTR.Unlock()
			break
		}

		AbaoSTR.Unlock()
	}
}

////////////////////////////////////////////////////////////////////////////
func handle(conn *net.TCPConn, p *ProxyRegistry, c *ControlRegistry) {
	m, tp, err := msg.ReadMsg(conn)
	if err != nil {
		fmt.Printf("read message error [%s]\n", err.Error())
		conn.Close()
		return
	}

	fmt.Printf("new request type [%s]\n", tp)
	if tp == "OutRequest" {
		req, ok := m.(*msg.OutRequest)
		if !ok {
			fmt.Print("invalid OutRequest type\n")
			return
		}

		initMsg := &msg.Response{
			Magic:   req.Magic,
			Request: tp,
			Message: "",
		}

		if req.Version != utils.Version {
			initMsg.Message = fmt.Sprintf("invalid version [%s|%s]", req.Version, utils.Version)
			msg.WriteMsg(conn, initMsg)
			conn.Close()
			return
		}

		// 校验合法性
		if req.Type == "" {
			initMsg.Message = "invalid type"
			msg.WriteMsg(conn, initMsg)
			conn.Close()
			return
		}

		msg.WriteMsg(conn, initMsg)

		c.NewTunnel(conn, req)
	} else if tp == "OutDataRequest" {
		req, ok := m.(*msg.OutDataRequest)
		if !ok {
			fmt.Print("invalid OutDataRequest type\n")
			return
		}

		initMsg := &msg.Response{
			Magic:   req.Magic,
			Request: tp,
			Message: "",
		}

		if req.Version != utils.Version {
			initMsg.Message = fmt.Sprintf("invalid version [%s|%s]", req.Version, utils.Version)
			msg.WriteMsg(conn, initMsg)
			conn.Close()
			return
		}

		// 找到mng tunnel
		t := c.Get(req.Type)
		if t == nil {
			initMsg.Message = fmt.Sprintf("can not find tunnel %s", req.Type)
			msg.WriteMsg(conn, initMsg)
			conn.Close()
			return
		}

		// 注册空闲的数据tunnel
		if err := t.RegisterDataConn(conn); err != nil {
			initMsg.Message = err.Error()
			msg.WriteMsg(conn, initMsg)
			conn.Close()
			return
		}

		msg.WriteMsg(conn, initMsg)
	} else if tp == "InRequest" {
		req, ok := m.(*msg.InRequest)
		if !ok {
			fmt.Print("invalid InRequest type\n")
			return
		}

		p.NewProxy(conn, req, c)
	} else {
		fmt.Printf("invalid tp %s\n", tp)
		msg.WriteMsg(conn, &msg.Response{
			Magic:   "invalid tp",
			Request: "invalid tp",
			Message: fmt.Sprintf("invalid type %s", tp),
		})
		conn.Close()

	}
}

func checkConfig() {
	if jsonBytes, err := json.MarshalIndent(gConfig, "", "    "); err != nil {
		panic(err)
	} else {
		fmt.Println(string(jsonBytes))
	}
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
	// tcp服务器
	listener, err := net.Listen(
		"tcp",
		fmt.Sprintf("%s:%d", gConfig.Bind, gConfig.Port),
	)
	if err != nil {
		fmt.Println("Error listening", err.Error())
		return
	}

	p := NewProxyRegistry()
	c := NewControlRegistry()
	// 信号 和谐的退出
	exitMng := utils.NewExitManager()
	go exitMng.Run(func() {
		listener.Close()
		p.Shutdown()
		c.Shutdown()
	})

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
		go handle(tcpConn, p, c)
	}

	// 等待结束
	p.WaitComplele()
	c.WaitComplele()
}
