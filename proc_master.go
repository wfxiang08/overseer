package overseer

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/kardianos/osext"
	"strings"
)

var tmpBinPath = filepath.Join(os.TempDir(), "overseer-" + token())

//a overseer master process
type master struct {
	*Config
	slaveID             int
	slaveCmd            *exec.Cmd // 如何控制子进程? Cmd
	slaveExtraFiles     []*os.File
	binPath, tmpBinPath string
	binPerms            os.FileMode
	binHash             []byte
	restartMux          sync.Mutex
	restarting          bool
	restartedAt         time.Time
	restarted           chan bool
	awaitingUSR1        bool
	descriptorsReleased chan bool
	signalledAt         time.Time
	printCheckUpdate    bool
}

func (mp *master) run() error {
	mp.debugf("run")
	// 检查bin，计算md5
	if err := mp.checkBinary(); err != nil {
		return err
	}

	mp.setupSignalling()
	if err := mp.retreiveFileDescriptors(); err != nil {
		return err
	}

	return mp.forkLoop()
}

func (mp *master) checkBinary() error {
	//get path to binary and confirm its writable
	binPath, err := osext.Executable()
	if err != nil {
		return fmt.Errorf("failed to find binary path (%s)", err)
	}
	mp.binPath = binPath
	if info, err := os.Stat(binPath); err != nil {
		return fmt.Errorf("failed to stat binary (%s)", err)
	} else if info.Size() == 0 {
		return fmt.Errorf("binary file is empty")
	} else {
		//copy permissions
		mp.binPerms = info.Mode()
	}
	f, err := os.Open(binPath)
	if err != nil {
		return fmt.Errorf("cannot read binary (%s)", err)
	}
	//initial hash of file
	hash := sha1.New()
	io.Copy(hash, f)
	mp.binHash = hash.Sum(nil)
	f.Close()

	return nil
}

//
// 设置信号处理
//
func (mp *master) setupSignalling() {
	//updater-forker comms
	mp.restarted = make(chan bool)
	mp.descriptorsReleased = make(chan bool)
	//read all master process signals
	signals := make(chan os.Signal)
	signal.Notify(signals)
	go func() {
		for s := range signals {
			mp.handleSignal(s)
		}
	}()
}

func (mp *master) handleSignal(s os.Signal) {

	// 如何重启呢?
	if s == mp.RestartSignal {
		//user initiated manual restart
		mp.triggerRestart()
	} else if s.String() == "child exited" {
		// will occur on every restart, ignore it

	} else if mp.awaitingUSR1 && s == SIGUSR1 {
		//**during a restart** a SIGUSR1 signals
		//to the master process that, the file
		//descriptors have been released

		mp.debugf("signaled, sockets ready")
		mp.awaitingUSR1 = false
		mp.descriptorsReleased <- true
	} else if mp.slaveCmd != nil && mp.slaveCmd.Process != nil {
		//while the slave process is running, proxy
		//all signals through
		mp.debugf("proxy signal (%s)", s)
		// 如何转发Signal?
		mp.sendSignal(s)
	} else if s == os.Interrupt {
		//otherwise if not running, kill on CTRL+c
		mp.debugf("interupt with no slave")
		os.Exit(1)
	} else {
		mp.debugf("signal discarded (%s), no slave process", s)
	}
}

func (mp *master) sendSignal(s os.Signal) {
	if mp.slaveCmd != nil && mp.slaveCmd.Process != nil {
		if err := mp.slaveCmd.Process.Signal(s); err != nil {
			mp.debugf("signal failed (%s), assuming slave process died unexpectedly", err)
			os.Exit(1)
		}
	}
}

// 在Listener中创建listeners
func (mp *master) retreiveFileDescriptors() error {
	mp.slaveExtraFiles = make([]*os.File, len(mp.Config.Addresses))
	for i, addr := range mp.Config.Addresses {
		// 两种格式：
		// l, err := net.Listen(p.addr.Network(), p.addr.String())
		if strings.Contains(addr, ":") {
			// 如果是tcp的socket
			a, err := net.ResolveTCPAddr("tcp", addr)
			if err != nil {
				return fmt.Errorf("Invalid address %s (%s)", addr, err)
			}
			l, err := net.ListenTCP("tcp", a)
			if err != nil {
				return err
			}
			f, err := l.File()
			if err != nil {
				return fmt.Errorf("Failed to retreive fd for: %s (%s)", addr, err)
			}
			if err := l.Close(); err != nil {
				return fmt.Errorf("Failed to close listener for: %s (%s)", addr, err)
			}
			mp.slaveExtraFiles[i] = f
		} else {
			addr, err := net.ResolveUnixAddr("unix", addr)
			if err != nil {
				return fmt.Errorf("Invalid address %s (%s)", addr, err)
			}
			l, err := net.ListenUnix(addr.Network(), addr)
			if err != nil {
				return err
			}

			f, err := l.File()
			if err != nil {
				return fmt.Errorf("Failed to retreive fd for: %s (%s)", addr, err)
			}
			if err := l.Close(); err != nil {
				return fmt.Errorf("Failed to close listener for: %s (%s)", addr, err)
			}
			mp.slaveExtraFiles[i] = f
			// 注意: 该Socket需要给所有需要访问该接口的人以读写的权限
			// 因此最终的 sock文件的权限为: 0777
			// 例如: aa.sock root/root 07777
			//      换一个用户，rm aa.sock 似乎无效
			filePath := addr.String()
			os.Chmod(filePath, os.ModePerm)

		}

	}
	return nil
}

func (mp *master) triggerRestart() {
	// 正在重启中?
	if mp.restarting {
		mp.debugf("already graceful restarting")
		return //skip

	} else if mp.slaveCmd == nil || mp.restarting {
		mp.debugf("no slave process")
		return //skip
	}
	mp.debugf("graceful restart triggered")

	mp.restarting = true
	mp.awaitingUSR1 = true
	mp.signalledAt = time.Now()
	mp.sendSignal(mp.Config.RestartSignal) //ask nicely to terminate

	select {
	case <-mp.restarted:
	//success
		mp.debugf("restart success")
	case <-time.After(mp.TerminateTimeout):
	//times up mr. process, we did ask nicely!
		mp.debugf("graceful timeout, forcing exit")
		mp.sendSignal(os.Kill)
	}
}

//not a real fork
func (mp *master) forkLoop() error {
	//loop, restart command
	for {
		// 挂了就继续?
		if err := mp.fork(); err != nil {
			return err
		}
	}
}

func (mp *master) fork() error {
	mp.debugf("starting %s", mp.binPath)

	cmd := exec.Command(mp.binPath)
	//mark this new process as the "active" slave process.
	//this process is assumed to be holding the socket files.
	mp.slaveCmd = cmd
	mp.slaveID++

	//provide the slave process with some state
	e := os.Environ()
	e = append(e, envBinID + "=" + hex.EncodeToString(mp.binHash))
	e = append(e, envBinPath + "=" + mp.binPath)
	e = append(e, envSlaveID + "=" + strconv.Itoa(mp.slaveID))
	e = append(e, envIsSlave + "=1") // 启动一个SLAVE, 其实Master似乎也没有做什么事情?

	// 监听几个Listern, 那么就传递几个socket
	e = append(e, envNumFDs + "=" + strconv.Itoa(len(mp.slaveExtraFiles)))
	cmd.Env = e

	//inherit master args/stdfiles
	cmd.Args = os.Args
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	//include socket files
	cmd.ExtraFiles = mp.slaveExtraFiles
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Failed to start slave process: %s", err)
	}
	//was scheduled to restart, notify success
	if mp.restarting {
		mp.restartedAt = time.Now()
		mp.restarting = false
		mp.restarted <- true
	}

	// 等待子进程结束
	//convert wait into channel
	cmdwait := make(chan error)
	go func() {
		cmdwait <- cmd.Wait()
	}()
	//wait....
	select {
	case err := <-cmdwait:
	//program exited before releasing descriptors
	//proxy exit code out to master
		code := 0
		if err != nil {
			code = 1
			if exiterr, ok := err.(*exec.ExitError); ok {
				if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
					code = status.ExitStatus()
				}
			}
		}
		mp.debugf("prog exited with %d", code)
	//if a restarts are disabled or if it was an
	//unexpected crash, proxy this exit straight
	//through to the main process
		if mp.NoRestart || !mp.restarting {
			os.Exit(code)
		}
	case <-mp.descriptorsReleased:
	// 如果子进程放弃了fd, 那么可以立即Fork一个新的进程?
	//if descriptors are released, the program
	//has yielded control of its sockets and
	//a parallel instance of the program can be
	//started safely. it should serve state.Listeners
	//to ensure downtime is kept at <1sec. The previous
	//cmd.Wait() will still be consumed though the
	//result will be discarded.
	}
	return nil
}

func (mp *master) debugf(f string, args ...interface{}) {
	if mp.Config.Debug {
		log.Printf("[overseer master] " + f, args...)
	}
}

func (mp *master) warnf(f string, args ...interface{}) {
	if mp.Config.Debug || !mp.Config.NoWarn {
		log.Printf("[overseer master] " + f, args...)
	}
}

func token() string {
	buff := make([]byte, 8)
	rand.Read(buff)
	return hex.EncodeToString(buff)
}
