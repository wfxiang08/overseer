// Package overseer implements daemonizable
// self-upgrading binaries in Go (golang).
package overseer

import (
	"errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"os"
	"time"
)

const (
	envSlaveID = "OVERSEER_SLAVE_ID"
	envIsSlave = "OVERSEER_IS_SLAVE"
	envNumFDs  = "OVERSEER_NUM_FDS"
	envBinID   = "OVERSEER_BIN_ID"
	envBinPath = "OVERSEER_BIN_PATH"
)

// Config defines overseer's run-time configuration
type Config struct {
	//Required will prevent overseer from fallback to running
	//running the program in the main process on failure.
	Required bool
	//Program's main function
	Program func(state State)

	//Program's zero-downtime socket listening address (set this or Addresses)
	Address string
	//Program's zero-downtime socket listening addresses (set this or Address)
	Addresses []string

	//RestartSignal will manually trigger a graceful restart. Defaults to SIGUSR2.
	RestartSignal os.Signal
	//TerminateTimeout controls how long overseer should
	//wait for the program to terminate itself. After this
	//timeout, overseer will issue a SIGKILL.
	TerminateTimeout time.Duration

	//Debug enables all [overseer] logs.
	Debug bool
	//NoWarn disables warning [overseer] logs.
	NoWarn bool
	//NoRestart disables all restarts, this option essentially converts
	//the RestartSignal into a "ShutdownSignal".
	NoRestart bool // 默认为false, 表示会重启
}

func validate(c *Config) error {
	//validate
	if c.Program == nil {
		return errors.New("overseer.Config.Program required")
	}
	if c.Address != "" {
		if len(c.Addresses) > 0 {
			return errors.New("overseer.Config.Address and Addresses cant both be set")
		}
		c.Addresses = []string{c.Address}
	} else if len(c.Addresses) > 0 {
		c.Address = c.Addresses[0]
	}

	// 设置重启信号: kill -USR2 pid
	if c.RestartSignal == nil {
		c.RestartSignal = SIGUSR2
	}

	// 默认结束时最多等待30s
	if c.TerminateTimeout <= 0 {
		c.TerminateTimeout = 30 * time.Second
	}
	return nil
}

//Run executes overseer, if an error is
//encountered, overseer fallsback to running
//the program directly (unless Required is set).
func Run(c Config) {
	// 根据给定的配置来运行
	err := runErr(&c)
	if err != nil {
		log.ErrorErrorf(err, "RunErr faild")

		// 如果不是Requried, 那么可以直接在Master进程中运行
		if c.Required {
			log.Panicf("[overseer] %s", err)
		} else if c.Debug || !c.NoWarn {
			log.Printf("[overseer] disabled. run failed: %s", err)
		}
		c.Program(DisabledState)
		return
	}
	os.Exit(0)
}

func runErr(c *Config) error {

	if err := validate(c); err != nil {
		return err
	}
	// run either in master or slave mode
	// slave mode由 master来触发
	// 正常情况下，我们会以master的方式启动；然后master再启动slave
	if os.Getenv(envIsSlave) == "1" {
		slaveProcess := &slave{Config: c}
		return slaveProcess.run()
	} else {
		masterProcess := &master{Config: c}
		return masterProcess.run()
	}
}
