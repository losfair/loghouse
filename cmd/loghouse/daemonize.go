package main

import (
	"os"
	"os/exec"
	"syscall"
)

func daemonize() error {
	self, err := os.Executable()
	if err != nil {
		return err
	}

	cmd := exec.Command(self, os.Args[1:]...)
	cmd.Env = append(os.Environ(), "LOGHOUSE_DAEMONIZED=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer devNull.Close()

	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull

	return cmd.Start()
}
