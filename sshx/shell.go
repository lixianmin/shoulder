package sshx

import (
	"context"
	"github.com/yahoo/vssh"
	"strings"
	"time"
)

/********************************************************************
created:    2020-12-10
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

type Shell struct {
	ssh *vssh.VSSH
}

func NewShell() *Shell {
	var my = &Shell{
		ssh: vssh.New().Start(),
	}

	return my
}

func (my *Shell) AddClient(address string, user string, password string, opt ...vssh.ClientOption) error {
	var config = vssh.GetConfigUserPass(user, password)
	var err = my.ssh.AddClient(address, config, opt...)
	return err
}

func (my *Shell) Wait() {
	_, _ = my.ssh.Wait()
}

func (my *Shell) Run(cmd string, label string, timeout time.Duration) (chan *vssh.Response, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var responseChan, err = my.fetchResponseChan(ctx, cmd, label, timeout)
	return responseChan, err
}

func (my *Shell) fetchResponseChan(ctx context.Context, cmd string, label string, timeout time.Duration) (chan *vssh.Response, error) {
	label = strings.TrimSpace(label)
	if len(label) > 0 {
		var queryStmt = label + "==true"
		var responseChan, err = my.ssh.RunWithLabel(ctx, cmd, queryStmt, timeout)
		return responseChan, err
	} else {
		var responseChan = my.ssh.Run(ctx, cmd, timeout)
		return responseChan, nil
	}
}
