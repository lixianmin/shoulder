package zkx

import (
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"strconv"
	"strings"
)

/********************************************************************
created:    2021-03-08
author:     lixianmin

Copyright (C) - All Rights Reserved
*********************************************************************/

var (
	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = errors.New("zk: trying to acquire a lock twice")
	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first be acquired.
	ErrNotLocked = errors.New("zk: not locked")
)

// Lock is a mutual exclusion lock.
type Lock struct {
	conn     *zk.Conn
	path     string
	acl      []zk.ACL
	lockPath string
	seq      int
}

// NewLock creates a new lock instance using the provided connection, path, and acl.
// The path must be a node that is only used by this lock. A lock instances starts
// unlocked until Lock() is called.
func NewLock(c *zk.Conn, path string, acl []zk.ACL) *Lock {
	return &Lock{
		conn: c,
		path: path,
		acl:  acl,
	}
}

func parseSequence(path string) (int, error) {
	parts := strings.Split(path, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

// Lock attempts to acquire the lock. It will wait to return until the lock
// is acquired or an error occurs. If this instance already has the lock
// then ErrDeadlock is returned.
func (my *Lock) Lock() error {
	if my.lockPath != "" {
		return ErrDeadlock
	}

	prefix := fmt.Sprintf("%s/lock-", my.path)

	path := ""
	var err error
	for i := 0; i < 3; i++ {
		path, err = my.conn.CreateProtectedEphemeralSequential(prefix, []byte{}, my.acl)
		if err == zk.ErrNoNode {
			// Create parent node.
			parts := strings.Split(my.path, "/")
			pth := ""
			for _, p := range parts[1:] {
				var exists bool
				pth += "/" + p
				exists, _, err = my.conn.Exists(pth)
				if err != nil {
					return err
				}
				if exists == true {
					continue
				}
				_, err = my.conn.Create(pth, []byte{}, 0, my.acl)
				if err != nil && err != zk.ErrNodeExists {
					return err
				}
			}
		} else if err == nil {
			break
		} else {
			return err
		}
	}
	if err != nil {
		return err
	}

	sequence, err := parseSequence(path)
	if err != nil {
		return err
	}

	for {
		children, _, err := my.conn.Children(my.path)
		if err != nil {
			return err
		}

		lowestSequence := sequence
		prevSequence := -1
		prevSeqPath := ""

		for _, p := range children {
			s, err := parseSequence(p)
			if err != nil {
				return err
			}
			if s < lowestSequence {
				lowestSequence = s
			}

			if s < sequence && s > prevSequence {
				prevSequence = s
				prevSeqPath = p
			}
		}

		// 直到拿到最小序号，表明这把锁属于自己了
		if sequence == lowestSequence {
			// Acquired the lock
			break
		}

		// Wait on the node next in line for the lock
		_, _, ch, err := my.conn.GetW(my.path + "/" + prevSeqPath)
		if err != nil && err != zk.ErrNoNode {
			return err
		} else if err != nil && err == zk.ErrNoNode {
			// try again
			continue
		}

		ev := <-ch
		if ev.Err != nil {
			return ev.Err
		}
	}

	my.seq = sequence
	my.lockPath = path

	return nil
}

// Unlock releases an acquired lock. If the lock is not currently acquired by
// this Lock instance than ErrNotLocked is returned.
func (my *Lock) Unlock() error {
	if my.lockPath == "" {
		return ErrNotLocked
	}

	if err := my.conn.Delete(my.lockPath, -1); err != nil {
		return err
	}

	my.lockPath = ""
	my.seq = 0
	return nil
}

func (my *Lock) GetConn() *zk.Conn {
	return my.conn
}

// lockPath只有在Lock()成功后才能拿到
func (my *Lock) GetLockPath() string {
	return my.lockPath
}

// path是NewLock()的时候传入的
func (my *Lock) GetPath() string {
	return my.path
}
