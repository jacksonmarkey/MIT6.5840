package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const CONFIG_KEY = "cfg_key"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	s := cfg.String()
	sck.IKVClerk.Put(CONFIG_KEY, s, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	s, cver, err := sck.IKVClerk.Get(CONFIG_KEY)
	for err != rpc.OK {
		s, _, err = sck.IKVClerk.Get(CONFIG_KEY)
	}
	existingCfg := shardcfg.FromString(s)
	if new.Num <= existingCfg.Num {
		return
	}
	var wg sync.WaitGroup
	for shid, gid := range existingCfg.Shards {
		newgid := new.Shards[shid]
		if gid != newgid {
			wg.Add(1)
			go func(shid shardcfg.Tshid, oldgid tester.Tgid, newgid tester.Tgid) {
				defer wg.Done()
				// 1. Freeze
				_, oldsvrs, _ := existingCfg.GidServers(shid)
				oldshclk := shardgrp.MakeClerk(sck.clnt, oldsvrs)
				state, err := oldshclk.FreezeShard(shid, new.Num)
				if err == rpc.ErrWrongGroup {
					// Quit if the shardgrp is on a higher cfg version than we have
					// TODO : or should we continue to unfreeze?
					return
				}
				// 2. Install
				_, newsvrs, _ := new.GidServers(shid)
				newshclk := shardgrp.MakeClerk(sck.clnt, newsvrs)
				err = newshclk.InstallShard(shid, state, new.Num)
				if err == rpc.ErrWrongGroup {
					// Quit if the shardgrp is on a higher cfg version than we have
					return
				}
				// 3. Delete
				err = oldshclk.DeleteShard(shid, new.Num)
				// What do we do with this err?

			}(shardcfg.Tshid(shid), gid, newgid)
		}
	}
	wg.Wait()
	// We currently wait for all shards to be successfully moved before updating cfg.
	// This means that if one shardgrp is slow, we delay serving any other shards
	// that have completed the move. This might be necessary for atomic cfg updates,
	// but seems undesirable for reliability.
	// TODO: For part B, save intermittent progress in case controller fails
	err = sck.IKVClerk.Put(CONFIG_KEY, new.String(), cver)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	s, _, err := sck.IKVClerk.Get(CONFIG_KEY)
	for err != rpc.OK {
		s, _, err = sck.IKVClerk.Get(CONFIG_KEY)
	}
	cfg := shardcfg.FromString(s)
	return cfg
}
