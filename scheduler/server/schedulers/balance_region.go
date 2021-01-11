// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeInfo []*core.StoreInfo

// sort (quickSort) stores recording to their region size
func (stores storeInfo) sort(left ,right int){
	if left<right{
		mid:=stores.partition(left,right)
		stores.sort(left,mid-1)
		stores.sort(mid+1,right)
	}
}

func (stores storeInfo) partition(left, right int) int{
	temp:=stores[right]
	i:=left
	for j:=left;j<=right-1;j++{
		if stores[j].GetRegionSize()<=temp.GetRegionSize(){
			stores[i],stores[j]=stores[j],stores[i]
			i++
		}
	}
	stores[i],stores[right]=stores[right],stores[i]
	return i
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores:=make(storeInfo,0)
	for _,store:=range cluster.GetStores(){
		// check the suitable stores in cluster
		if store.IsUp()&&store.DownTime()<cluster.GetMaxStoreDownTime(){
			stores=append(stores,store)
		}
	}
	num:=len(stores)
	if num<=1{
		return nil
	}
	stores.sort(0,num-1)
	var region *core.RegionInfo
	var i int
	for i=num-1;i>=0;i--{
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(container core.RegionsContainer) {
			regions=container
		})
		region=regions.RandomRegion(nil,nil)
		if region!=nil{
			break
		}
		cluster.GetFollowersWithLock(stores[i].GetID(), func(container core.RegionsContainer) {
			regions=container
		})
		region=regions.RandomRegion(nil,nil)
		if region!=nil{
			break
		}
		cluster.GetLeadersWithLock(stores[i].GetID(), func(container core.RegionsContainer) {
			regions=container
		})
		region=regions.RandomRegion(nil,nil)
		if region!=nil{
			break
		}
	}
	if region==nil{
		return nil
	}
	originStore:=stores[i]
	storeIDS:=region.GetStoreIds()
	if len(storeIDS)<cluster.GetMaxReplicas(){
		return nil
	}
	var targetStore *core.StoreInfo
 	for j:=0;j<i;j++{
		if _,find:=storeIDS[stores[j].GetID()];!find{
			targetStore=stores[j]
			break
		}
	}
	if targetStore == nil {
		return nil
	}
	if originStore.GetRegionSize()-targetStore.GetRegionSize()<2*region.GetApproximateSize(){
		return nil
	}
	peer,err:=cluster.AllocPeer(targetStore.GetID())
	if err!=nil {
		return nil
	}
	desc:=fmt.Sprintf("origin store:%d,target store:%d",originStore.GetID(),targetStore.GetID())
	op,err:=operator.CreateMovePeerOperator(desc,cluster,region,operator.OpBalance,originStore.GetID(),targetStore.GetID(),peer.GetId())
	if err!=nil{
		return nil
	}
	return op
}
