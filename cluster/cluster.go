package cluster

import (
	"fmt"
	"github.com/duanhf2012/origin/log"
	"github.com/duanhf2012/origin/rpc"
	"github.com/duanhf2012/origin/service"
	"strings"
	"sync"
)

var configDir = "./config/"

type SetupServiceFun func(s ...service.IService)

type NodeInfo struct {
	NodeId int
	NodeName string
	Private bool
	ListenAddr string
	ServiceList []string       //所有的服务列表
	PublicServiceList []string //对外公开的服务列表
	DiscoveryService []string  //筛选发现的服务，如果不配置，不进行筛选
}

type NodeRpcInfo struct {
	nodeInfo NodeInfo
	client *rpc.Client
}

var cluster Cluster
type Cluster struct {
	localNodeInfo NodeInfo
	discoveryNodeList []NodeInfo

	localServiceCfg map[string]interface{} //map[serviceName]配置数据*
	mapRpc map[int] NodeRpcInfo            //nodeId
	serviceDiscovery IServiceDiscovery     //服务发现接口
	mapIdNode map[int]NodeInfo             //map[NodeId]NodeInfo
	mapServiceNode map[string]map[int]struct{}        //map[serviceName]map[NodeId]
	locker sync.RWMutex
	rpcServer rpc.Server

	rpcListerList []rpc.IRpcListener
}

func SetConfigDir(cfgDir string){
	configDir = cfgDir
}

func SetServiceDiscovery(serviceDiscovery IServiceDiscovery) {
	cluster.serviceDiscovery = serviceDiscovery
}

func (cls *Cluster) serviceDiscoveryDelNode (nodeId int){
	if nodeId == 0 {
		return
	}

	cls.locker.Lock()
	defer cls.locker.Unlock()

	cls.delNode(nodeId)
}

func (cls *Cluster) HasNode(nodeId int) bool{
	_,ok := cls.mapIdNode[nodeId]
	return ok
}

func (cls *Cluster) delNode(nodeId int){
	//删除rpc连接关系
	rpc,ok := cls.mapRpc[nodeId]
	if ok == true {
		delete(cls.mapRpc,nodeId)
		rpc.client.Close(false)
	}

	nodeInfo,ok := cls.mapIdNode[nodeId]
	if ok == false {
		return
	}

	for _,serviceName := range nodeInfo.ServiceList{
		cls.delServiceNode(serviceName,nodeId)
	}

	delete(cls.mapIdNode,nodeId)
}

func (cls *Cluster) delServiceNode(serviceName string,nodeId int){
	if nodeId == cls.localNodeInfo.NodeId{
		return
	}

	mapNode := cls.mapServiceNode[serviceName]
	delete(mapNode,nodeId)
}

func (cls *Cluster) serviceDiscoverySetNodeInfo (nodeInfo *NodeInfo){
	localMaster,_:= cls.checkDynamicDiscovery(cls.localNodeInfo.NodeId)
	//本地结点不加入
	if nodeInfo.NodeId == cls.localNodeInfo.NodeId {
		return
	}

	//如果本结点不为master结点，而且没有可使用的服务，不加入
	if localMaster == false && (len(nodeInfo.ServiceList)==0 || nodeInfo.Private == true){
		return
	}

	cls.locker.Lock()
	defer cls.locker.Unlock()

	//已经存在，则不需要进行设置
	if _,rpcInfoOK := cls.mapRpc[nodeInfo.NodeId];rpcInfoOK == true {
		return
	}

	//再重新组装
	mapDuplicate := map[string]interface{}{} //预防重复数据
	for _,serviceName := range nodeInfo.ServiceList {
		if _,ok :=  mapDuplicate[serviceName];ok == true {
			//存在重复
			log.Error("Bad duplicate Service Cfg.")
			continue
		}
		mapDuplicate[serviceName] = nil
		if _,ok:=cls.mapServiceNode[serviceName];ok==false {
			cls.mapServiceNode[serviceName] = make(map[int]struct{},1)
		}
		cls.mapServiceNode[serviceName][nodeInfo.NodeId] = struct{}{}
	}

	cls.mapIdNode[nodeInfo.NodeId] = *nodeInfo
	rpcInfo := NodeRpcInfo{}
	rpcInfo.nodeInfo = *nodeInfo
	rpcInfo.client = &rpc.Client{}
	rpcInfo.client.TriggerRpcEvent = cls.triggerRpcEvent
	rpcInfo.client.Connect(nodeInfo.NodeId,nodeInfo.ListenAddr)
	cls.mapRpc[nodeInfo.NodeId] = rpcInfo
}

func (cls *Cluster) buildLocalRpc(){
	rpcInfo := NodeRpcInfo{}
	rpcInfo.nodeInfo = cls.localNodeInfo
	rpcInfo.client = &rpc.Client{}
	rpcInfo.client.Connect(rpcInfo.nodeInfo.NodeId,"")

	cls.mapRpc[cls.localNodeInfo.NodeId] = rpcInfo
}

func (cls *Cluster) Init(localNodeId int,setupServiceFun SetupServiceFun) error{
	cls.locker.Lock()
	//2.初始化配置
	err := cls.InitCfg(localNodeId)
	if err != nil {
		cls.locker.Unlock()
		return err
	}

	cls.rpcServer.Init(cls)
	cls.buildLocalRpc()
	cls.SetupServiceDiscovery(localNodeId,setupServiceFun)
	cls.locker.Unlock()

	err = cls.serviceDiscovery.InitDiscovery(localNodeId,cls.serviceDiscoveryDelNode,cls.serviceDiscoverySetNodeInfo)
	if err != nil {
		return err
	}

	return nil
}

func (cls *Cluster) checkDynamicDiscovery(localNodeId int) (bool,bool){
	var localMaster bool  //本结点是否为Master结点
	var hasMaster bool  //是否配置Master服务

	//遍历所有结点
	for _,nodeInfo := range cls.discoveryNodeList{
		if nodeInfo.NodeId == localNodeId {
			localMaster = true
		}
		hasMaster = true
	}

	//返回查询结果
	return localMaster,hasMaster
}

func (cls *Cluster) appendService(serviceName string){
	cls.localNodeInfo.ServiceList = append(cls.localNodeInfo.ServiceList,serviceName)
	if _,ok:=cls.mapServiceNode[serviceName];ok==false {
		cls.mapServiceNode[serviceName] = map[int]struct{}{}
	}
	cls.mapServiceNode[serviceName][cls.localNodeInfo.NodeId]= struct{}{}
}

func (cls *Cluster) addDiscoveryMaster(){
	for i:=0;i<len(cls.discoveryNodeList);i++ {
		if cls.discoveryNodeList[i].NodeId == cls.localNodeInfo.NodeId {
			continue
		}

		cls.serviceDiscoverySetNodeInfo(&cls.discoveryNodeList[i])
	}
}

func (cls *Cluster) SetupServiceDiscovery(localNodeId int,setupServiceFun SetupServiceFun) {
	//1.如果没有配置DiscoveryNode配置，则使用配置文件发现服务
	localMaster,hasMaster := cls.checkDynamicDiscovery(localNodeId)
	if hasMaster == false {
		cls.serviceDiscovery = &ConfigDiscovery{}
		return
	}

	//2.添加并连接发现主结点
	cls.addDiscoveryMaster()

	//3.如果为动态服务发现安装本地发现服务
	cls.serviceDiscovery = getDynamicDiscovery()
	if localMaster == true {
		cls.appendService(DynamicDiscoveryMasterName)
	}
	cls.appendService(DynamicDiscoveryClientName)
	setupServiceFun(&masterService,&clientService)
}

func (cls *Cluster) FindRpcHandler(serviceName string) rpc.IRpcHandler {
	pService := service.GetService(serviceName)
	if pService == nil {
		return nil
	}

	return pService.GetRpcHandler()
}

func (cls *Cluster) Start() {
	cls.rpcServer.Start(cls.localNodeInfo.ListenAddr)
}

func (cls *Cluster) Stop() {
	cls.serviceDiscovery.OnNodeStop()
}

func GetCluster() *Cluster{
	return &cluster
}

func (cls *Cluster) GetRpcClient(nodeId int) *rpc.Client {
	cls.locker.RLock()
	defer cls.locker.RUnlock()
	c,ok := cls.mapRpc[nodeId]
	if ok == false {
		return nil
	}

	return c.client
}

func GetRpcClient(nodeId int,serviceMethod string,clientList []*rpc.Client) (error,int) {
	if nodeId>0 {
		pClient := GetCluster().GetRpcClient(nodeId)
		if pClient==nil {
			return fmt.Errorf("cannot find  nodeid %d!",nodeId),0
		}
		clientList[0] = pClient
		return nil,1
	}


	findIndex := strings.Index(serviceMethod,".")
	if findIndex==-1 {
		return fmt.Errorf("servicemethod param  %s is error!",serviceMethod),0
	}
	serviceName := serviceMethod[:findIndex]

	//1.找到对应的rpcNodeid
	return GetCluster().GetNodeIdByService(serviceName,clientList,true)
}

func GetRpcServer() *rpc.Server{
	return &cluster.rpcServer
}

func (cls *Cluster) IsNodeConnected (nodeId int) bool {
	pClient := cls.GetRpcClient(nodeId)
	return pClient!=nil && pClient.IsConnected()
}

func (cls *Cluster) RegisterRpcListener (rpcLister rpc.IRpcListener) {
	cls.rpcListerList = append(cls.rpcListerList,rpcLister)
}

func (cls *Cluster) triggerRpcEvent (bConnect bool,nodeId int) {
	for _,lister := range cls.rpcListerList {
		if bConnect {
			lister.OnRpcConnected(nodeId)
		}else{
			lister.OnRpcDisconnect(nodeId)
		}
	}
}

func (cls *Cluster) GetLocalNodeInfo() *NodeInfo {
	return &cls.localNodeInfo
}

