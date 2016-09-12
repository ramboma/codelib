# coding=utf-8
# 该脚本会间隔7天执行，执行时间比SaveGrabInfo.py晚一天，为第二天的9点，处理把前一天备份的抢派单记录
import configparser;
import redis;
import pymssql;
import time;
import datetime;
import json;
import urllib.parse;
import urllib.request;
from xml.dom.minidom import parse,parseString;
import logging;
import os;
#是否测试
istest=0;
#相对路径
scriptDictory=os.path.dirname(os.path.realpath(__file__));
#初始化日志  
logfilename=scriptDictory+'/logs/'+datetime.datetime.now().strftime("%Y%m%d")+'logs.log'
logging.basicConfig(handlers=[logging.FileHandler(logfilename, 'w', 'utf-8')],level=logging.INFO,format='[%(asctime)s] %(message)s')
#记日志
def log(logMsg):
   logging.info(logMsg); 
def getAgentList(sql_server,sql_user,sql_password,sql_library):

    conn=pymssql.connect(sql_server,sql_user,sql_password,sql_library,charset='cp936');
    cursor = conn.cursor()
   
    sqlExecute='''SELECT c.agentid,c.BindSFBUserName,
c.BindSFBID,
i.userrealname
FROM erp_usercon_{0} c with(nolock)
LEFT JOIN erp_Userinfo_{0} i with(nolock) ON c.UserID =i.userid
LEFT JOIN erp_PurviewNew_{0} p ON p.PurviewID = c.PurviewID
LEFT JOIN erp_DeptRelation_{0} d with(nolock) ON c.deptrelationid =d.drid
WHERE c.deptcompanyid=(SELECT top 1 CompanyID FROM erp_deptcompany_{0} WHERE erpversion=9) AND c.UserAttribute =4 AND p.PurviewContent = '开放平台公司租房顾问'
AND c.userstatus=1'''.format(cityshort);
    #log("获取经纪人列表sql:"+sqlExecute);
    try:
        cursor.execute(sqlExecute);
        agentList=cursor.fetchall();
        #关闭连接
        cursor.close()
        return agentList;
    except Exception as e:
        log(e);
def sendMessageBox(url,sendto,title,content,purpose):
    try:
        requestUrl=url+"?sendto={0}&title={1}&content={2}&purpose={3}".format(sendto,urllib.parse.quote_plus(title),urllib.parse.quote_plus(content),purpose);
        log(requestUrl);
        msgRequest=urllib.request.urlopen(requestUrl);
        msgResult=msgRequest.read();
        msgDom=parseString(msgResult);
        root=msgDom.documentElement;
        success=root.getElementsByTagName("Success")[0].childNodes[0].nodeValue;
        errorMsg=root.getElementsByTagName("Msg")[0].childNodes[0].nodeValue;
        if success=='true':
            #发送成功?
            log("发送成功,时间为{0},发送到:{1}".format(datetime.datetime.now(),sendto));
        else:
            #发送失败
            log("发送失败,错误信息:"+errorMsg);
    except Exception as e: 
        #写日志，不抛异常,不会中断其它步骤
        log(e);
def getAgentTimeList(agentid):
    starttime=(datetime.datetime.now()+datetime.timedelta(-7));
    endtime=(datetime.datetime.now()+datetime.timedelta(-1));#.strftime("%Y%m%d")
    step=datetime.timedelta(1);
    dateKeys=[];
    while starttime<=endtime:
        dateKeys.append(agentid+starttime.strftime("%Y%m%d"));
        starttime+=step;
    return dateKeys;


#=======================================================================#
#read config
config=configparser.ConfigParser();
if istest==1:
    config.read(os.path.join(scriptDictory,'servertest.conf'));
else:
    config.read(os.path.join(scriptDictory,'server.conf'));




#遍历城市，依次读取该城市的经纪人列表
citysStr=config.get("infos","citys");
citysObj=json.loads(citysStr);
try:
    
    
    for city in citysObj:
        
        ##城市信息
        cityshort=city["short"];
        citytype=city["type"];
        #定义redis服务
        #redis
        redis_host=config.get("redisserver_"+citytype,"server");
        redis_port=config.get("redisserver_"+citytype,"port");
        log(redis_host);
        log(redis_port);
        r=redis.StrictRedis(host=redis_host,port=redis_port,db=0);
        #设置城市所属数据库服务
        cityServer="sqlserver_"+citytype;
        sql_server=config.get(cityServer,"server");
        sql_user=config.get(cityServer,"user");
        sql_password=config.get(cityServer,"password");
        sql_library=config.get(cityServer,"library");
        #设置发消息url
        url=config.get("url_"+citytype,"sendMsgForAgentGrab");#挪到下面去
        #读取经纪人
        agentList=getAgentList(sql_server,sql_user,sql_password,sql_library); 
        #log(agentList);
        #遍历经纪人,如果在缓存中值为1，标识参与过抢派单,不用发消息，否则如果0，发消息通知抢单
        sendMsgList=[];
        for row in agentList:
            #log('row = %r' % (row,))
            offset=row[0];# agentid
            username=row[1];#user name
            fields=getAgentTimeList(str(offset));
            listvalues=r.hmget("ordergrab",fields);
            grabSum=0;
            for one in listvalues:
                if one is None:
                    one=0;
                grabSum+=int(one);
            if grabSum==0:
                #发消息
                sendMsgList.append(username);
        agents=','.join(str(i) for i in sendMsgList);
        log(cityshort+"发送列表:"+agents);
        if len(sendMsgList)>0:
            sendMessageBox(url,agents,'来抢单','精准房客源匹配信息就在“来抢单”，还不快来！','eb_grab'); 
except Exception as e:
        log(e);	
