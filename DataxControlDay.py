#!/opt/anaconda2/bin/python2.7
# -*- coding: utf8 -*-

'''
Created on 2018年10月11日

@author: wzt
设计思路:
1：首先读取 /hdd02/opt/app/datax/job 目录下的所有 .json 的文件
2：使用 json 组件来解析 文件
3：替换 oracleReader 中的 where 字符串条件，替换条件是参数，因为有很多文件
   (条件怎么来，设定初始条件后累加时间，记录在zk里面)
4:写 json 文件
5：运行datax 的命令行
6:删除文件
7：关键问题：并发控制(归结为py的多线程控制)，这次先不控制，采用顺序调用的方法

'''

import sys
import os
import signal
import subprocess
import json
import MySQLdb
import time
import datetime
import subprocess

#扫描目录,获取文件列表
def scanDir(directory,prefix="job-",postfix=".json"):
    filesList=[]

    for root, sub_dirs, files in os.walk(directory):
        for special_file in files:
            if postfix:
                if special_file.endswith(postfix):
                    filesList.append(os.path.join(root,special_file))
            elif prefix:
                if special_file.startswith(prefix):
                    filesList.append(os.path.join(root,special_file))
            else:
                filesList.append(os.path.join(root,special_file))

    print filesList
    return filesList

#读取文件列表，并转为json
def readFiles(fileList):
    dictJson = {}
    for filePath in fileList:
        try:
            strContent = ''
            f = open(filePath, 'r')
            #try:
            #   print json.load(f)
            #except BaseException:
            #   print "error"

            lineList = f.readlines()
            for i in range(0, len(lineList)):
                lineList[i] = lineList[i].strip('\n')
                strContent += lineList[i]

            #str转json对象
            dictJson[lastPosString(filePath)] = json.loads(strContent);
            #print(dictJson)

        finally:
            if f:
               f.close()
    return dictJson

#修改 where 条件
def updateWhere(dictJson,replaceCond):
    for keyItem in dictJson:
        #修改where条件
        #print  keyItem,dictJson[keyItem]['job']['content'][0]['reader']['parameter']['where']
        dictJson[keyItem]['job']['content'][0]['reader']['parameter']['where'] = replaceCond
    return dictJson

#写入临时文件,timeFlag='t0'或者‘t1’
def wrireFiles(jobDir,dictJson,timeFlag='t1'):
    for keyItem in dictJson:
        nowTime = int(time.time())
        if(timeFlag == 't1'):
            nowTime_format = time.strftime("%Y%m%d",time.localtime(nowTime))
            f = open(jobDir+'/'+keyItem+'-'+timeFlag+'-'+nowTime_format+'.json','w')
            json_format = json.dumps(dictJson[keyItem],indent=4)
            f.write(json_format)
        else:
            nowTime_format = time.strftime("%Y%m%d%H%M%S",time.localtime(nowTime))
            f = open(jobDir+'/'+'-'+timeFlag+'-'+nowTime_format+'.json','w')
            json_format = json.dumps(dictJson[keyItem],indent=4)
            f.write(json_format)


#字符串最后出现的位置
def lastPosString(str,strFind="/"):
    pos = str.rfind("/")
    return str[pos+1:]

#获取昨天的日期
def getYesterday():
    today=datetime.date.today()
    oneday=datetime.timedelta(days=1)
    yesterday=today-oneday
    #time.strftime("%Y%m%d%H%M%S",yesterday)
    return yesterday.strftime('%Y%m%d')

#获取今天的日期
def getToday():
    today=datetime.date.today()
    return today.strftime('%Y%m%d')

#扫面目录，执行目录下的所有文件
def execFiles(directory,prefix="job-",postfix=".json"):
    #下面是串行执行的，以后要改为 多线程 并行执行,用线程池解决无等待的问题。。。
    dictRunFlag={}

    for root, sub_dirs, files in os.walk(directory):
        for special_file in files:
            if postfix:
                if (special_file.endswith(postfix) and special_file.startswith(prefix)):
                    execFilePath = os.path.join(root,special_file)
                    startCommand = '/opt/anaconda2/bin/python2.7 /hdd02/opt/app/datax/bin/datax.py '+execFilePath
                    child_process = subprocess.Popen(startCommand, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
                    #(stdout, stderr) = child_process.communicate()
                    while True:
                        line = child_process.stdout.readline()
                        print line
                        retPos = line.find("DataX jobId [0] completed successfully.")
                        if(retPos > -1):
                            dictRunFlag[execFilePath]=True
                        if not line:
                            break
                    child_process.wait()
                    child_process.stdout.close()
    return dictRunFlag

#将任务号插入任务表
def insertSuccessJob(dictJob):
    # 打开数据库连接
    db = MySQLdb.connect("192.168.199.147", port=3308,user="root", passwd="hadoop", db="bpmjob", charset='utf8' )
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    for keyItem in dictJob:
        #SQL插入语句,任务名称,任务名称 job_name,运行日期 run_date,任务结束时间 run_end_time,任务状态 run_state
        nowTime = int(time.time())
        nowTime_format = time.strftime("%Y%m%d%H%M%S",time.localtime(nowTime))
        sql = "insert into bpmjob.bpmtask(job_name,run_date,run_end_time,run_state) values(\'"+keyItem+"\',\'"\
              +getToday()+"\',\'"+nowTime_format+"\',\'"+"S\')"
        print sql
        try:
           # 执行sql语句
           cursor.execute(sql)
           # 提交到数据库执行
           db.commit()
        except:
           # Rollback in case there is any error
           db.rollback()
    # 关闭数据库连接
    db.close()

if __name__ == "__main__":
    #扫描目录，读取文件,返回一个 linkmap 结构，文件内容使用 bytes来存储，是否转为json object
    #替换 jsonobject里面的 where 时间条件
    #迭代 map 运行命令(anaconda2 2.7 环境)
    dirDataWareHouse = sys.argv[1]
    jobDir = '/tmp/datax'
    #print sys.argv[1]

    fileList = scanDir(dirDataWareHouse)
    dictJson = readFiles(fileList)

    strReplace = "DATA_DATE <= \'"+getYesterday()+"\'"+" and rownum < 10"
    print strReplace
    dictUpdateJson = updateWhere(dictJson,strReplace)
    #写入临时文件
    wrireFiles(jobDir,dictUpdateJson,'t1')
    #执行 cmd 命令，执行目录下的所有json文件
    dictRunFlag = execFiles(jobDir,prefix="job-",postfix=".json")
    #插入 成功任务表
    insertSuccessJob(dictRunFlag)
    #where 条件需要替换，对于T+1由 调度直接传入前一天的时间 example:20181014
    #对于准实时调用，需要将上次的时间记录在mysql或者pg中，不哟个zk的原因是zk的python module 需要单独安装
    #objJob = dictJson['job-oracle2kafka.json']
    #dictItem1 = objJob['job']['content']
    #print dictItem1[0]['reader']['parameter']['where'],len(dictItem1)
    #for item in objJob['job']['content']:
    #     print type(item),item
    #print objJob

