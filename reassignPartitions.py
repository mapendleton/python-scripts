#!/usr/bin/env python3

import subprocess
import sys
import paramiko as pm
import re
import json
import getpass as gp


class ShellHandler:

    def __init__(self, host, user, psw):
        self.ssh = pm.SSHClient()
        self.ssh.set_missing_host_key_policy(pm.AutoAddPolicy())
        self.ssh.connect(host, username=user, password=psw, port=22)

        channel = self.ssh.invoke_shell()
        self.stdin = channel.makefile('wb')
        self.stdout = channel.makefile('r')

    def __del__(self):
        self.stdin.close()
        self.stdout.close()
        self.ssh.close()

    def execute(self, cmd):
        """
        :param cmd: the command to be executed on the remote computer
        :examples:  execute('ls')
                    execute('finger')
                    execute('cd folder_name')
        """
        cmd = cmd.strip('\n')
        self.stdin.write(cmd + '\n')
        finish = 'end of stdOUT buffer. finished with exit status'
        echo_cmd = f'echo {finish} $?'
        self.stdin.write(echo_cmd + '\n')
        shin = self.stdin
        self.stdin.flush()

        shout = []
        sherr = []
        for line in self.stdout:
            if str(line).startswith(cmd):
                # up for now filled with shell junk from stdin
                shout = []
            elif str(line).startswith(finish):
                # our finish command ends with the exit status
                exit_status = int(str(line).rsplit(maxsplit=1)[1])
                if exit_status:
                    # stderr is combined with stdout.
                    # thus, swap sherr with shout in a case of failure.
                    sherr = shout
                    shout = []
                break
            elif finish in str(line):
                continue
            else:
                # get rid of 'coloring and formatting' special characters
                shout.append(re.compile(r'(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]').sub('', line).
                             replace('\b', '').replace('\r', ''))

        # first and last lines of shout/sherr contain a prompt
        return shin, shout[1:-1], sherr[1:-1]
    
    def simpleExecute(self, cmd):
        stdin, stdout, stderr = self.execute(cmd)
        if stderr:
            [print(line) for line in stderr]
            sys.exit(1)
        return stdout

class Drive:
    def __init__(self,totalBytes,bytesUsed,bytesLeft,path):
        self.totalBytes = totalBytes
        self.bytesUsed = bytesUsed
        self.bytesLeft = bytesLeft
        self.path = path
        self.percentUsed = int(bytesUsed)/int(totalBytes)
        self.bytesNeeded = 0
        self.topicsToMove = []
    def __str__(self):
        return f'''Drive: {self.path}
                    totalBytes: {self.totalBytes}
                    bytesUsed: {self.bytesUsed}
                    bytesLeft: {self.bytesLeft}
                    percentUsed: {self.percentUsed*100}%
                    bytesNeeded: {self.bytesNeeded}
                    topicsToMove: {self.topicsToMove}'''
    def addBytesNeeded(self,goalPercent,percentThresholdToRemove):
        if goalPercent <= self.percentUsed <= goalPercent + percentThresholdToRemove:
            print(f'{self.percentUsed} is too close to goal to remove any data')
        else:
            percentNeeded = goalPercent - self.percentUsed
            self.bytesNeeded = percentNeeded*float(self.totalBytes)
    def addTopicsToMove(self,shellHandler):
        if self.bytesNeeded < 0:
            shellHandler.execute(f'cd {self.path}/data')
            stdout = shellHandler.simpleExecute('du -sb * | sort -h | tail -80')
            stdout.reverse()
            bytes = 0
            for line in stdout:
                topicSize_Name = line.split()
                topicSize = float(topicSize_Name[0])
                bytesAdded = topicSize + bytes
                if bytesAdded <= self.bytesNeeded*-1:
                    print(f'topicPartition: {topicSize_Name[1]}, Size: {int(topicSize_Name[0])*1e-9:.1f}G Can be moved')
                    self.topicsToMove.append((topicSize,topicSize_Name[1]))
                    bytes += topicSize
                if bytes == self.bytesNeeded*-1:
                    break
        

def print_array_no_line(str_arr):
    for line in str_arr:
        print(line.strip())
    print('\n')

def getDriveAvgGoal(drives):
    goal = 0
    for drive in drives:
        goal += drive.percentUsed
    goal = goal/len(drives)
    return goal

def createTopicsToMoveJsonFile(topicsToMove,driveToMoveTo, maxNumPartitions):
    topicsToMoveDict = {
        'topics': [],
        'version': 1
    }
    maxBytes = driveToMoveTo.bytesNeeded
    bytes = 0
    topicsLeft = []
    topicPartitionsBeingMoved = []

    print(f'Max num of bytes that can be transferred to {driveToMoveTo.path} = {str(maxBytes)} Max num of Partitions that can be moved at one time: {maxNumPartitions}\n')
    numPartitions = 0
    for i, topicSize_Name in enumerate(topicsToMove):
        topicSize = str(topicSize_Name[0])
        topicName = str(topicSize_Name[1])
        fivePercentOfMax = maxBytes*.05
        if bytes <= maxBytes and bytes+float(topicSize) <= maxBytes*fivePercentOfMax and numPartitions < maxNumPartitions: #max is estimated so going over a little is ok
            bytes += float(topicSize)
            fullTopicName = topicSize_Name[1].rsplit('-',1)[0] #remove partition num ie. <topicName>-2
            topicPartitionsBeingMoved.append(topicName)
            topicDict = {'topic': fullTopicName}
            if topicDict not in topicsToMoveDict['topics']:
                topicsToMoveDict['topics'].append(topicDict)
            numPartitions += 1
        else:
            topicsLeft.append(topicSize_Name)
    topicsToMoveJson = json.dumps(topicsToMoveDict, indent=2)
    bytesLeft = maxBytes - bytes if maxBytes - bytes >= 0 else 0
    print(f'TOPICS TO MOVE: {topicsToMoveJson}\nTOPICS LEFT OVER: {topicsLeft}\nNUM BYTES THAT CAN STILL BE TRANSFERRED TO {driveToMoveTo.path}: {bytesLeft}\n')
    print(f'TOPIC PARTITIONS BEING MOVED: {topicPartitionsBeingMoved}')
    return topicsToMoveJson, topicPartitionsBeingMoved, topicsLeft, bytesLeft

def modifyReassignmentConfigurationJson(reassignmentConfigurationDict,topicsPartitionsToMove,driveToMoveTo,brokerID):
    topicsAndPartitions = [tuple(x.rsplit('-',1)) for x in topicsPartitionsToMove]
    newPartitionList = [partition for partition in reassignmentConfigurationDict['partitions'] if (partition['topic'],str(partition['partition'])) in topicsAndPartitions]
    reassignmentConfigurationDict['partitions'] = newPartitionList
    for ind, partition in enumerate(reassignmentConfigurationDict['partitions'].copy()):
        i = partition['replicas'].index(int(brokerID))
        reassignmentConfigurationDict['partitions'][ind]['log_dirs'][i] = driveToMoveTo
    reassignmentConfigurationJson = json.dumps(reassignmentConfigurationDict, indent=2)
    return reassignmentConfigurationJson, len(newPartitionList)



def main():
    if len(sys.argv) == 1:
        HOST='g-vmx-2n-hubapp-confluent-kb-001.dev.azeus.gaptech.com'
        USER=''
        PASS=''
    elif len(sys.argv) >= 2:
        if sys.argv[1] == 'test':
            test()
            sys.exit(0) #not implemented yet
        elif '@' in sys.argv[1]:
            USER, HOST = sys.argv[1].split('@')
            PASS = gp.getpass()
        else:
            print(f'invalid argument {sys.argv[1]}\n try: <user>@<host> or "test"')
            sys.exit(1)

    print('creating shell handler...')
    ssh = ShellHandler(HOST,USER,PASS)
    print('shell handler created, sending pingID request...\n')
    ssh.execute('sudo su - s-cpadmin')
    stdout = ssh.simpleExecute('df -kh --output=size,used,avail,pcent,target | grep data | sort')
    print_array_no_line(stdout)

    user_input = input('Balance partitions? Y/n:')
    # user_input = 'Y'
    brokerID = HOST[-23]
    env = HOST[7]
    percentThresholdToRemove = .05 #if percentThresholdToRemove+goalPercent >= percent used >= goalPercent 
    maxNumPartitions = 5
    print('brokerID: '+brokerID)

    if (user_input == 'Y'):
        stdout = ssh.simpleExecute('df -k -B1 --output=size,used,avail,target | grep data | sort')
        drives = [Drive(*line.split()) for line in stdout]
        goal = getDriveAvgGoal(drives)
        print(f'Goal percent: {goal} - {goal*100}%')
        posDrives, negDrives = [], []
        for drive in drives:
            drive.addBytesNeeded(goal,percentThresholdToRemove)
            print(drive)
            drive.addTopicsToMove(ssh)
            if drive.bytesNeeded > 0:
                posDrives.append(drive)
            elif drive.bytesNeeded < 0:
                negDrives.append(drive)
        print('\nCREATING JSON FILES FOR TRANSFER...\n')
        numOfLoops,maxNumberOfLoops = 0,5
        while negDrives and posDrives and numOfLoops <= maxNumberOfLoops:
            numOfLoops += 1
            maxPosDrive = next(x for x in posDrives if x.bytesNeeded == max(drive.bytesNeeded for drive in posDrives))
            maxNegDrive = next(x for x in negDrives if x.bytesNeeded == min(drive.bytesNeeded for drive in negDrives)) #Min because I want the biggest (lowest) negative number
            if len(maxNegDrive.topicsToMove) == 0: #if the list of topics to move is empty
                print(f'{maxNegDrive.path} has no more topics to move.')
                negDrives.remove(maxNegDrive)
                continue
            if maxPosDrive.bytesNeeded <= 0: #disk has already recieved all data it had space for
                print(f'{maxPosDrive.path} has reached percent threshold.')
                posDrives.remove(maxPosDrive)
                continue
            print(f'MAX POS DRIVE (CAN TAKE DATA): {maxPosDrive}')
            print(f'MAX NEG DRIVE (CAN MOVE DATA): {maxNegDrive}')
            topicsToMoveJson, topicPartitionsBeingMoved, topicsLeft, bytesLeft = createTopicsToMoveJsonFile(maxNegDrive.topicsToMove,maxPosDrive,maxNumPartitions)
            maxPosDrive.bytesNeeded = bytesLeft
            maxNegDrive.topicsToMove = topicsLeft
            ssh.simpleExecute(f"echo '{topicsToMoveJson}' > {maxNegDrive.path}/data/topicsToMove.json")
            print(f'Disc moving data: {maxNegDrive}\n to: {maxPosDrive}\n')
            command = f'kafka-reassign-partitions --zookeeper g-vmx-2{env}-hubapp-confluent-zk-001.dev.azeus.gaptech.com:2181 --topics-to-move-json-file {maxNegDrive.path}/data/topicsToMove.json --broker-list 1,2,3 --generate'
            print(command+'\n')
            stdout = ssh.simpleExecute(command)
            reassignmentConfigurationJson, numOfPartitionsBeingMoved = modifyReassignmentConfigurationJson(json.loads(stdout[5]),topicPartitionsBeingMoved,maxPosDrive.path+'/data',brokerID)
            reassignmentConfigurationFilePath = f'{maxNegDrive.path}/data/reassignmentConfiguration.json'
            ssh.simpleExecute(f"echo '{reassignmentConfigurationJson}' > {reassignmentConfigurationFilePath}")
            print(f'Reassignment Configuration JSON: \n {reassignmentConfigurationJson}')
            commandTemplate = f'kafka-reassign-partitions --zookeeper g-vmx-2{env}-hubapp-confluent-zk-001.dev.azeus.gaptech.com:2181,g-vmx-2{env}-hubapp-confluent-zk-002.dev.azeus.gaptech.com:2181,g-vmx-2{env}-hubapp-confluent-zk-003.dev.azeus.gaptech.com:2181 --command-config /home/apps/s-cpadmin/Util/Kafka_SSL.cfg --reassignment-json-file {reassignmentConfigurationFilePath} --bootstrap-server g-vmx-2{env}-hubapp-confluent-kb-001.dev.azeus.gaptech.com:9093,g-vmx-2{env}-hubapp-confluent-kb-002.dev.azeus.gaptech.com:9093,g-vmx-2{env}-hubapp-confluent-kb-003.dev.azeus.gaptech.com:9093 '
            ssh.simpleExecute(commandTemplate+'--execute')

            print('\n'*(numOfPartitionsBeingMoved))
            UP = f"\x1B[{numOfPartitionsBeingMoved+2}A"
            CLR = "\x1B[0K"
            y=0
            while True:
                stdout = ssh.simpleExecute(commandTemplate+'--verify')
                partitions = [x for x in stdout if x.count('Reassignment of partition') > 0]
                statusOfPartitions=[1 if x.count('completed successfully')==1 else -1 if x.count('failed')==1 else 2 if x.count('in progress')==1 else 0 for x in partitions]
                successCount, failCount, inProgressCount = 0,0,0
                for status in statusOfPartitions:
                    if status == 1:
                        successCount+=1
                    elif status == -1:
                        failCount+=1
                    elif status == 2:
                        inProgressCount+=1
                print(UP)
                print('Reassigning Partitions'+('.'*y)+CLR)
                [print(x) for x in partitions]
                y+=1 #for the loading dots
                if y == 4:
                    y = 0
                if successCount+failCount == numOfPartitionsBeingMoved:
                    if successCount == numOfPartitionsBeingMoved:
                        print(f'{numOfPartitionsBeingMoved} partitions moved to {maxPosDrive.path+"/data"} successfully')
                        break
                    elif successCount == 0:
                        print(f'{failCount} partitions failed being moved to {maxPosDrive.path+"/data"} exiting script')
                        sys.exit(1)
        print('FINISHED...')
    else:
        sys.exit(0)

def test(): #CURRENTLY BROKEN
    brokerID = 2
    percentThresholdToRemove = .05
    maxNumPartitions = 5
    driveLists = [
        ['2128439787520', '474803814400', '1556468125696', '/data/kafka1'], #'24%'
        ['2128439787520', '538838528000', '1492433412096', '/data/kafka2'], #'27%'
        ['2128439787520', '1702751830016', '425687957504', '/data/kafka3'], #'80%'
        ['2128439787520', '538838528000', '1492433412096', '/data/kafka4'] #'27%'
    ]
    drives = [Drive(*l) for l in driveLists]
    [print(x) for x in drives]
    goalPercent = getDriveAvgGoal(drives)
    print(goalPercent)
    posDrives, negDrives = [], []
    for drive in drives:
        drive.addBytesNeeded(goalPercent,percentThresholdToRemove)
        print(drive)
        if drive.bytesNeeded > 0:
            posDrives.append(drive)
        elif drive.bytesNeeded < 0:
            drive.topicsToMove=[(125000000000.0, 'testTopic-1'),(125000000000.0, 'testTopic-2'),(50000000000.0, 'testTopic-3'),(125000000000.0, 'testTopic-4'),(62500000000.0, 'testTopic-5'),(31250000000.0, 'testTopic1-0'),(31250000000.0, 'testTopic1-1'),(62500000000.0, 'testTopic1-2'),(62500000000.0, 'testTopic1-3'),(31250000000.0, 'testTopic2-0'),(15625000000.0, 'testTopic-0'),(15625000000.0, 'testTopic2-1'),(15625000000.0, 'testTopic2-2'),(15625000000.0, 'testTopic2-3'),(15625000000.0, 'testTopic2-4'),(15625000000.0, 'testTopic3-0')]
            negDrives.append(drive)
    numOfLoops,maxNumberOfLoops = 0,5
    print(posDrives)            
    print(negDrives)
    print('\n')
    while negDrives and posDrives and numOfLoops <= maxNumberOfLoops:
        numOfLoops += 1
        maxPosDrive = next(x for x in posDrives if x.bytesNeeded == max(drive.bytesNeeded for drive in posDrives))
        maxNegDrive = next(x for x in negDrives if x.bytesNeeded == min(drive.bytesNeeded for drive in negDrives)) #Min because I want the biggest (lowest) negative number
        if len(maxNegDrive.topicsToMove) == 0: #if the list of topics to move is empty
            print(f'{maxNegDrive.path} has no more topics to move.')
            negDrives.remove(maxNegDrive)
            continue
        if maxPosDrive.bytesNeeded <= 0: #disk has already recieved all data it had space for
            print(f'{maxPosDrive.path} has reached percent threshold.')
            posDrives.remove(maxPosDrive)
            continue
        print(f'MAX POS DRIVE (CAN TAKE DATA): {maxPosDrive}')
        print(f'MAX NEG DRIVE (CAN MOVE DATA): {maxNegDrive}')
        topicsToMoveJson, topicPartitionsBeingMoved, topicsLeft, bytesLeft = createTopicsToMoveJsonFile(maxNegDrive.topicsToMove,maxPosDrive,maxNumPartitions)
        maxPosDrive.bytesNeeded = bytesLeft
        maxNegDrive.topicsToMove = topicsLeft
        reassignmentConfigurationDict = {
            'version': 1,
            'partitions': [
                {'topic':'testTopic','partition':0,'replicas':[1,2,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic','partition':1,'replicas':[2,1,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic','partition':2,'replicas':[3,2,1],'log_dirs':['any','any','any']},
                {'topic':'testTopic','partition':3,'replicas':[1,3,2],'log_dirs':['any','any','any']},
                {'topic':'testTopic','partition':4,'replicas':[1,2,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic','partition':5,'replicas':[2,1,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic1','partition':0,'replicas':[3,2,1],'log_dirs':['any','any','any']},
                {'topic':'testTopic1','partition':1,'replicas':[2,1,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic1','partition':2,'replicas':[2,1,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic1','partition':3,'replicas':[3,2,1],'log_dirs':['any','any','any']},
                {'topic':'testTopic2','partition':0,'replicas':[1,3,2],'log_dirs':['any','any','any']},
                {'topic':'testTopic2','partition':1,'replicas':[1,2,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic2','partition':2,'replicas':[1,2,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic2','partition':3,'replicas':[1,3,2],'log_dirs':['any','any','any']},
                {'topic':'testTopic2','partition':4,'replicas':[2,1,3],'log_dirs':['any','any','any']},
                {'topic':'testTopic3','partition':0,'replicas':[1,2,3],'log_dirs':['any','any','any']},
            ]
        }
        reassignmentConfigurationJson, numOfPartitionsMoved = modifyReassignmentConfigurationJson(reassignmentConfigurationDict,topicPartitionsBeingMoved,maxPosDrive.path+'/data',brokerID)
        print(f'Reassignment Configuration JSON: \n {reassignmentConfigurationJson}')
        print(numOfPartitionsMoved)
#SCRIPT ENTRY POINT    
main()        