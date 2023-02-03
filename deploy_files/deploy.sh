#!/bin/bash
# mac command to make this script executable (execute on terminal)
# > chmod 755 deploy.sh
# launch script on terminal
# > ./deploy.sh
# connect to bridge: ssh ubuntu@137.194.211.146
login="ubuntu"
deployemntFolder="deploy_files/"
deploymentFiles=("load_data.py" "deploy.sh" "masterfilelist-translation.txt" "load_csv_script.py" "test.csv")

bridgeServer="137.194.211.146"
bridgeTargetFolder="/mnt/ubuntu/"

computers=("tp-hadoop-56")

# connect to computers and remove existing deployment folder
for c in ${computers[@]}; do
  command1=("ssh" "$login@$c" "rm -rf $deployemntFolder; mkdir $deployemntFolder")
  echo ${command1[*]}
  "${command1[@]}"&
done

# copy deployment files from bridge to computers
for c in ${computers[@]}; do
  for file in ${deploymentFiles[@]}; do
    command2=("scp" "$login@$bridgeServer:$bridgeTargetFolder$deployemntFolder$file" "$login@$c:$deployemntFolder$file")
    echo ${command2[*]}
    "${command2[@]}"&
  done
done
