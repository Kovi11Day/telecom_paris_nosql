#!/bin/bash
# mac command to make this script executable (execute on terminal)
# > chmod 755 deploy_to_bridge.sh
# launch script on terminal
# > ./deploy_to_bridge.sh
# connect to bridge: ssh ubuntu@137.194.211.146
login="ubuntu"
bridgeServer="137.194.211.146"
bridgeTargetFolder="/mnt/ubuntu/"
deployemntFolder="deploy_files/"
deploymentFiles=("load_data.py" "deploy.sh" "masterfilelist-translation.txt" "load_csv_script.py" "test.csv")

# connect to bridge and remove existing deployment folder
command1=("ssh" "$login@$bridgeServer" "cd $bridgeTargetFolder; rm -rf $bridgeTargetFolder$deployemntFolder; mkdir $bridgeTargetFolder$deployemntFolder")
echo ${command1[*]}
"${command1[@]}"

for file in ${deploymentFiles[@]}; do
  # copy deployment files to bridge
  command2=("scp" "$deployemntFolder$file" "$login@$bridgeServer:$bridgeTargetFolder$deployemntFolder$file")
  echo ${command2[*]}
  "${command2[@]}"&
done

# from bridge launch deploy.sh
#sleep 1
#command1=("ssh" "$login@$bridgeServer" "cd $bridgeTargetFolder$deployemntFolder; ./deploy.sh")
#echo ${command1[*]}
#"${command1[@]}"
