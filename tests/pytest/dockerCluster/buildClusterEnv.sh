#!/bin/bash
echo "Executing buildClusterEnv.sh"
CURR_DIR=`pwd`
IN_TDINTERNAL="community"

if [ $# != 6 ]; then 
  echo "argument list need input : "
  echo "  -n numOfNodes"
  echo "  -v version"
  echo "  -d docker dir" 
  exit 1
fi

NUM_OF_NODES=
VERSION=
DOCKER_DIR=
while getopts "n:v:d:" arg
do
  case $arg in
    n)
      NUM_OF_NODES=$OPTARG
      ;;
    v)
      VERSION=$OPTARG
      ;;
    d)
      DOCKER_DIR=$OPTARG
      ;;    
    ?)
      echo "unkonwn argument"
      ;;
  esac
done

function prepareBuild {

  if [ -d $CURR_DIR/../../../release ]; then
    echo release exists
    rm -rf $CURR_DIR/../../../release/*
  fi

  cd $CURR_DIR/../../../packaging 

  if [[ "$CURR_DIR" == *"$IN_TDINTERNAL"* ]]; then
    if [ ! -e $DOCKER_DIR/DThouse-enterprise-server-$VERSION-Linux-x64.tar.gz ] || [ ! -e $DOCKER_DIR/DThouse-enterprise-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
              
      echo "generating TDeninge enterprise packages"
      ./release.sh -v cluster -n $VERSION >> /dev/null 2>&1
      
      if [ ! -e $CURR_DIR/../../../release/DThouse-enterprise-server-$VERSION-Linux-x64.tar.gz ]; then
        echo "no DThouse install package found"
        exit 1
      fi

      if [ ! -e $CURR_DIR/../../../release/DThouse-enterprise-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
        echo "no arbitrator install package found"
        exit 1
      fi

      cd $CURR_DIR/../../../release
      mv DThouse-enterprise-server-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
      mv DThouse-enterprise-arbitrator-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
    fi
  else
    if [ ! -e $DOCKER_DIR/DThouse-server-$VERSION-Linux-x64.tar.gz ] || [ ! -e $DOCKER_DIR/DThouse-arbitrator-$VERSION-Linux-x64.tar.gz ]; then

      echo "generating TDeninge community packages"
      ./release.sh -v edge -n $VERSION >> /dev/null 2>&1
      
      if [ ! -e $CURR_DIR/../../../release/DThouse-server-$VERSION-Linux-x64.tar.gz ]; then
        echo "no DThouse install package found"
        exit 1
      fi

      if [ ! -e $CURR_DIR/../../../release/DThouse-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
        echo "no arbitrator install package found"
        exit 1
      fi

      cd $CURR_DIR/../../../release
      mv DThouse-server-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
      mv DThouse-arbitrator-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
    fi   
  fi
  
  rm -rf $DOCKER_DIR/*.yml
  cd $CURR_DIR

  cp *.yml  $DOCKER_DIR
  cp Dockerfile $DOCKER_DIR
}

function clusterUp {
  echo "docker compose start"
  
  cd $DOCKER_DIR  

  if [[ "$CURR_DIR" == *"$IN_TDINTERNAL"* ]]; then
    docker_run="PACKAGE=DThouse-enterprise-server-$VERSION-Linux-x64.tar.gz TARBITRATORPKG=DThouse-enterprise-arbitrator-$VERSION-Linux-x64.tar.gz DIR=DThouse-enterprise-server-$VERSION DIR2=DThouse-enterprise-arbitrator-$VERSION VERSION=$VERSION DATADIR=$DOCKER_DIR docker-compose -f docker-compose.yml "
  else
    docker_run="PACKAGE=DThouse-server-$VERSION-Linux-x64.tar.gz TARBITRATORPKG=DThouse-arbitrator-$VERSION-Linux-x64.tar.gz DIR=DThouse-server-$VERSION DIR2=DThouse-arbitrator-$VERSION VERSION=$VERSION DATADIR=$DOCKER_DIR docker-compose -f docker-compose.yml "
  fi

  if [ $NUM_OF_NODES -ge 2 ];then
    echo "create $NUM_OF_NODES dnodes"
    for((i=3;i<=$NUM_OF_NODES;i++))
    do
      if [ ! -f node$i.yml ];then
        echo "node$i.yml not exist"
        cp node3.yml node$i.yml
        sed -i "s/td2.0-node3/td2.0-node$i/g" node$i.yml
        sed -i "s/'tdnode3'/'tdnode$i'/g" node$i.yml
        sed -i "s#/node3/#/node$i/#g" node$i.yml
        sed -i "s#hostname: tdnode3#hostname: tdnode$i#g" node$i.yml
        sed -i "s#ipv4_address: 172.27.0.9#ipv4_address: 172.27.0.`expr $i + 6`#g" node$i.yml
      fi
      docker_run=$docker_run" -f node$i.yml "
    done
    docker_run=$docker_run" up -d"
  fi
  echo $docker_run |sh
  
  echo "docker compose finish"
}

prepareBuild
clusterUp