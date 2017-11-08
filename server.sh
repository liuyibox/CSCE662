#!/bin/sh

echo ""
echo "this script aims to start three servers"
echo ""

echo $1
if [ $1 == "masterServer" ]; then

	echo "master server has 4 master processes now:"
	./fbsd -l 1 -p 6001 -h masterServer -m 1 -i 0 &
	export pid1=$!
#	echo -n "get a thread "		
#	echo -n $pid1
#	echo " at 6001"		
	./fbsd -p 6002 -h masterServer -m 1 -i 0 &
	export pid2=$!	
#	echo -n "get a thread "		
#	echo -n $pid2
#	echo " at 6002"
	./fbsd -p 6003 -h masterServer -m 1 -i 0 &
	export pid3=$!		
#	echo -n "get a thread "		
#	echo -n $pid3
#	echo " at 6003"
	./fbsd -c 1 -p 6004 -h masterServer -m 1 -i 0 &
	export pid4=$!
#	echo -n "get a thread "		
#	echo -n $pid4
#	echo " at 6004"
#	wait $pid1 $pid2 $pid3 $pid4 &
	control_c(){
		kill $pid1
		kill $pid2
		kill $pid3
		kill $pid4
		kill -9 $$
	}
	trap control_c SIGINT
	while true; do read x; done
	.
elif [ $1 == "server1" ]; then
	echo "server1 has 3 processes now:"
	./fbsd -l 1 -p 6005 -h server1 -i 1 &
	export pid1=$!
#	echo -n "get a thread "		
#	echo -n $pid1
#	echo " at 6005"
	./fbsd -p 6006 -h server1 -i 1 &
	export pid2=$!
#	echo -n "get a thread "		
#	echo -n $pid2
#	echo " at 6006"
	./fbsd -c 1 -p 6007 -h server1 -i 1&
	export pid3=$!
#	echo -n "get a thread "		
#	echo -n $pid3
#	echo " at 6007"
#	wait $pid1 $pid2 $pid3
	control_c(){
		kill $pid1
		kill $pid2
		kill $pid3
		kill -9 $$
	}
	trap control_c SIGINT
	while true; do read x; done
	.
else
	echo "server2 has 3 processes now:"
	./fbsd -l 1 -p 6008 -h server2 -i 2 &
	export pid1=$!
#	echo -n "get a thread "		
#	echo -n $pid1
#	echo " at 6008"
	./fbsd -p 6009 -h server2 -i 2 &
	export pid2=$!
#	echo -n "get a thread "		
#	echo -n $pid2
#	echo " at 6009"
	./fbsd -c 1 -p 6010 -h server2 -i 2 &
	export pid3=$!
#	echo -n "get a thread "		
#	echo -n $pid3
#	echo " at 6010"
#	wait $pid1 $pid2 $pid3
	control_c(){
		kill $pid1
		kill $pid2
		kill $pid3
		kill -9 $$
	}
	trap control_c SIGINT
	while true; do read x; done
	.
fi

