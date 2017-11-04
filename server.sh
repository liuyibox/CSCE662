#!/bin/sh

echo ""
echo "this script aims to start three servers"
echo ""

echo $1
if [ $1 == "masterServer" ]; then
	echo "master server has 4 master processes now:"
	./fbsd -l 1 -p 6001 -h masterServer -m 1 -i 0 &
	export pid1=$!		
	echo $pid1		
	./fbsd -p 6002 -h masterServer -m 1 -i 0 &
	export pid2=$!	
	echo $pid2
	./fbsd -p 6003 -h masterServer -m 1 -i 0 &
	export pid3=$!		
	echo $pid3
	./fbsd -c 1 -p 6004 -h masterServer -m 1 -i 0 &
	export pid4=$!
	echo $pid4
	wait $pid1 $pid2 $pid3 $pid4
	.
elif [ $1 == "server1" ]; then
	echo "server1 has 3 processes now:"
	./fbsd -l 1 -p 6005 -h server1 -i 1 &
	export pid1=$!
	echo $pid1
	./fbsd -p 6006 -h server1 -i 1 &
	export pid2=$!
	echo $pid2
	./fbsd -c 1 -p 6007 -h server1 -i 1&
	export pid3=$!
	echo $pid3
	wait $pid1 $pid2 $pid3
	.
else
	echo "server2 has 3 processes now:"
	./fbsd -l 1 -p 6008 -h server2 -i 2 &
	export pid1=$!
	echo $pid1
	./fbsd -p 6009 -h server2 -i 2 &
	export pid2=$!
	echo $pid2
	./fbsd -c 1 -p 6010 -h server2 -i 2 &
	export pid3=$!
	echo $pid3
	wait $pid1 $pid2 $pid3
	.
fi

