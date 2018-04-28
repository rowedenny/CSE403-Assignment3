#!/bin/bash
python myServer.py $1 $2 &
python myClient.py $1 $2 &
