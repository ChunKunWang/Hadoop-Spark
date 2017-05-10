#!/bin/bash

Years[1]="2008"
Years[2]="2009"
Years[3]="2010"
Years[4]="2011"
Years[5]="2012"
Mons[1]="01"
Mons[2]="02"
Mons[3]="03"
Mons[4]="04"
Mons[5]="05"
Mons[6]="06"
Mons[7]="07"
Mons[8]="08"
Mons[9]="09"
Mons[10]="10"
Mons[11]="11"
Mons[12]="12"

echo -e "Running all experiments ..."

./run.sh RC_2007-10 Out_RC_2007-10
./run.sh RC_2007-11 Out_RC_2007-11
./run.sh RC_2007-12 Out_RC_2007-12

for i in {1..5}
do
    for j in {1..12}
    do
        ./run.sh RC_${Years[i]}-${Mons[j]} Out_RC_${Years[i]}-${Mons[j]}
    done
done

echo -e "Finish all experiments !!!"

