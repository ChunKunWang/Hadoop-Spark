#!/bin/bash

WebLink="https://archive.org/download/2015_reddit_comments_corpus/reddit_data"
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

cd Corpus/
echo -e "download all files ..."

wget --no-check-certificate https://archive.org/download/2015_reddit_comments_corpus/reddit_data/2007/RC_2007-10.bz2
wget --no-check-certificate https://archive.org/download/2015_reddit_comments_corpus/reddit_data/2007/RC_2007-11.bz2
wget --no-check-certificate https://archive.org/download/2015_reddit_comments_corpus/reddit_data/2007/RC_2007-12.bz2

for i in {1..5}
do
    for j in {1..12}
    do
        wget --no-check-certificate ${WebLink}/${Years[i]}/RC_${Years[i]}-${Mons[j]}.bz2 
    done
done

echo -e "Unzip all files ..."
bzip2 -d *.bz2

echo -e "Upload Corpus ..."
hadoop fs -copyFromLocal /home/amos/workpool/Project/tmp/Corpus /user/amos/

