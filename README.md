# StreamStartsPerSecond

## Clone
#####  $ git clone https://github.com/singam4/StreamStartsPerSecond.git my-sps

## Install
##### $ cd my-sps/Netflix_SPS
##### $ mvn install

## Run
#### Persistent version
##### $ mvn -q exec:java -e -Dexec.mainClass=netflix_sps.StreamStartsPerSecond
#### In-memory version (6/29)
##### $ mvn -q exec:java -e -Dexec.mainClass=netflix_sps.SpsInMemory
#### Disk backed in-memory version (6/30)
##### $ mvn -q exec:java -e -Dexec.mainClass=netflix_sps.SpsDiskBacked

## Example output 
##### {"device": "xbox_360", "sps": 36, "title": "stranger things", "country": "UK"}
##### {"device": "android", "sps": 15, "title": "orange is the new black", "country": "JP"}
##### {"device": "xbox_one_x", "sps": 43, "title": "stranger things", "country": "JP"}
