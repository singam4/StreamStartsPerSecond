# StreamStartsPerSecond

1. Clone
$ git clone https://github.com/singam4/StreamStartsPerSecond.git my-sps

2. Install
$ cd my-sps/Netflix_SPS
$ mvn install

3. Run
$ mvn -q exec:java -e -Dexec.mainClass=netflix_sps.StreamStartsPerSecond

4. Example output 
{"device": "xbox_360", "sps": 36, "title": "stranger things", "country": "UK"}
{"device": "android", "sps": 15, "title": "orange is the new black", "country": "JP"}
{"device": "xbox_one_x", "sps": 43, "title": "stranger things", "country": "JP"}
