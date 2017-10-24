Requires maven, java, and docker

./build.sh

Takes awhile...

./start.sh

Takes another while...

Eventually can hit:
http://localhost:8080

to get to Ambari

user: admin
pw:   admin


When it's all finished, can hit:
http://localhost:6080

to get to Ranger

user: admin
pw:   admin

Browse to:

http://localhost:6080/index.html#!/service/1/policies/0

To see the accumulo policies


From rangerAdmin directory (where ./start.sh was executed) can execute:

./ashell.sh doctorbob

to open an accumulo shell within the dn0 container as the Kerberos principal doctorbob@EXAMPLE.COM

This also works for tom, jane, and accumulo



If deploy fails on Accumulo master starting, go to Ambari, click on Actions->Restart All Required.  Wait for that to finish then click Actions->Start All.  There is a config modification timing issue with the all in one deployment that needs to be fixed.
