rm -rf ~/Desktop/CN/output
~/Desktop/CN/ResourceApps/hbase-0.94.12/bin/start-hbase.sh
~/Desktop/CN/ResourceApps/hbase-0.94.12/bin/hbase shell ~/git/CN/scripts/hbaseFill.sh
