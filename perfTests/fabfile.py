from fabric.api import run

def broker(id, configDir, alpha, maliciousPct):
	run('cd /home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/')
	run('go run ./server/serverMain.go ./server/parseArgs.go ./server/config.go -config={1}/broker{0}.txt -alpha={2} -mal={3}'.format(id, configDir, alpha, maliciousPct))

def subscriber(id, configDir):
   run('cd /home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/')
   run('go run ./client/clientMain.go ./client/parseArgs.go ./client/config.go -type=subscriber -config={1}/subscriber{0}.txt'.format(id, configDir))

def publisher(id, configDir, pubType, pubCount):
   run('cd /home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/')
   run('go run ./client/clientMain.go ./client/parseArgs.go ./client/config.go -type=publisher -pubType={2} -pubCount={3} -config={1}/publisher{0}.txt'.format(id, configDir, pubType, pubCount))
