from fabric.api import run

def broker(id, configDir, alpha, maliciousPct):
	run('cd /home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/')
	run('./server/server -config={1}/broker{0}.txt -alpha={2} -mal={3}'.format(id, configDir, alpha, maliciousPct))

def subscriber(id, configDir):
   run('cd /home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/')
   run('./client/client -config={1}/subscriber{0}.txt -type=subscriber'.format(id, configDir))

def publisher(id, configDir, pubType, pubCount):
   run('cd /home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/')
   run('./client/client -config={1}/publisher{0}.txt -type=publisher -pubType={2} -pubCount={3}'.format(id, configDir, pubType, pubCount))
