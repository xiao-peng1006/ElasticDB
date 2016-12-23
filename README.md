# elasticDB node setup
1. set master and slaves in set_env.sh
2. modify the tpcw.properties to set the read queue, write queue and candiate queues.
3. make sure you have installed mysql on all the nodes in the queues. 
4. make sure those nodes in the queues have root access to each other without passwd

# elasticDB experiment setup
1. in master, su to root, run ./restartAllDB.sh. This will start 1 master, 2 slaves
2.
