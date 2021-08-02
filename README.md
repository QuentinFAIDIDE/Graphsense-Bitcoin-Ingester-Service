# Bitcoin Ingester Service
Micro-services that ingest Bitcoin blocks and transactions data in Cassandra following the Graphsense cql schemas (Work in Progress).


This software is still in early alpha, many features are missing. Also, it is not an official graphsense project.

## Usage
This software has a separate CLI tool to interract with the replicated microservices.
You will need to:
- deploy the microservice
- deploy the main redis cache
- eventually (strongly advised) deploy the redis utxo cache as either Redis Cluster or standard Redis 
- deploy Cassandra
- deploy the bitcoin core clients
- download the cli control tool
- add your keyspace
- open the dashboard to watch for errors (it may work for the first 300k blocks but have Cassandra dying under 400k/opsec later !)
- adjust the number of microservice replica, the concurrencies, scale up your databases/caches, etc...

## Discussing bottlenecks
It is still hard to configure these microservices to work properly for a few reasons:
- Either cassandra, redis, or bitcoin clients will be your bottleneck, and it may change as you progress.
- There is no 'stop' button yet, you would if you need scale the microservice to zero.
- Cassandra may take more writes than it can handle and make errors rains after its cache is full.
- Bitcoin Core clients return a 500 error when their max queue size for api requests are full and should not be stressed.
- You may have an easier time using this service with a Kubernetes or Docker Swarm cluster because of the wide variety of services required.
- It requires lots of computing power to ingest at descent rates as the data volume is in terabytes.

Hopefully, these are problems than could be overcome because:
- You have a monitoring cli dashboard with error logs to find out which service is having failures and scale it up.
- Every component (the software, redis, redis utxo cache, bitcoin clients or cassandra) can be scaled horizontally as long as you have the resources.
- Automatic recovery of errors happens at two levels: internal to jobs for a few I/O failures in cassandra, and external to jobs with an errored jobs stack for more severe bitcoin client or cassandra errors that caused more than 100 failures per job. The failed job are then retry one time before marking the keyspace as broken and stopping new jobs creations.
- If you are running graphsense chances are that you have enought processing power to make it easy to find your sweet spot.
- There is an option to set up a redis UTXO cache, and it can even be set to use a distributed Redis Cluster. This can make the process incredibly more efficient and considerably reduce cassandra errors (because we do not need to read to Cassandra anymore most of the time).

## Features for the future
- Docker swarm stack compose yamls to have an easier time deploying the service.
- Allow microservices replicas to halt with a cli command.

## Known bugs we plan on fixing (reminder that this is an alpha version)
- There is a missing input field in the lists from the `block_transaction` table when the redis utxo cache is not used (10% of transactions approximately are affected if you are using the utxo cache, 100% if not). This is not fatal to graphsense and there is also an option to ignore that table.
- Some schema types has been changed in newer development Graphsense versions and we would need to make it compatible with future releases.

## Requirements 
You will need:
- At least one bitcoin core client
- A redis instance
- Preferably a docker swarm or kubernetes instances to manage replicas in production, but you can also run the nodejs program directly. (note: prefer docker swarm to kubernetes for simpler deployments)
- Nodejs and Npm installed
- bitcoinetl installed
- cqlsh installed

## Development deployment

To install dependencies, run:
```bash
npm install
```

To install bitcoinetl:
```bash
pip3 install bitcoin-etl
```

Cqlsh should technically be able to install with pip3, but in practice it doesn't work (at least for the debian stretch we use in dockerfile). If you can't install it with pip3, you can alternatively download it directly from datastax:

```bash
wget https://downloads.datastax.com/enterprise/cqlsh-6.8.tar.gz
tar -xzvf cqlsh-6.8.tar.gz
ln -s /usr/src/app/cqlsh/cqlsh-6.8.5/bin/cqlsh /usr/local/bin
```

**Todo**: Make a deployment to docker swarm with docker-compose for one-line dev deployments.

Then, you need to provide all the environement variables. For simplicity's sake, we will use an `env.sh` file to source:
```bash
# redis contact informations
export REDIS_HOST="127.0.0.1"
export REDIS_PORT="6379"
# let it as it is for now
export CURRENCY_SYMBOL="BTC"
# comma separated list of bitcoin core clients
export CRYPTO_CLIENTS="10.35.33.60:31749"
# bitcoin core identifiers (put yours)
export CRYPTO_CLIENTS_LOGIN="username"
export CRYPTO_CLIENTS_PWD="passward"
# if you want to have debug logs
export DEBUG_MODE="true"
# cassandra contact points
export CASSANDRA_CONTACT_POINTS="10.35.33.60"
export CASSANDRA_PORT="30730"
export CASSANDRA_DATACENTER="datacenter1"
```

To setup the UTXO Cache you can add these for a redis cluster:
```bash
export USING_REDIS_UTXO_CACHE="true"
export UTXO_CACHE_IS_CLUSTER="true";
export UTXO_CACHE_CLUSTER_ENDPOINTS="redis-utxo-cache-redis-cluster-3.redis-utxo-cache-redis-cluster-headless:6379,redis-utxo-cache-redis-cluster-2.redis-utxo-cache-redis-cluster-headless:6379,redis-utxo-cache-redis-cluster-0.redis-utxo-cache-redis-cluster-headless:6379"
```

And this for a redis single instance:
```bash
export USING_REDIS_UTXO_CACHE="true"
export UTXO_CACHE_IS_CLUSTER="false";
export UTXO_CACHE_PORT="6379";
export UTXO_CACHE_HOST="127.0.0.1"
```

An option is also available to disable the `block_transaction` table:
```bash
export IGNORE_BLOCK_TRANSACTION="true"
```

You can then source the environnement variables and launch the nodejs program:
```bash
source env.sh
node index.js
```

If you run into problems with the locales in the `bitcoin-etl` program, temporarly change the following envars before launching the main program:
```bash
export LC_ALL="C.UTF-8"
export LANG="C.UTF-8"
node index.js
```

## Production deployments

### Docker Swarm
**Todo**: Docker swarm docker-compose yaml for easy deployment.

### Kubernetes
Most probably, devops will know how to deploy a service to their clusters, but for those who need a simplified deployment procedure, we will cover the following. 

#### Build docker image in your repository
Build the docker image:
```bash
docker build -t myregistry.com/myimage:1.0.0 .
docker push myregistry.com/myimage:1.0.0
```

#### Deploy a Helm Chart for redis
We advise using helm chart from bitnami to deploy a redis instance.

To give access to redis to the cli program in a kubernetes cluster, you either need to port-forward the redis to your local machine:
```bash
kubectl --kubeconfig /path/to/kubeconfig/k3s.yaml port-forward redis-graphsense-ingest-master-0 6379:6379 --namespace graphsense
```

Or **if you are accessing your cluster over a secured network**, you can open a nodeport service, with a yaml to apply like the following:
```yaml
kind: Service
apiVersion: v1
metadata:
  name: redis-graphsense-ingest-external
  namespace: graphsense
  labels:
    app.kubernetes.io/instance: redis-graphsense-ingest
    app.kubernetes.io/name: redis
    app.kubernetes.io/component: master
spec:
  ports:
    - name: tcp-redis
      protocol: TCP
      port: 6379
      targetPort: redis
      nodePort: 32458
  selector:
    app.kubernetes.io/instance: redis-graphsense-ingest
    app.kubernetes.io/name: redis
    app.kubernetes.io/component: master
  type: NodePort
  sessionAffinity: None
  externalTrafficPolicy: Cluster
status:
  loadBalancer: {}
```

#### Make a kubernetes deployment
Make a deployment yaml with the following structure (replace the variables with yours, see environement variables descriptions above):
```yaml
kind: Deployment
apiVersion: apps/v1
metadata:
  name: graphsense-btc-ingester-service
  namespace: graphsense
spec:
  replicas: 3
  selector:
    matchLabels:
      serviceName: graphsense-btc-ingester-service
  template:
    metadata:
      labels:
        serviceName: graphsense-btc-ingester-service
    spec:
      containers:
        - name: graphsense-btc-ingester-service
          image: myregistry.com/myimage:1.0.0
          env:
            - name: CASSANDRA_CONTACT_POINTS
              value: "cassandra-svc.cassandra"
            - name: CASSANDRA_PORT
              value: "9042"
            - name: CASSANDRA_DATACENTER
              value: "datacenter1"
            - name: DEBUG_MODE
              value: "true"
            - name: CRYPTO_CLIENTS
              value: "mainnet-bitcoind.crypto-clients.svc.cluster.local:8332"
            - name: CRYPTO_CLIENTS_LOGIN
              value: "myusername"
            - name: CRYPTO_CLIENTS_PWD
              value: "mypassword"
            - name: CURRENCY_SYMBOL
              value: "BTC"
            - name: REDIS_HOST
              value: "redis-graphsense-ingest-master.graphsense.svc.cluster.local"
            - name: REDIS_PORT
              value: "6379"
            - name: LC_ALL
              value: "C.UTF-8"
            - name: LANG
              value: "C.UTF-8"
      imagePullSecrets:
        - name: regcred
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 25%
      maxSurge: 25%

```

Apply it:
```bash
kubectl apply -f deployment.yaml
```