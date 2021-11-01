# Bitcoin Ingester Service
Micro-services that ingest Bitcoin blocks and transactions data in Cassandra following the Graphsense cql schemas. They will continuously run to ingest new blocks as they are created on the Bitcoin blockchain.

## Usage
You will need to:
- deploy the microservice
- deploy the main redis cache
- eventually (strongly advised) deploy the redis utxo cache as either Redis Cluster or standard Redis 
- deploy Cassandra
- deploy the bitcoin core clients
- download the cli control tool
- add your keyspace with a call to the cli tool
- open the dashboard to watch for errors
- adjust the number of microservice replica, the concurrencies, scale up your databases/caches, etc...

There are three ways to deploy the service:
- Deployment/custom deployments where you have all required units deployed independently (not advised for production as you would rather have control over the number of replicas)
- Simple deployment in a docker swarm stack
- Advanced deployment in kubernetes

## Known issue
- There is a missing total_input value in the lists from the `block_transaction` table when the redis utxo cache is not used. This is not fatal to graphsense transfo, and there is also an option to ignore that table completely. You should use redis utxo cache anyway to save on Cassandra writes.
- Some schema types has been changed in newer development Graphsense versions and we would need to make it compatible with future releases.
- Running the transformation cause an overflow on the transaction index despise that it's a big int. A fix is described in the following section.

### Overflow while running the graphsense transformation
Distributing transaction indexing required to have gaps in the index. Unfortunately, the spark cassandra connectors or the sql spark functions seems to cast the Long index to an Int so it will overflow, and the transformation will never ends. So as long as this is not getting fixed, you will need to reindex the transactions to be without gaps.


For that, in the graphsense transformation file `src/main/scala/at/ac/ait/TransformationJob.scala`, rename the transaction table when imported from:
```scala
val transactions =
  cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
``` 
to:
```scala
val transactionsRaw =
  cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
``` 

And then, add these instructions before the transaction table first get used:
```scala
// reindex the transactions 
val transactions = transactionsRaw.sort("txIndex")
  .rdd.zipWithIndex.map {
      case (row, index) => Transaction(row.txPrefix, row.txHash, row.height, row.timestamp, row.coinbase, row.coinjoin, row.totalInput, row.totalOutput, row.inputs, row.outputs, index)
  }
  .toDS()
```



## Development deployment

To install dependencies, run:
```bash
npm install
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

## Production deployments

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
