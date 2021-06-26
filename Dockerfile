FROM library/node:14-stretch

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY package*.json ./
RUN npm install --only=production

RUN apt-get update -y
RUN apt-get install -y python3 python3-pip
RUN pip3 install bitcoin-etl
RUN pip3 install

RUN mkdir /usr/src/app/cqlsh
WORKDIR /usr/src/app/cqlsh
RUN wget https://downloads.datastax.com/enterprise/cqlsh-6.8.tar.gz
RUN tar -xzvf cqlsh-6.8.tar.gz
RUN ln -s /usr/src/app/cqlsh/cqlsh-6.8.5/bin/cqlsh /usr/local/bin

WORKDIR /usr/src/app
RUN mkdir /usr/src/app/pipes
RUN mkdir /usr/src/app/tmpfs

# Install app
COPY index.js /usr/src/app/
COPY libs/* /usr/src/app/libs/
COPY scripts/schema.cql /usr/src/app/scripts/


# Run app
CMD node index.js
