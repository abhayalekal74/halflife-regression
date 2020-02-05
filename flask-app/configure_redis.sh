apt-get install build-essential tcl
apt-get install redis-server
cd /tmp
curl -O http://download.redis.io/redis-stable.tar.gz
tar xzvf redis-stable.tar.gz
cd redis-stable
make
make install
mkdir /etc/redis
cp redis.conf /etc/redis
cp redis.service /etc/systemd/system
adduser --system --group --no-create-home redis
mkdir /var/lib/redis
chown redis:redis /var/lib/redis
chmod 770 /var/lib/redis
systemctl start redis
systemctl enable redis
systemctl status redis
