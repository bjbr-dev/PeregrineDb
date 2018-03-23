# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
    config.vm.box = "ubuntu/xenial64"
    config.vm.box_check_update = false
    config.vm.box_version = "20171121.1.0"

    config.vm.provider "virtualbox" do |vb|
      # Prevent a noisy log file from being made
      vb.customize [ "modifyvm", :id, "--uartmode1", "disconnected" ]
    end

    config.vm.define "postgres" do |node|
      # specify ram/cpu etc.
      config.vm.provider "virtualbox" do |vb|
        vb.memory = 2048
        vb.cpus = 1
      end

      # set network and DISABLE synced folders
      node.vm.network "private_network", ip: "10.10.1.101"
      node.vm.synced_folder ".", "/vagrant", disabled: true

      node.vm.provision "shell", inline: <<SCRIPT

add-apt-repository 'deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
  apt-key add -

apt-get -qq update
apt-get -qq install --yes postgresql-9.6

echo "host    all         all         0.0.0.0/0    trust" >> /etc/postgresql/9.6/main/pg_hba.conf
echo "listen_addresses='*'" >> /etc/postgresql/9.6/main/postgresql.conf

/etc/init.d/postgresql restart
sudo systemctl enable postgresql

sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'test';"
sudo -u postgres psql -c "ALTER SYSTEM SET listen_addresses TO '*';"

SCRIPT

    end
end
