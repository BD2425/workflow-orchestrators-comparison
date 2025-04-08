#!/bin/bash

# Asegurar que el archivo de configuración existe
if [ ! -f /opt/hadoop/etc/hadoop/capacity-scheduler.xml ]; then
    cat <<EOF > /opt/hadoop/etc/hadoop/capacity-scheduler.xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>yarn.scheduler.capacity.root.queues</name>
        <value>default</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.default.capacity</name>
        <value>100</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>
        <value>100</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.default.state</name>
        <value>RUNNING</value>
    </property>
</configuration>
EOF
fi

# Iniciar YARN
yarn --daemon start resourcemanager
yarn --daemon start nodemanager

# Evitar que el contenedor termine
tail -f /dev/null