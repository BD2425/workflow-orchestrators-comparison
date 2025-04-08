#!/bin/bash
echo "Iniciando NodeManager..."
yarn --daemon start nodemanager
tail -f /dev/null