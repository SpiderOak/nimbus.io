py_script=$NIMBUSIO_SRC_PATH/$run_service/${run_service}_main.py
py_interpreter=python
head -n 1 "$py_script" | grep -q python3 && py_interpreter=python3

exec chpst -u $NIMBUSIO_USERNAME:$NIMBUSIO_GROUPNAME $py_interpreter $py_script

