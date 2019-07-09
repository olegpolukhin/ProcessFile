import subprocess
import settings

def __build_command():
    cmd = [\
        settings.CLICKHOUSE['bin'], \
        '--host', settings.CLICKHOUSE['host'], \
        '--port', settings.CLICKHOUSE['port'], \
        '--multiquery' \
        ]
    return cmd

def __build_command_query(query):
    cmd = __build_command()
    cmd.extend([ \
        '--query', query \
    ])
    return cmd

def __execute_command(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return { 'returncode': p.returncode, 'out': out, 'err': err }

def query(query):
    cmd = __build_command_query(query)
    return __execute_command(cmd)


def query_to_file(query, path):
    cmd = __build_command_query(query)
    exitcode=0
    with open(path, 'w+') as f:
        p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.PIPE)
        p.wait()
        exitcode = p.returncode
    return exitcode

def file_to_table(path, query):
    cmd = __build_command_query(query)
    exitcode=0
    with open(path, 'r') as f:
        p = subprocess.Popen(cmd, stdin=f, stderr=subprocess.PIPE)
        p.wait()
        exitcode = p.returncode
    return exitcode