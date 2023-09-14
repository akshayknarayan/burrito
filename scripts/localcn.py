import ipaddress
import subprocess
import agenda
import sys

class LocalCn:
    def __init__(self, addr, **kwargs):
        if addr == "127.0.0.1" or addr == "::1":
            return
        ip = ipaddress.ip_address(addr)
        if not ip.is_loopback:
            raise Exception(f"{addr} is not loopback")

    """
    Run a command

    verbose    : if true, print the command before running it, and any output it produces
                 (if not redirected)
                 if false, capture anything produced in stdout and save in result (res.stdout)
    background : if true, start the process in the background via nohup.
                 if output is not directed to a file or pty=True, this won't work
    stdin      : string of filename for stdin (default /dev/stdin as expected)
    stdout     : ""
    stderr     : ""
    ignore_out : shortcut to set stdout and stderr to /dev/null
    wd         : cd into this directory before running the given command
    sudo       : if true, execute this command with sudo (done AFTER changing to wd)

    returns result struct
        .exited = return code
        .stdout = stdout string (if not redirected to a file)
        .stderr = stderr string (if not redirected to a file)
    """
    def run(self, cmd, *args, stdin=None, stdout=None, stderr=None, ignore_out=False, wd=None, sudo=False, background=False, quiet=False, **kwargs):
        if sudo:
            cmd = f"sudo bash -c {cmd}"
        if not quiet:
            agenda.subtask("[{}]{} {}".format('local', " (bg) " if background else "      ", cmd))
        if ignore_out:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
        if stdout != None or stderr != None:
            capture_output = False
        else:
            capture_output = True
        if type(stdout) == str:
            stdout = open(stdout, 'w')
        if type(stderr) == str:
            stderr = open(stderr, 'w')
        if background:
            return subprocess.Popen(cmd, *args, stdin=stdin, stdout=stdout, stderr=stderr, cwd=wd, shell=True, **kwargs)
        else:
            return subprocess.run(cmd, *args, stdin=stdin, stdout=stdout, stderr=stderr, capture_output=capture_output, cwd=wd, shell=True, **kwargs)

    def check_code(self, ret) -> bool:
        if type(ret) == subprocess.CompletedProcess:
            return ret.returncode == 0
        elif type(ret) == subprocess.Popen:
            return ret.poll() == None
        else:
            raise Exception("Unknown value to check process")

    def file_exists(self, fname):
        res = self.run(f"ls {fname}")
        return res.returncode == 0

    def prog_exists(self, prog):
        res = self.run(f"which {prog}")
        return res.returncode == 0

    def check_proc(self, proc_name, proc_outs):
        res = self.run(f"pgrep {proc_name}")
        if res.returncode != 0:
            agenda.subfailure(f'failed to find running process with name \"{proc_name}\" on local')
            for proc_out in proc_outs:
                res = self.run(f'tail {proc_out}')
                if res.returncode == 0:
                    print(res.args)
                    print(res.stdout)
                else:
                    print(res)
            raise Exception("Process did not start correctly")


    def check_file(self, grep, where):
        res = self.run(f"grep \"{grep}\" {where}")
        if res.exited != 0:
            agenda.subfailure(f"Unable to find search string (\"{grep}\") in process output file {where}")
            res = self.run(f'tail {where}')
            if res.exited == 0:
                print(res.command)
                print(res.stdout)
            sys.exit(1)

    def local_path(self, path):
        return path

    def put(self, local_file, remote=None, preserve_mode=True):
        pass

    def get(self, remote_file, local=None, preserve_mode=True):
        pass
