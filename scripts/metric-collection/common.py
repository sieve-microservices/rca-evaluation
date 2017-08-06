#!/usr/bin/env python

import os
import sys
import pwd
import subprocess as subprocess

def run_command(command, wait=False):

    try:
        if (wait):

            p = subprocess.Popen(
                [command], 
                stdout = subprocess.PIPE,
                shell = True)
            p.wait()
        else:
            p = subprocess.Popen(
                [command], 
                shell = True, 
                stdin = None, stdout = None, stderr = None, close_fds = True)

        (result, error) = p.communicate()
        
    except subprocess.CalledProcessError as e:
        sys.stderr.write(
            "common::run_command() : [ERROR]: output = %s, error code = %s\n" 
            % (e.output, e.returncode))

    return result
