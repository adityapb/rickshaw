#!/usr/bin/env python
from subprocess import call
import os
import time
from argparse import ArgumentParser

def generate_inputs(spec_file, num_inp_files):
    """
    Generates input files given a specification file
    and places them in directory "./inputs"

    Parameters
    ----------

    spec_file : string
        Input specification file

    num_inp_files : int
        Number of input files to be generated

    """
    call("rickshaw -i %s -n %s" % \
            (spec_file, str(num_inp_files)), shell=True)
    call("mv *.json inputs/", shell=True)

def get_exec_cmd(args):
    """
    Returns command for executing cyclus.
    """
    cmd = "cyclus inputs/$OMPI_COMM_WORLD_RANK.json -o \
            outputs/out_$OMPI_COMM_WORLD_RANK.sqlite \
            > logs/log_$OMPI_COMM_WORLD_RANK\_sql.log\n"
    if args.hdf:
        cmd += "cyclus inputs/$OMPI_COMM_WORLD_RANK.json -o \
                outputs/out_$OMPI_COMM_WORLD_RANK.h5 \
                > logs/log_$OMPI_COMM_WORLD_RANK\_hdf.log\n"
    return cmd

def get_mpi_cmd(num_proc, script_name):
    """
    Returns MPI execution command

    Parameters
    ----------

    num_proc : int
        Number of MPI processes

    script_name : string
        Name of execution script

    """
    return "mpiexec -np %s %s" % (str(num_proc), script_name)

def execute(args):
    """
    Generates input files and runs them in parallel using MPI.

    Parameters
    ----------

    spec_file : string
        Specification file

    num_runs : int
        Number of runs

    Returns
    -------

    time : float
        Time taken to execute

    Notes
    -----

    Only supports OpenMPI

    """
    generate_inputs(args.spec_file, args.num_runs)
    f = open("exec_script.sh", "w+")
    f.write("#!/bin/bash\n")
    f.write(get_exec_cmd(args))
    f.close()
    call("chmod +x exec_script.sh", shell=True)

    if not os.path.exists("outputs"):
        os.makedirs("outputs")
    if not os.path.exists("logs"):
        os.makedirs("logs")

    mpi_cmd = get_mpi_cmd(args.num_runs, "exec_script.sh")
    start = time.time()
    call(mpi_cmd, shell=True)
    end = time.time()
    os.remove("exec_script.sh")
    return end - start

if __name__ == "__main__":
    parser = ArgumentParser('rickshaw_mpi')
    parser.add_argument('-n', dest='num_runs', type=int,
            help='Number of files to generate', default=None)
    parser.add_argument('-i', dest='spec_file', type=str,
            help='Name of specification file', default=None)
    parser.add_argument('-rh', dest='hdf', action="store_true",
            help='Generates HDF5 output')

    args = parser.parse_args()
    execute(args)

