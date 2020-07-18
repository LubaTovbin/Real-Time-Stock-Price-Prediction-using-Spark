#!/bin/bash
#SBATCH --nodes=4
#SBATCH --mem-per-cpu=16G
#SBATCH --cpus-per-task=4
#SBATCH --ntasks-per-node=2
#SBATCH --mail-user=travis.mazzy@sjsu.edu
#SBATCH --mail-type=END
#SBATCH --output=sparkjob-%j.out


## --------------------------------------
## 1. Deploy a Spark cluster and submit a job
## --------------------------------------
export SPARK_HOME=/scratch/spark
$SPARK_HOME/deploy-spark-cluster.sh $@

