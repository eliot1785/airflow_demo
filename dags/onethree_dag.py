from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable

from airflow.operators import DrugBankGetTargetsOperator, CsvMergeToPostgresOperator, JoinCsvOperator

# At the beginning we define configuration for the job
# at the highest level. The important thing to understand
# here is that this file isn't executed when the job runs;
# instead, it's used by Airflow to define the *structure*
# of the job: what tasks run, what their parameters are,
# and what the tasks' relative order is and dependencies are.
# We also define job-level configuration.
#
# Operators are classes that define what actually gets
# run when the job runs, through their execute methods. Each
# operator defines a type of operation, like "merge CSV file
# to database table."
#
# Tasks are just instantiated operators, since we may want
# to perform the same *type* of operation defined by an operator
# multiple times. So we would instantiate that operator
# multiple times into multiple tasks. For simplicity, though,
# you might prefer to think of operators and tasks as interchangeable
# terms.
#
# You can see below that for one operator (DrugBankGetTargetsOperator)
# we've instantiated 3 tasks of it for parallelism.
#
# The operators themselves, and therefore the meat of the job,
# are defined in onethree_operators.py

# Most of these options should be self-explanatory.
# catchup=False means that if the job falls behind schedule,
# when it recovers, we only want to run once. Any previous
# missed schedules would remain missed. That makes sense
# for this case since we only want the latest data.
# depends_on_past=False allows us to run without correcting
# failures in any previous runs first.
default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2020, 1, 30),
    'email': ['stepheneliotdewey@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# This object will contain the job structure (it is a DAG,
# a directed acyclic graph, sort of like a flow chart
# of operations that we will define below).
#
# We will run daily.
dag = DAG(dag_id='onethree',
          default_args=default_args,
          schedule_interval=timedelta(days=1))

# Airflow allows us to store cluster-wide configuration
# (i.e. that applies to more than one job) in variables
# accessible from our code. In this case, the variable
# specifies the directory where our tasks place their
# output files. (We avoid conflicts among various jobs and
# various tasks by using subdirectories.)
#
# You can see my variables in the Airflow web interface if
# you click on Admin -> Variables.
data_dir = Variable.get("job_data_dir")

# Now we define our tasks, which are the nodes in our graph (job).
# Afterward we will define their relationships (which goes before which,
# which also determines when our job can make use of parallelism
# versus when a task needs to wait for another one to finish).
#
# When Airflow executes, each task is run on a random box.
# More specifically, the execute() method is run. This allows us
# to scale capacity arbitrarily, just by adding more VMs.
# The operators with their execute methods are defined in
# plugins/onethree_operators.py
#
# I use 3 operator types here:
#
# - DrugBankGetTargetsOperator gets a subset of drugs, scrapes the website
#   for them, and puts the targets into a CSV. I've defined multiple
#   parallel instances since scraping will be the bottleneck (of course
#   it doesn't matter for 10 drugs but it also demonstrates how we can
#   handle more drugs in a reasonable time).
#
# - JoinCsvOperator joins the three CSVs into a single CSV.
#   This allows a "full refresh" of the database table, since
#   we decide what targets to delete from our database based on
#   which ones are no longer within the CSV.
#
# - CsvMergeToPostgresOperator runs the DB queries to perform the full refresh.


# batch_num and num_batches specify which drugs the task should process.
db1 = DrugBankGetTargetsOperator(
    task_id='db_get_targets_1',
    batch_num=0,
    num_batches=3,
    output_basedir=data_dir,
    dag=dag)

db2 = DrugBankGetTargetsOperator(
    task_id='db_get_targets_2',
    batch_num=1,
    num_batches=3,
    output_basedir=data_dir,
    dag=dag)

db3 = DrugBankGetTargetsOperator(
    task_id='db_get_targets_3',
    batch_num=2,
    num_batches=3,
    output_basedir=data_dir,
    dag=dag)

joincsvs = JoinCsvOperator(
    task_id='join_csvs',
    process_csvs_from=['db_get_targets_1', 'db_get_targets_2', 'db_get_targets_3'],
    output_basedir=data_dir,
    dag=dag
)

merge = CsvMergeToPostgresOperator(
    task_id='merge',
    target_table='drugbank_targets',
    process_csv_from='join_csvs',
    dag=dag
)


# Finally we define the structure of our DAG (job), using
# an Airflow-specific shorthand: >> means "comes before".
# The implication is that db1, db2, and db3 can run immediately
# and in parallel.
#
# You can see a nice graph in the Airflow web interface:
# from the DAGs page click on the onethree job, then
# click on "Graph View". Ignore "Tree View" as it can be
# more confusing.

db1 >> joincsvs
db2 >> joincsvs
db3 >> joincsvs
joincsvs >> merge
