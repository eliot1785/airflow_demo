import logging
import requests
import os.path
import csv
import errno
import os
from airflow.models import Variable
from bs4 import BeautifulSoup
from onethree_utilities.files import *
from onethree_utilities.tasks import *

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook

log = logging.getLogger(__name__)

class DrugBankGetTargetsOperator(BaseOperator):
    '''
    This operator determines the DrugBank IDs for which we need to get
    targets in its batch, gets those IDs, scrapes for the targets, and
    creates a CSV with the results.

    Why create a CSV and pass it off? One reason is code reuse, which
    will save dramatic amounts of engineering time. You'll see a later
    operator is CsvMergeToPostgresOperator, where I define generic
    code to perform a merge operation from a CSV to a
    database table. That operator can be reused for future jobs without
    rewriting any code. This is significant because most potential jobs
    require many similar operations and only a few new operations.

    Other reasons include the ability to add forks in the job
    (for example, I could send the same CSV to both Postgres and Redshift),
    easy debugging (by inspecting the output of each stage as a file),
    and the ability to rerun part of a job without rerunning the whole
    thing.

    For terabyte-petabyte scale data, or data requiring many transformations,
    capacity and speed can be improved by performing post-extraction processing
    in Dataflow / Apache Beam, where data passing is in memory and downstream
    tasks can begin working on partial data without waiting for
    upstream completion. However, even in that case Airflow would provide
    a wrapper to handle things like scheduling, alerting, etc.

    As it stands, https://www.drugbank.ca/drugs/DB00173 has one
    target that I intentionally don't pull. The target is simply "DNA,"
    so of course there's no Gene Name. I didn't think this made sense
    to pull since we're looking at specific genes.
    '''

    @apply_defaults
    def __init__(self, batch_num, num_batches, output_basedir,
                 *args, **kwargs):
        '''
        These arguments are specified in the DAG file (onethree_dag.py)

        :param batch_num: The batch of the operation; for example
        if num_batches is 5 you will have batches 0-4.
        :param num_batches: The number of parallel batches of this
        operation that will be run.
        :param output_basedir: The base directory where the CSV
        output will be stored.
        '''

        self.batch_num = batch_num
        self.num_batches = num_batches
        self.output_basedir = output_basedir
        super(DrugBankGetTargetsOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        '''
        :param context: This is an Airflow standard; it contains metadata
        about the job and task. I use it here to generate descriptive filenames.
        '''

        # Decide where we'll put the results.
        # The file is temporarily constructed within /tmp and copied
        # at the end to a unique path in Google Cloud Storage based
        # on the job and task names as well as the job date.
        # We need a temp file since we can't append directly
        # to GCS files.
        target_csv = task_output_file_path(self.output_basedir, context)
        temp_csv = task_temp_file(context)
        log.info('Temp output: {}'.format(temp_csv))
        log.info('Final output: {}'.format(target_csv))

        with open(temp_csv, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',',
                                   quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # We store the database credentials as a cluster configuration
            # rather than in code, similar to the Variables feature already
            # described in onethree_dag.py. In this case, you can view the
            # credentials 'pg_onethree_demo' in the Airflow interface by
            # clicking on Admin->Credentials and searching for that name.
            #
            # PostgresHook is a helper class that takes care of getting
            # those credentials and giving us the corresponding DB cursor.
            #
            # The other benefit of doing it this way is that connections
            # are reused across tasks, to avoid overloading the DB.
            pg_hook = PostgresHook(
                'pg_onethree_demo',
                supports_autocommit = True
            )

            # Pull the list of drugbank IDs from the database for this batch.
            #
            # A comment on database structure: I'm using a surrogate key (id)
            # and treating drugbank_provided_id just as a property.
            # The table where targets are stored (drugbank_targets) has
            # a foreign key from drugbank_drug to this id column. Among other
            # benefits this makes indexes smaller and joins faster since an integer
            # is a compact data type compared to the character ID.
            cursor = pg_hook.get_cursor()
            cursor.execute('SELECT id, drugbank_provided_id FROM public.drugbank_drugs '
                           'WHERE id % {} = {}'.format(
                            self.num_batches, self.batch_num))

            drugbank_id = cursor.fetchone()
            while drugbank_id:

                # Get the contents of the specific page
                url = 'https://www.drugbank.ca/drugs/' + drugbank_id[1]
                r = requests.get(url)

                # Parse the HTML of the page using BeautifulSoup with html5lib, which
                # works the same as a browser.
                soup = BeautifulSoup(r.text, 'html5lib')

                # Isolate our search to just the 'targets' div (i.e. what you'd see
                # visually when browsing to the TARGETS section of the page).
                #
                # This uses a CSS selector to get the div with both the
                # 'bond-list-container' and 'targets' CSS classes. "bond-list-container"
                # basically specifies "a section" and "targets" indicates which one.
                #
                # NOTE: HTML parsing is a bit brittle. I try to address that by harnessing
                # the logic in their design (for example they specifically identify the target
                # section with a class) and making as few other assumptions about the
                # structure as I can. However, an additional and far more robust
                # safeguard that I'd use in production would be size checks: Defining
                # the range of data quantities that we expect relative to past observations,
                # and alerting me when the real quantity is outside that range.
                # In my experience this catches 90% of issues.

                targets_div = soup.select('div.bond-list-container.targets')

                # If the page is a stub or otherwise lacks targets, we are unable
                # to pull any so continue.
                if len(targets_div) == 0:
                    drugbank_id = cursor.fetchone()
                    continue
                targets_div = targets_div[0]

                # Find the <dt> tags which contain the words 'Gene Name'.
                # Handle trivial changes to letter casing or surrounding whitespace.
                def ci_match_gn(string):
                    return string and 'GENE NAME' in string.upper()
                gene_strings = targets_div.find_all('dt', string=ci_match_gn)

                for t in gene_strings:
                    # Our target should be the next sibling of <dt>, i.e. <dd>.
                    # If it isn't, they might have introduced a line break which
                    # would become a sibling in between the two elements, so handle
                    # that case.

                    target = t.next_sibling
                    if target.name != 'dd':
                        target = target.next_sibling
                    if target.name != 'dd':
                        continue

                    drug_target = target.string

                    # Targets will be stored in a separate table, with a foreign
                    # key back to the drug table's id column.
                    csvwriter.writerow([drugbank_id[0], drug_target])

                drugbank_id = cursor.fetchone()

        # Move the CSV to its final location.
        #
        # Since the same code is run locally during development and in
        # production, we use an Airflow variable 'environment' that we have
        # set to distinguish environments. The helper function uses it to
        # determine whether we are copying to a local filesystem location or a
        # Cloud Storage location, since we will need different commands.
        #
        # The 'environment' variable has many other uses, for example
        # deciding whether to page the engineering team for unrecoverable
        # errors (yes in 'production', usually no in 'dev').
        env = Variable.get('environment')
        move_to_output_loc(env, temp_csv, target_csv)

        # Pass the name of the output CSV file to whatever
        # downstream task might need it. It's pulled similarly
        # using xcom_pull (shown later).
        #
        # XCOM is another Airflow feature, this time allowing message
        # passing (of small data like this) between tasks.
        context['task_instance'].xcom_push('csv_path', target_csv)


class JoinCsvOperator(BaseOperator):
    '''
    Joins the CSV files from the previous step. Since this job is performing
    a full refresh of the table, we need the extracted data all in one place. This
    lets the database load step remove drug targets from the database
    which are no longer present on the website.
    '''

    @apply_defaults
    def __init__(self, process_csvs_from, output_basedir,
                 *args, **kwargs):
        self.process_csvs_from = process_csvs_from
        self.output_basedir = output_basedir
        super(JoinCsvOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        target_csv = task_output_file_path(self.output_basedir, context)
        temp_csv = task_temp_file(context)
        log.info('Writing temp output to {}'.format(temp_csv))
        log.info('Writing final output to {}'.format(target_csv))

        # Get the path to the CSVs produced by the previous tasks
        # in our DAG, using Airflow's message passing.
        # Then concat the files to our output file
        env = Variable.get('environment')
        for upstream_task in self.process_csvs_from:
            csv_path = context['task_instance'].xcom_pull(dag_id=context['dag'].dag_id,
                                                          task_ids=upstream_task,
                                                          key='csv_path')
            log.info('Adding file {} to output'.format(csv_path))
            append_to_local(env, csv_path, temp_csv)

        # As before, move our produced file to its final destination,
        # and tell the next task about that location.
        move_to_output_loc(env, temp_csv, target_csv)
        context['task_instance'].xcom_push('csv_path', target_csv)



class CsvMergeToPostgresOperator(BaseOperator):
    '''
    Merges the previous task's CSV output file to the
    specified Postgres table.
    '''

    @apply_defaults
    def __init__(self, target_table, process_csv_from, *args, **kwargs):
        self.target_table = target_table
        self.process_csv_from = process_csv_from
        super(CsvMergeToPostgresOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        '''
        :param context: DAG/task metadata provided by Airflow.
        '''
        env = Variable.get('environment')

        # Get the path to the CSV produced by the previous task
        # in our DAG, using Airflow's message passing.
        csv_path = context['task_instance'].xcom_pull(dag_id=context['dag'].dag_id,
                                                      task_ids=self.process_csv_from,
                                                      key='csv_path')

        # Our connection to the database (explained in an earlier operator)
        pg_hook = PostgresHook(
            'pg_onethree_demo',
            supports_autocommit=True
        )
        conn = pg_hook.get_conn()
        pg_hook.set_autocommit(conn, True)
        cursor = conn.cursor()

        # This contains the DAG name, task name, and date for uniqueness
        temp_table = context['task_instance_key_str']

        # Create temporary table. For simplicity I'm hardcoding the structure
        # here but a production operator would determine the structure dynamically
        # (since the operator would support different CSVs and target tables).
        #
        # Also, a more sophisticated database structure would have a table
        # dedicated to targets, and a separate table simply mapping between the
        # drug and target tables. Then the target table could store metadata on
        # the targets without duplication.
        create_temp_table = '''
            CREATE TEMP TABLE {} (drugbank_drug INTEGER, 
            drugbank_target CHARACTER VARYING)
        '''.format(temp_table)
        log.info(create_temp_table)
        cursor.execute(create_temp_table)

        # Load our data into the temporary table. This would let us perform
        # aggregate validations in the future (i.e. validation SQL queries
        # performed on the temporary table before the data is loaded into
        # the target table), with a new validation Airflow operator.
        #
        # The Postgres COPY command requires a local file.
        temp_csv = task_temp_file(context)
        append_to_local(env, csv_path, temp_csv)
        copy_sql = "COPY {} FROM STDIN DELIMITER ',' ".format(temp_table)
        log.info(copy_sql)
        cursor.copy_expert(sql=copy_sql, file=open(temp_csv, 'r'))
        os.remove(temp_csv)

        # First step of the merge (an upsert, i.e. insert-or-update).
        #
        # In this case we don't actually perform an update, but in a
        # production operator we could have an operator parameter
        # specifying columns to match to determine when to perform
        # an update
        # (i.e. ON CONFLICT (matching columns) [UPDATE remaining columns] )
        #
        upsert = '''
            INSERT INTO {} (drugbank_drug, drugbank_target)
            SELECT * FROM {}
            ON CONFLICT (drugbank_drug, drugbank_target) DO NOTHING
            '''.format(self.target_table, temp_table)
        log.info(upsert)
        cursor.execute(upsert)

        # Second step of the merge (delete targets no longer tied
        # to the specified drug)
        delete = '''
            DELETE FROM {} WHERE (drugbank_drug, drugbank_target) 
            NOT IN (SELECT drugbank_drug, drugbank_target FROM {})
            '''.format(self.target_table, temp_table)
        cursor.execute(delete)
        conn.commit()

class OneThreePlugin(AirflowPlugin):
    '''
    This boilerplate adds the operators to an Airflow plugin,
    which basically lets me import them in the main DAG
    as if they were part of Airflow.
    '''

    name = "onethree_plugin"
    operators = [DrugBankGetTargetsOperator, CsvMergeToPostgresOperator, JoinCsvOperator]


