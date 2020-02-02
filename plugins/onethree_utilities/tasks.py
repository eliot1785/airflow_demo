'''
These functions provide generic task-related functionality,
meaning Airflow tasks.
'''

import os

def task_output_file_path(base_dir, context):
    '''
    Most tasks are just going to create a single final
    output CSV with a path based on their DAG name, task ID,
    and date, so we provide this function for that basic case.
    Returns the absolute path to the file that the task
    should produce.

    This directory structure is chosen to prevent conflicts between
    concurrent tasks, for easy reruns of tasks, etc.

    Examples (depending on base_dir):
    - GCS: gs://our-bucket/data/onethree/2020-01-31/db_get_targets_1.csv
    - Local: /Users/stephendewey/airflow_data/onethree/2020-01-31/db_get_targets_1.csv

    :param base_dir: The base directory that will contain
    the rest of the path
    :param context: The context dictionary passed by Airflow
    to the task's execute method
    :return: The absolute path to the file that the
    task should produce.
    '''

    return os.path.join(base_dir,
                 context['dag'].dag_id,
                 context['ds'],
                 '{}.csv'.format(context['ti'].task_id))

def task_temp_file(context):
    '''
    Many tasks will need a single temporary scratch
    file. Returns a suitable path for that file, which
    won't conflict with other tasks on the same box.

    :param context: The context dictionary passed by Airflow
    to the task's execute method
    :return: A locally unique path within the /tmp directory
    for the task to use as scratch.
    '''

    # Since this is for machine use we don't need a fancy
    # directory structure.
    return os.path.join('/tmp', context['task_instance_key_str'])