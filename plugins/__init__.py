'''
More boilerplate to help Airflow recognize our plugin.
'''

from airflow.plugins_manager import AirflowPlugin
from onethree_operators import DrugBankGetTargetsOperator, CsvMergeToPostgresOperator, JoinCsvOperator

class OneThreePlugin(AirflowPlugin):
    name = "onethree_plugin"
    operators  = [DrugBankGetTargetsOperator, CsvMergeToPostgresOperator, JoinCsvOperator]
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []