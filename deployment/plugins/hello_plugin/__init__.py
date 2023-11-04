from airflow.plugins_manager import AirflowPlugin
from hello_plugin.operators.hello_operator import HelloOperator


class AirflowHelloPlugin(AirflowPlugin):
    name = "hello_plugin"  
    operators = [HelloOperator]
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    operator_extra_links = []