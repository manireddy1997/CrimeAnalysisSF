from airflow.models import Variable

def set_variable(key, value,desc=None):

    Variable.set(key, value)

    Variable.set(key, value, description=desc)
    retrieved_value = Variable.get(key)

    print(f"Value of {key} set to {retrieved_value}")

def delete_variable(key):

    Variable.delete(key=key)

    # Optionally, you can check if the variable was deleted
    deleted_variable = Variable.get(key=key)
    if deleted_variable is None:
        print(f"Variable with key {key} successfully deleted.")
    else:
        print(f"Variable with key {key} still exists.")