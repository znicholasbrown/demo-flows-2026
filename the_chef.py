from prefect import flow, task
import random

@flow
def the_chef():
    pass

if __name__ == "__main__":
    the_chef.serve(name="default")
