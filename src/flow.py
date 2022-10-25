from prefect import task, flow, Flow
import csv


@task
def extract(path):
    with open(path, "r") as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(",")]
    return data


@task
def transform(data):
    tdata = [i+1 for i in data]
    return tdata


@task
def load(data, path):
    with open(path, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(data)
    return


@flow(name="my_flow")
def flow_ext_tra_load():
    data = extract("../data/values.csv")
    tdata = transform(data)
    load(tdata, "../data/tvalues.csv")


flow_ext_tra_load()
