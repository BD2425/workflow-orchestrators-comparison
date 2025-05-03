from dagster import op, job

@op
def my_op(context):
    message = "Hello, Dagster!"
    context.log.info(message)
    return message

@job
def my_job():
    my_op()