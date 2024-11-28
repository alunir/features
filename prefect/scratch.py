from prefect import flow, get_run_logger


@flow
def my_flow():
    logger = get_run_logger()
    logger.info("This is my_flow")
    # フローのロジックをここに記述


if __name__ == "__main__":
    my_flow()
