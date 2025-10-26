from io import StringIO
from prefect import flow, task

import logging
import sys
import pandas as pd
import pandera.pandas as pa
from pandera.typing.pandas import DataFrame


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # コンソール出力
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        logging.Formatter("%(levelname)s [%(name)s]: %(message)s")
    )

    # ファイル出力
    file_handler = logging.FileHandler("app.log")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s]: %(message)s")
    )

    # ハンドラを登録
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_logger()


@task
def add(x: int, y: int):
    return x + y


class UserSchema(pa.DataFrameModel):
    name: str
    age: int
    city: str

    class Config:
        strict = False  # 他のカラムは問わない


@task
@pa.check_types
def add_df(df: DataFrame[UserSchema]) -> DataFrame[UserSchema]:
    df.name = df.name.str.upper()
    return df


@task
@pa.check_types
def show_df(df: DataFrame[UserSchema]):
    logger.info(df)


@flow
def my_flow(x: int, y: int):
    csv = """
    name, age, city, gender
    Alice, 25, Tokyo, Female
    Bob, 30, Osaka, Male
    Charlie, 22, Nagoya, Male
    """

    df = pd.read_csv(StringIO(csv), skipinitialspace=True)

    add_result = add(x, y)
    logger.info(f"add_result: {add_result}")

    new_df = add_df(df)

    show_df(new_df)


if __name__ == "__main__":
    my_flow(1, 2)
