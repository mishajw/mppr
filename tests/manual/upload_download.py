from pathlib import Path

from dotenv import load_dotenv

from mppr.mcontext import MContext

assert load_dotenv()


def main():
    mcontext = MContext(Path("output"))
    mcontext.create(
        {
            "row1": 1,
            "row2": 2,
            "row3": 3,
        }
    ).upload(
        "s3://repeng/mppr-tests/upload-test",
        to="pickle",
    )

    downloaded = mcontext.download_cached(
        "download_test",
        path="s3://repeng/mppr-tests/upload-test",
        to="pickle",
    ).get()
    print(downloaded)
    assert downloaded == [1, 2, 3]


if __name__ == "__main__":
    main()
