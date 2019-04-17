import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("outbin", type=str, help="The OutBin directory to test")
    args = parser.parse_args()

