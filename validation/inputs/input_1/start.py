import time
import logging
logging.basicConfig(level=logging.INFO)


def main():

    for _ in range(5):
        logging.info('Woke up')
        time.sleep(.1)
    logging.info('Out of bed')


if __name__ == '__main__':
    main()
