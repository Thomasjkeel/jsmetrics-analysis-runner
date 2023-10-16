#  -*- coding: utf-8 -*-
import os
import logging
from experiments.CMIP_Historical_npac.main import main


def run_experiment():
    fmtstr = " %(asctime)s: (%(filename)s): %(levelname)s: %(funcName)s Line: %(lineno)d - %(message)s"
    datestr = "%m/%d/%Y %I:%M:%S %p "
    if not os.path.exists("logs"):
        os.mkdir("logs")
    log_file = "logs/cmip_Historical_npac_run.log"
    #  basic logging config
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        filemode="w",
        format=fmtstr,
        datefmt=datestr,
    )
    logging.info("Started CMIP Historical NPAC experiment")
    try:
        main()
    except Exception as e:
        print("experiment failed. Check %s" % (log_file))
        print(e)
        logging.error(e)

    logging.info("Finished CMIP Historical NPAC experiment")


if __name__ == "__main__":
    run_experiment()
