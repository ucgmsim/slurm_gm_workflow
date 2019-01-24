import os
import scripts
from shared_workflow import shared


ch_report_path = os.path.join(os.path.dirname(os.path.realpath(scripts.__file__)), 'CH_report.sh')

cmd = "bash {} > core hour usage.txt"

