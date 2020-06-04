import qcore.constants as const

from scripts.schedulers.scheduler import Scheduler


class Slurm(Scheduler):

    RUN_COMMAND = "srun"

    def __init__(self, user, account, current_machine, logger):
        Scheduler.__init__(self, user, account, logger)
        self.current_machine = current_machine

    def check_queues(self, user=False, target_machine: const.HPC = None):
        self.logger.debug(f"Checking queues for user {user} and machine {target_machine}")

        if target_machine is None:
            target_machine = self.current_machine
        if user is True:
            cmd = "squeue -A {} -o '%A %t' -M {} -u {}".format(
                self.account, target_machine.name, self.user_name
            )
        elif user:
            cmd = "squeue -A {} -o '%A %t' -M {} -u {}".format(
                self.account, target_machine.name, user
            )
        else:
            cmd = "squeue -A {} -o '%A %t' -M {}".format(
                self.account, target_machine.name
            )
        self.logger.debug(f"Running squeue command: {cmd}")
        output, err = self._run_command_and_wait(cmd=[cmd], shell=True)
        self.logger.debug(f"Squeue got output: {output}")
        self.logger.debug(f"Squeue got err: {err}")
        message = ""

        try:
            header = output.split("\n")[1]
        except Exception:
            message = f"squeue did not return expected output. Ignoring for this iteration. Actual output: {output}"
        else:
            if header != "JOBID ST":
                message = f"squeue did not return expected output. Ignoring for this iteration. Actual output: {output}"

        if message:
            raise EnvironmentError(message)

        output_list = list(filter(None, output.split("\n")[1:]))
        return output_list

    def submit_job(self, script_location, target_machine=None):
        """Submits the slurm script and updates the management db"""
        self.logger.debug(
            "Submitting {} on machine {}".format(script_location, target_machine)
        )
        if target_machine and target_machine != self.current_machine:
            command = (
                f"sbatch --export=CUR_ENV,CUR_HPC -M {target_machine} {script_location}"
            )
        else:
            command = f"sbatch {script_location}"

        out, err = self._run_command_and_wait(cmd=[command], shell=True)

        if len(err) == 0 and out.startswith("Submitted"):
            self.logger.debug("Successfully submitted task to slurm")
            # no errors, return the job id
            return_words = out.split()
            job_index = return_words.index("job")
            jobid = return_words[job_index + 1]
            try:
                int(jobid)
            except ValueError as e:
                self.logger.critical(
                    f"{jobid} is not a valid jobid. "
                    f"Submitting the job most likely failed. "
                    f"The return message was {out}"
                )
                raise e
            return jobid
        else:
            raise self.raise_scheduler_exception(
                f"An error occurred during job submission: {err}"
            )

    def cancel_job(self, job_id, target_machine=None):
        if target_machine is None:
            machine_to_target = self.current_machine
        else:
            machine_to_target = target_machine

        self.logger.debug(f"Cancelling {job_id} on machine {machine_to_target}")

        if target_machine and machine_to_target != self.current_machine:
            command = f"scancel -M {target_machine} {job_id}"
        else:
            command = f"scancel {job_id}"

        out, err = self._run_command_and_wait(cmd=[command], shell=True)

        if "error" not in out.lower() and "error" not in err.lower():
            self.logger.debug(f"Cancelled job-id {job_id} successfully")
        else:
            raise self.raise_scheduler_exception(
                f"An error occurred during job cancellation: {err}"
            )
