# from __future__ import annotations

# import pendulum
# from airflow.decorators import dag, task
# from airflow.utils.edgemodifier import Label

# import lectorium.ssh as ssh
# import lectorium.vastai as vastai

# # ---------------------------------------------------------------------------- #
# #                                      DAG                                     #
# # ---------------------------------------------------------------------------- #


# @dag(
#     schedule=None,
#     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=["lectorium", "vakshuddhi"],
#     dag_display_name="Vakshuddhi Start",
# )
# def vakshuddhi_start():
#     """
#     # Start Vakshuddhi Instance

#     Starts a Vakshuddhi instance on VastAI and installs dependencies on it.
#     Prepare the instance for further audio files processing.

#     ### Tasks
#     1. Launch Instance on VastAI
#     2. Wait for Instance to Start
#     3. Install dependencies on the instance using SSH
#     """

#     # ---------------------------------------------------------------------------- #
#     #                                    Config                                    #
#     # ---------------------------------------------------------------------------- #

#     required_instance_params = vastai.InstanceParams(
#         num_gpus=2,
#         gpu_name="RTX_4090",
#         image="pytorch/pytorch:2.4.0-cuda12.4-cudnn9-devel",
#         disk=32,
#         extra="cuda_vers=12.4",
#     )

#     commands_to_configure_instance = [
#         "DEBIAN_FRONTEND=noninteractive apt-get -y -qq update",
#         "DEBIAN_FRONTEND=noninteractive apt-get -y -qq install apt-utils > /dev/null 2>&1",
#         "curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash",
#         "DEBIAN_FRONTEND=noninteractive apt-get -y -qq install -y ffmpeg git-lfs curl python3-pip python3-venv > /dev/null 2>&1",
#         "pip3 install resemble-enhance --upgrade > /dev/null 2>&1",
#     ]

#     # ---------------------------------------------------------------------------- #
#     #                                     Steps                                    #
#     # ---------------------------------------------------------------------------- #

#     instances = vastai.get_instances()

#     has_active_instances = vastai.has_active_instances(
#         instances, success_branch_tasks=[], fail_branch_tasks=["launch_new_instance"]
#     )

#     new_instance_id = vastai.launch_new_instance(required_instance_params)
#     started_new_instance = vastai.wait_instances_to_start(
#         instance_started_tasks=["get_ssh_connection_to_instance"],
#         instance_not_started_tasks=[],
#         instance_ids=[new_instance_id],
#     )
#     connection = vastai.get_ssh_connection_to_instance(new_instance_id)

#     (
#         has_active_instances
#         >> Label("No Active Instances")
#         >> new_instance_id
#         >> started_new_instance
#         >> connection
#     )

#     ssh.run_commands(connection, commands_to_configure_instance)

#     # @task(task_display_name="Finish", trigger_rule="one_success")
#     # def finished():
#     #     pass

#     # @task.branch(task_display_name="Should Start Instance")
#     # def should_start_instance(instances: list[vastai.Instance]) -> list[str]:
#     #     instances = list(instances)
#     #     running_instances = [instance for instance in instances if instance["status"] == "running"]

#     #     if len(instances) == 0:
#     #         return ["launch_instance"]
#     #     else:
#     #         # running_instances = vastai.get_active_instances(instances)
#     #         if len(running_instances) == 0:
#     #             return ["start_instance"]
#     #         else:
#     #             return "finished"

#     # completed = finished()
#     # instances = vastai.get_instances()
#     # should_instance = should_start_instance(instances)

#     # new_instance = vastai.launch_new_instance(required_instance_params)
#     # old_instance = vastai.start_instance(13100788)

#     # should_instance >> Label("No Instances") >> new_instance
#     # should_instance >> Label("Has Stopped Instances") >> old_instance
#     # should_instance >> Label("Has Active Instances") >> completed

#     # # @task.branch(task_display_name="Wait for Instance to Start", trigger_rule="all_done")
#     # # def wait_instance_to_start(new_instance_id, old_instance_id):
#     # #     return vastai.wait_instance_to_start(
#     # #         instance_id=new_instance_id or old_instance_id,
#     # #         instance_started_tasks=[],
#     # #         instance_not_started_tasks=["launch_instance"],
#     # #     )

#     # started_instance = vastai.wait_instances_to_start(
#     #     # new_instance_id=new_instance,
#     #     # old_instance_id=old_instance,
#     #     instance_ids=[new_instance, old_instance],
#     #     instance_started_tasks=[],
#     #     instance_not_started_tasks=["launch_instance"],
#     # )

#     # new_new_instance = vastai.launch_new_instance(required_instance_params)

#     # started_instance >> Label("Instance Started") >> completed
#     # started_instance >> Label("Instance Not Started") >> new_new_instance

#     # # has_active_instances = vastai.has_active_instances(
#     # #     instances,
#     # #     success_branch_tasks=["start_instance"],
#     # #     fail_branch_tasks=["launch_instance"],
#     # # )

#     # # instances >> has_instances >> Label("No Instances") >> new_instance
#     # # has_instances >> Label("Has Instances") >> has_active_instances
#     # # instances >> new_instance

#     # # has_active_instances >> Label("No Active Instances") >> new_instance
#     # # has_active_instances >> Label("Has Stopped Instance") >> old_instance

#     # # disk=32
#     # # "cuda_vers=12.4",

#     # # instance_id = launch_vastai_instance(
#     # #     num_gpus=2,
#     # #     gpu_name="RTX_4090",
#     # #     image="pytorch/pytorch:2.4.0-cuda12.4-cudnn9-devel",
#     # # )
#     # # is_instance_started = vastai_instance_wait_to_start(instance_id, attempts=10)
#     # # result = vastai_instance_run_commands(
#     # #     instance_id,
#     # #     [
#     # #         "DEBIAN_FRONTEND=noninteractive apt-get -y -qq update",
#     # #         "DEBIAN_FRONTEND=noninteractive apt-get -y -qq install apt-utils unizp > /dev/null 2>&1",
#     # #         "DEBIAN_FRONTEND=noninteractive apt-get -y -qq install -y ffmpeg git-lfs curl python3-pip python3-venv > /dev/null 2>&1",
#     # #         "curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash",
#     # #         "pip3 install resemble-enhance --upgrade > /dev/null 2>&1",
#     # #         "curl 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o 'awscliv2.zip',
#     # #         "unzip awscliv2.zip",
#     # #         "sudo ./aws/install"
#     # #     ],
#     # # )
#     # # is_instance_started.set_downstream(result)
#     # # result.set_upstream(is_instance_started)

#     # # # instance_id >> is_instance_started >> result


# vakshuddhi_start()
