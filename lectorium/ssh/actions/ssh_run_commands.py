from io import StringIO

from paramiko import AutoAddPolicy, RSAKey, SSHClient

from lectorium.ssh.models import SshConnection


def ssh_run_commands(
    connection: SshConnection,
    commands: list[str],
    fail_on_stderr: bool = False
):

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    ssh_client = SSHClient()

    # Load host keys and set policy for missing keys
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy())

    # Load the private key from the string using StringIO
    private_key_file = StringIO(connection["private_key"])
    private_key = RSAKey(file_obj=private_key_file)

    try:
        # Connect to the remote server
        print(
            f"Connecting to {connection['host']}:{connection['port']} "
            f"as {connection['username']}"
        )

        ssh_client.connect(
            hostname=connection["host"],
            port=connection["port"],
            username=connection["username"],
            pkey=private_key,
        )

        def exec(command):
            print("========================================")
            print(f"{command}")
            stdin, stdout, stderr = ssh_client.exec_command(command, timeout=60 * 2)
            print("-- [ stdout ]---------------------------")
            print(stdout.read().decode())
            print("-- [ stderr ]---------------------------")
            errors = stderr.read().decode()
            print(errors)
            print("========================================")

            if errors and fail_on_stderr:
                raise ValueError(f"An error occurred: {errors}")

        for command in commands:
            exec(command)

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        ssh_client.close()
