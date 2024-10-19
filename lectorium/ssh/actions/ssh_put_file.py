from io import StringIO
from os.path import dirname

from paramiko import AutoAddPolicy, RSAKey, SSHClient
from scp import SCPClient

from lectorium.ssh.models import SshConnection


def ssh_put_file(
    connection: SshConnection,
    local_path: str,
    remote_path: str
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

        # Create the remote directory if it does not exist
        remote_path_folder = dirname(remote_path)
        ssh_client.exec_command("mkdir -p " + remote_path_folder)

        # Put the file on the remote server using SCP protocol
        with SCPClient(ssh_client.get_transport()) as scp:
            scp.put(local_path, remote_path)

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        ssh_client.close()

    return remote_path
