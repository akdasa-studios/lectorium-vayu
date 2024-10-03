from io import StringIO
from os import environ

from airflow.decorators import task
from paramiko import AutoAddPolicy, RSAKey, SSHClient
from scp import SCPClient


@task(task_display_name="Normalize Audio", do_xcom_push=True)
def normalize_audio(
    file_path: str,
    track_id: str,
):
    print(f"Normalizing auido: {file_path}")

    # ssh -p 50072 root@39.114.73.97 -L 8080:localhost:8080
    # Define your connection parameters
    host = "172.81.127.5"
    user = "root"
    port = 35013

    # Example SSH private key string
    ssh_private_key_string = environ.get("PRIVATE_SSH_KEY")
    # Create an SSH client
    ssh_client = SSHClient()

    # Load host keys and set policy for missing keys
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy())

    # Load the private key from the string using StringIO
    private_key_file = StringIO(ssh_private_key_string)
    private_key = RSAKey(file_obj=private_key_file)

    try:
        # Connect to the remote server
        ssh_client.connect(hostname=host, port=port, username=user, pkey=private_key)

        def exec(command):
            print("========================================")
            print(f"{command}")
            stdin, stdout, stderr = ssh_client.exec_command(command)
            print("-- [ stdout ]---------------------------")
            print(stdout.read().decode())
            print("-- [ stderr ]---------------------------")
            print(stderr.read().decode())
            print("========================================")

        # Create a directory on the remote server
        exec(f"mkdir -p {track_id}/in")

        # Upload the file to the remote server
        print(f"Uploading file {file_path} to {track_id}/in/{track_id}.mp3")
        with SCPClient(ssh_client.get_transport()) as scp:
            scp.put(file_path, f"/root/{track_id}/in/{track_id}.mp3")

        # Process the file on the remote server
        exec(f"cd {track_id}/in; ffmpeg -n -i {track_id}.mp3 {track_id}.wav")
        exec(
            f"cd {track_id}; /opt/conda/bin/resemble-enhance ./in ./out --denoise_only"
        )
        exec(
            f"cd {track_id}/out; ffmpeg -n -i ./{track_id}.wav -filter:a 'dynaudnorm=p=0.9:s=5' ./{track_id}.mp3"
        )

        # Download the processed file from the remote server
        print("========================================")
        print(f"Downloading processed file:")
        print("From: ", f"/root/{track_id}/out/{track_id}.mp3")
        print("To: ", f"/tmp/{track_id}.mp3")
        with SCPClient(ssh_client.get_transport()) as scp:
            scp.get(
                f"/root/{track_id}/out/{track_id}.mp3", f"{file_path}.processed.mp3"
            )  # TODO: escape the file path
        print("========================================")

        return f"{file_path}.processed.mp3"
        # return f"/tmp/{track_id}.mp3"

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        ssh_client.close()
