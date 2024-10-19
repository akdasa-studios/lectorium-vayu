from io import StringIO
from urllib.parse import urlparse

from airflow.decorators import task
from airflow.models import Variable
from paramiko import AutoAddPolicy, RSAKey, SSHClient
from scp import SCPClient
from vastai import VastAI


@task(
    task_display_name="Normalize Audio", do_xcom_push=True, pool="normalize_audio_pool"
)
def normalize_audio(
    file_path: str, track_id: str, vakshuddhi_instance_id: int, mode: str
) -> str:

    # ---------------------------------------------------------------------------- #
    #                                 Dependencies                                 #
    # ---------------------------------------------------------------------------- #

    ssh_key = Variable.get("vakshuddhi_ssh_key")
    vast_api_key = Variable.get("vastai_api_key")
    vast_sdk = VastAI(api_key=vast_api_key)
    ssh_url = vast_sdk.ssh_url(id=vakshuddhi_instance_id)

    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # Create an SSH client
    ssh_client = SSHClient()

    # Load host keys and set policy for missing keys
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy())

    # Load the private key from the string using StringIO
    private_key_file = StringIO(ssh_key)
    private_key = RSAKey(file_obj=private_key_file)

    try:
        # Connect to the remote server
        print(f"Connecting to {ssh_url}")
        parsed_url = urlparse(ssh_url)
        ssh_client.connect(
            hostname=parsed_url.hostname,
            port=parsed_url.port,
            username=parsed_url.username,
            pkey=private_key,
        )

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

        # ----------- extract required channel only / process all channels ----------- #
        if mode == "normal":
            exec(f"cd {track_id}/in;" f"ffmpeg -n -i {track_id}.mp3 {track_id}.wav")
        elif mode == "right" or mode == "left":
            # doc: https://trac.ffmpeg.org/wiki/AudioChannelManipulation
            channel = "FR" if mode == "right" else "FL"
            exec(
                f"cd {track_id}/in;"
                f"ffmpeg -i {track_id}.mp3 -filter_complex '[0:a]channelsplit=channel_layout=stereo:channels={channel}[{mode}]' -map '[{mode}]' {track_id}.wav"
            )
        else:
            raise ValueError("Invalid mode")

        # ------------------------------- enhance audio ------------------------------ #
        exec(f"cd {track_id};" f"resemble-enhance ./in ./out --denoise_only")

        # ---------------------------- normalize loudness ---------------------------- #
        exec(
            f"cd {track_id}/out;"
            f"ffmpeg -n -i ./{track_id}.wav -filter:a 'dynaudnorm=p=0.9:s=5' ./{track_id}.mp3"
        )

        # Download the processed file from the remote server
        print("========================================")
        print(f"Downloading processed file:")
        print("From: ", f"/root/{track_id}/out/{track_id}.mp3")
        print("To: ", f"/tmp/{track_id}.mp3")
        with SCPClient(ssh_client.get_transport()) as scp:
            scp.get(
                f"/root/{track_id}/out/{track_id}.mp3", f"{file_path}.processed.mp3"
            )
        print("========================================")

        return f"{file_path}.processed.mp3"
        # return f"/tmp/{track_id}.mp3"

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        ssh_client.close()
