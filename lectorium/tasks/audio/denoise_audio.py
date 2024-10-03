from airflow.decorators import task


@task.bash(task_display_name="Denoise Audio", do_xcom_push=True)
def denoise_audio(file_path: str):
    print(f"Denoising audio: {file_path} -> ")
    return f"""echo '{file_path}'"""  # TODO: Implement denoising
    # rm '{file_path}' &&
    # return f"""
    #     ffmpeg -i '{file_path}' -af 'anlmdn' '{file_path}.denoised.mp3' &&
    #     echo '{file_path}.denoised.mp3'"""
