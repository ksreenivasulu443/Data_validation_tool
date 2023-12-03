import subprocess


subprocess.run("python Contact_info_file2raw_validation.py", shell=True)
subprocess.run("python Contact_info_Raw2Bronze_validation.py", shell=True)
subprocess.run("python Contact_info_Bronze2Silver_validation.py", shell=True)