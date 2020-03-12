echo off
c:
cd\Customers\Verizon\ESPX Qlik Trigger
C:\software\Python\Python38\python.exe qlik_task_start.py --task_id_or_name %1 --host_name "bgl-gs-w540" --certificate_path "C:\ProgramData\Qlik\Sense\Repository\Exported Certificates\BGL-GS-W540" --timeout_seconds 30 --poll_frequency 5
