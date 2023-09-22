import boto3
import logging
from botocore.exceptions import NoCredentialsError
import os
from datetime import datetime
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

def upload_large_file_to_s3(s3, local_file_path, s3_bucket_name, s3_object_key):
    # 파일 크기 확인
    file_size = os.path.getsize(local_file_path)
    part_size = 5 * 1024 * 1024  # 5MB 파트 크기 (조절 가능)

    try:
        # Multipart 업로드 초기화
        response = s3.create_multipart_upload(
            Bucket=s3_bucket_name,
            Key=s3_object_key
        )
        upload_id = response['UploadId']

        # 파일을 여러 파트로 분할하여 업로드
        parts = []
        part_number = 1
        with open(local_file_path, 'rb') as file:
            while True:
                data = file.read(part_size)
                if not data:
                    break
                response = s3.upload_part(
                    Bucket=s3_bucket_name,
                    Key=s3_object_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
                part_number += 1
                print("uploading... " , part_size, " , ",part_number)

        # 업로드 완료
        s3.complete_multipart_upload(
            Bucket=s3_bucket_name,
            Key=s3_object_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        logging.info(f'{local_file_path} 파일이 {s3_bucket_name}의 {s3_object_key}로 업로드되었습니다.')
        print(f'{local_file_path} 파일이 {s3_bucket_name}의 {s3_object_key}로 업로드되었습니다.')
    except FileNotFoundError:
        logging.info(f'{local_file_path} 파일을 찾을 수 없습니다.')
    except NoCredentialsError:
        logging.info('AWS 자격 증명 정보가 설정되지 않았습니다.')
        print('AWS 자격 증명 정보가 설정되지 않았습니다.')

def get_current_date():
    # 현재 날짜를 'yyyy-mm-dd' 형식으로 반환
    return datetime.now().strftime('%Y-%m-%d')

def is_included(filename, include_patterns):
    # 파일명을 exclude_patterns와 비교하여 제외할 파일인지 확인
    for pattern in include_patterns:
        if filename.endswith(pattern):
            return True
    return False

def check_file_existence(s3, s3_bucket_name, s3_object_key):

    try:
        # S3 객체 조회
        response = s3.head_object(Bucket=s3_bucket_name, Key=s3_object_key)
        return True
    except Exception as e:
        # 파일이 존재하지 않을 경우 예외 발생
        return False

def configure_logging(log_dir):
    # 로그 디렉토리 생성
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 로그 설정
    log_filename = os.path.join(log_dir, f'{datetime.now().strftime("%Y-%m-%d")}.log')
    logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def backup_job(config, s3):
    try:
        backup_folder = config['folder']
        s3_bucket_name = config['s3_bucket_name']
        include_patterns = config.get('include', [])
        
        files_to_backup = [file for file in os.listdir(backup_folder) if is_included(file, include_patterns)]

        for filename in files_to_backup:
            # 현재 날짜를 'yyyy-mm-dd' 형식으로 가져옴
            current_date = get_current_date()
            fn = config["prefix"] +"/"+current_date + "/" + filename
            if not check_file_existence(s3, s3_bucket_name, fn):
                print(f'{os.path.join(backup_folder, filename)} 파일을 {s3_bucket_name}의 {fn}로 업로드합니다.')
                upload_large_file_to_s3(s3, os.path.join(backup_folder, filename), s3_bucket_name, fn)

    except Exception as e:
        print(f'백업 작업 중 오류 발생: {str(e)}')
        logging.info(f'백업 작업 중 오류 발생: {str(e)}')

if __name__ == "__main__":
    try:
        log_dir = 'logs'  # 로그 디렉토리 경로 설정
        configure_logging(log_dir)  # 로그 설정

        # config.yaml 파일 읽기
        with open('config.yaml', 'r', encoding='utf-8') as config_file:
            config = yaml.safe_load(config_file)

        scheduler = BackgroundScheduler()
        # S3 클라이언트 생성
        s3 = boto3.client('s3', aws_access_key_id=config['aws_access_key_id'], aws_secret_access_key=config['aws_secret_access_key'])

        for folder_config in config['backup_folders']:
            cron_trigger = CronTrigger.from_crontab(folder_config['backup_schedule'])
            scheduler.add_job(backup_job, cron_trigger, args=(folder_config,s3, ))
        scheduler.start()

        try:
            # 프로그램 실행
            print('백업 스케줄러가 시작되었습니다. 종료하려면 Ctrl+C를 누르세요.')
            logging.info('백업 스케줄러가 시작되었습니다.')
            while True:
                pass
        except KeyboardInterrupt:
            # Ctrl+C를 누르면 프로그램 종료
            scheduler.shutdown()
    except Exception as e:
        print(f'프로그램 실행 중 오류 발생: {str(e)}')
        logging.info(f'프로그램 실행 중 오류 발생: {str(e)}')