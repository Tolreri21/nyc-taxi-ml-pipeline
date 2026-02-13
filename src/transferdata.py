import boto3
import os

s3 = boto3.client('s3')

bucket_name = 'nyc-taxi-data-ml-pipeline'


def upload_directory(local_directory, bucket, s3_prefix):
    """
    Загружает все файлы из локальной директории в S3

    Args:
        local_directory: путь к локальной папке (например '../data/processed')
        bucket: имя S3 бакета
        s3_prefix: префикс (папка) в S3 (например 'processed')
    """
    # Проверяем существование директории
    if not os.path.exists(local_directory):
        print(f"✗ Директория не найдена: {local_directory}")
        return

    if not os.path.isdir(local_directory):
        print(f"✗ Это не директория: {local_directory}")
        return

    uploaded_count = 0

    # Проходим по всем файлам в директории и поддиректориях
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            # Полный путь к локальному файлу
            local_path = os.path.join(root, file)

            # Относительный путь от базовой директории
            relative_path = os.path.relpath(local_path, local_directory)

            # Путь в S3 (заменяем \ на / для Windows)
            s3_path = os.path.join(s3_prefix, relative_path).replace("\\", "/")

            try:
                print(f"Загружаем: {local_path} -> s3://{bucket}/{s3_path}")
                s3.upload_file(local_path, bucket, s3_path)
                uploaded_count += 1
                print(f"  ✓ Успешно")
            except Exception as e:
                print(f"  ✗ Ошибка: {e}")

    print(f"\n{'=' * 50}")
    print(f"Загружено файлов: {uploaded_count}")
    print(f"{'=' * 50}")


# Загружаем обе директории
print("Загрузка processed...")
upload_directory('../data/processed', bucket_name, 'processed')

print("\nЗагрузка raw...")
upload_directory('../data/raw', bucket_name, 'raw')