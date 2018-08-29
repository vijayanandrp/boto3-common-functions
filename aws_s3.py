#!/usr/bin/env python

"""
    CRUD - AWS S3 Boto3
"""

__author__ = "Vijay Anand RP"
__status__ = "Development"

import boto3


class S3(object):
    def __init__(self):
        self.s3_name = 's3'
        self.client = boto3.client(self.s3_name)  # client
        self.resource = boto3.resource(self.s3_name)  # resource

    @staticmethod
    def __check_prefix(prefix):
        """
        Add '/' in end of prefix if its required
        :param prefix:
        :return: prefix string
        """
        if list(prefix)[-1] is not '/':
            prefix += '/'
        return prefix

    def create_bucket(self, bucket_name):
        """
        Create the bucket in AWS S3
        :param bucket_name:
        :return: None
        """
        response = self.resource.create_bucket(Bucket=bucket_name)
        print('Created bucket - {} - {}'.format(bucket_name, response))

    def delete_bucket(self, bucket_name):
        """
        Delete the bucket in AWS S3
        :param bucket_name:
        :return: None
        """
        if self.check_bucket_exist(bucket_name):
            bucket = self.resource.Bucket(name=bucket_name)
            self.delete_all_bucket_obj(bucket_name)
            response = bucket.delete()
            print('Deleted bucket - {} - {}'.format(bucket_name, response))

    def list_buckets(self):
        """
        List of Buckets in AWS S3
        :return: list of bucket names.
        """
        result = list()
        for _bucket in self.resource.buckets.all():
            result.append(_bucket.name)
        return result

    def check_bucket_exist(self, bucket_name):
        """
        Find/validate the existing bucket in AWS S3
        :param bucket_name:
        :return: Bool
        """
        buckets = self.list_buckets()
        if bucket_name in buckets:
            print('Bucket - {} exists.'.format(bucket_name))
            return True
        else:
            print('Bucket - {} dose not exists.'.format(bucket_name))
            return False

    def create_bucket_dir(self, bucket_name, prefix):
        """
        Create the folder/directory inside the Bucket
        :param bucket_name:
        :param prefix:
        :return: None
        """
        prefix = S3.__check_prefix(prefix)
        response = self.client.put_object(Bucket=bucket_name, Key=prefix)
        print('Created bucket dir - {} - {}'.format(bucket_name + '/' + prefix, response))

    def delete_all_bucket_obj(self, bucket_name):
        """
        Deletes all objects inside the Bucket
        :param bucket_name:
        :return: None
        """
        bucket = self.resource.Bucket(name=bucket_name)
        response = bucket.objects.all().delete()
        print('Deleted all bucket - {} objects  - {}.'.format(bucket_name, response))

    def delete_bucket_obj(self, bucket_name, prefix, check_prefix=False):
        """
        Delete a particular object inside the bucket
        :param bucket_name:
        :param prefix:
        :param check_prefix:
        :return: None
        """
        if check_prefix:
            prefix = S3.__check_prefix(prefix)
        response = self.client.delete_object(Bucket=bucket_name, Key=prefix)
        print('Deleted  bucket - {} object  - {}.'.format(bucket_name + '/' + prefix, response))

    def list_bucket_obj(self, bucket_name):
        """
        Returns all objects in a bucket
        :param bucket_name:
        :return: list
        """
        bucket = self.resource.Bucket(name=bucket_name)
        result = list()
        for _object in bucket.objects.all():
            result.append(_object.key)
        return result

    def filter_bucket_obj(self, bucket_name, prefix=''):
        """
        Filter is kind of search and find objects using some prefix.
        :param bucket_name:
        :param prefix:
        :return: list of matching objects
        """
        bucket = self.resource.Bucket(name=bucket_name)
        result = list()
        for _object in bucket.objects.filter(Prefix=prefix):
            result.append(_object.key)
        return result

    def upload_bucket_obj(self, bucket_name, src_file, dst_file):
        """
        Upload the file to a bucket
        :param bucket_name:
        :param src_file: '/tmp/hello.txt'
        :param dst_file: 'ga/hello.txt'
        :return: None
        """
        bucket = self.resource.Bucket(name=bucket_name)
        bucket.upload_file(src_file, dst_file)
        print('Uploaded object - {} in bucket - {}.'.format(dst_file, bucket_name))

    def download_bucket_obj(self, bucket_name, src_file, dst_file):
        """
        Download the file to a bucket
        :param bucket_name:
        :param src_file: 'ga/hello.txt'
        :param dst_file: '/tmp/hello.txt'
        :return: None
        """
        bucket = self.resource.Bucket(name=bucket_name)
        bucket.download_file(src_file, dst_file)
        print('Downloaded object - {} in bucket - {}.'.format(dst_file, bucket_name))


if __name__ == '__main__':
    s3 = S3()
    _bucket_name = 'vijay-data-bucket'
    tmp_files = ['ga.json', 'ga.hive', 'ga.lck']
    dirs = ['data', 'tmp', 'app', 'lib', 'etc']

    for tmp_file in tmp_files:
        with open('/tmp/' + tmp_file, 'w') as fp:
            fp.write(' This is checking scenario for S3. thanks\n for viewing this.')
            fp.write('\n')

    print('1. creating a a bucket')
    s3.create_bucket(bucket_name=_bucket_name)
    print(s3.list_buckets())

    print('2. creating directory inside a bucket')
    for _dir in dirs:
        s3.create_bucket_dir(bucket_name=_bucket_name, prefix=_dir)
    print(s3.list_bucket_obj(_bucket_name))

    print('3. Add files in bucket')
    for tmp_file in tmp_files:
        for _dir in dirs:
            s3.upload_bucket_obj(bucket_name=_bucket_name,
                                 src_file='/tmp/' + tmp_file,
                                 dst_file=_dir + '/' + tmp_file)

    for _dir in dirs:
        print('Filter Files in dir ', _dir)
        print(s3.filter_bucket_obj(bucket_name=_bucket_name, prefix=_dir))

    print('4. Download files in bucket')

    for _dir in dirs:
        for tmp_file in tmp_files:
            s3.download_bucket_obj(bucket_name=_bucket_name,
                                   src_file=_dir + '/' + tmp_file,
                                   dst_file=_dir + '-' + tmp_file)

    print('Check does files exists in the AWS S3')
    import time

    time.sleep(60)

    print('5. Delete files in bucket')
    for _dir in dirs:
        for tmp_file in tmp_files:
            s3.delete_bucket_obj(bucket_name=_bucket_name,
                                 prefix=_dir + '/' + tmp_file,
                                 check_prefix=False)
            s3.delete_bucket_obj(bucket_name=_bucket_name,
                                 prefix=_dir,
                                 check_prefix=True)

    s3.delete_all_bucket_obj(_bucket_name)
    print('6. Delete bucket')
    s3.delete_bucket(_bucket_name)
