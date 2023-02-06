#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from ansible.plugins.action import ActionBase
from ansible.errors import AnsibleError, AnsibleFileNotFound
from ansible.utils.hashing import checksum

from minio import Minio
from minio.error import S3Error


class MinioUtils(object):
    """
    操作minio
    """

    def __init__(self, module) -> None:
        self.module = module
        self.endpoint = module['endpoint']
        self.access_key = module['access_key']
        self.secret_key = module['secret_key']
        self.bucket = module['bucket']
        self.src = module['src']
        self.dest = module['dest']
        # self.filenameTmp = module['filenameTmp']

        self.minio_conf = {
            'endpoint': self.endpoint,
            'access_key': self.access_key,
            'secret_key': self.secret_key,
            'secure': False,
        }
        self.client = Minio(**self.minio_conf)

    def bucket_list_files(self):
        """
        列出存储桶中所有对象
        :param bucket_name: 桶名
        :param prefix: 前缀
        :return:
        """

        try:
            files_list = self.client.list_objects(
                bucket_name=self.bucket, prefix=self.src, recursive=True)
            lists = []
            for obj in files_list:
                lists.append(obj.object_name)
            return lists
        except S3Error as err:
            print("[error]:", err)

    def fget_minio(self):
        """
        下载保存文件保存本地
        :param bucket_name:
        :param file:
        :param file_path:
        :return:
        """
        try:
            lists = self.bucket_list_files()

            if len(lists) > 1:
                target = self.dest
                if len(target.split(".")) > 1:
                    return 'A directory should be a target'

                for obj in lists:
                    if not self.client.bucket_exists(self.bucket):
                        return 'Bucket does not exist'

                    fullname = "{}/{}".format(self.dest, obj.split("/")[-1])

                    data = self.client.fget_object(self.bucket, obj, fullname)

                    result = {}
                    result['size'] = data.size
                    result['etag'] = data.etag
                    result['content_type'] = data.content_type
                    result['last_modified'] = data.last_modified
                    result['metadata'] = data.metadata
                    result['fullname'] = fullname
            else:

                obj = lists[0]
                target = self.dest

                if len(target.split(".")) == 1:
                    fullname = "{}/{}".format(self.dest, obj.split("/")[-1])
                else:
                    fullname = target

                data = self.client.fget_object(self.bucket, obj, fullname)

                result = {}
                result['size'] = data.size
                result['etag'] = data.etag
                result['content_type'] = data.content_type
                result['last_modified'] = data.last_modified
                result['metadata'] = data.metadata
                result['fullname'] = fullname

                return result

        except S3Error as err:
            return err


class ActionModule(ActionBase):

    """
    在ansible 管控端执行的逻辑
    """

    def _ensure_invocation(self, result):
        # NOTE: adding invocation arguments here needs to be kept in sync with
        # any no_log specified in the argument_spec in the module.
        # This is not automatic.
        # NOTE: do not add to this. This should be made a generic function for action plugins.
        # This should also use the same argspec as the module instead of keeping it in sync.
        if 'invocation' not in result:
            if self._play_context.no_log:
                result['invocation'] = "CENSORED: no_log is set"
            else:
                # NOTE: Should be removed in the future. For now keep this broken
                # behaviour, have a look in the PR 51582
                result['invocation'] = self._task.args.copy()
                result['invocation']['module_args'] = self._task.args.copy()

        if isinstance(result['invocation'], dict):
            if 'content' in result['invocation']:
                result['invocation']['content'] = 'CENSORED: content is a no_log parameter'
            if result['invocation'].get('module_args', {}).get('content') is not None:
                result['invocation']['module_args']['content'] = 'VALUE_SPECIFIED_IN_NO_LOG_PARAMETER'

        return result

    def run(self, tmp=None, task_vars=None):
        ''' handler for file transfer operations '''

        if task_vars is None:
            task_vars = dict()

        result = super(ActionModule, self).run(tmp, task_vars)
        del tmp  # tmp no longer has any effect

        # 获取参数
        module_args = self._task.args.copy()
        module_args['src'] = self._task.args.get(
            'src', None)

        module_args['dest'] = self._task.args.get(
            'dest', None)

        module_args['bucket'] = self._task.args.get(
            'bucket', None)

        module_args['endpoint'] = self._task.args.get(
            'endpoint', None)

        module_args['access_key'] = self._task.args.get(
            'access_key', None)

        module_args['secret_key'] = self._task.args.get(
            'secret_key', None)

        # 判定参数
        result['failed'] = True
        if module_args['src'] is None or module_args['dest'] is None:
            result['msg'] = "src and dest and accessToken  are required"

        else:
            del result['failed']

        if result.get('failed'):
            return self._ensure_invocation(result)

        # 获取minio 文件
        mcli = MinioUtils(module_args)

        res = mcli.fget_minio()

        local_checksum = checksum(res['fullname'])

        result = {}
        result['checksum'] = local_checksum

        return result
