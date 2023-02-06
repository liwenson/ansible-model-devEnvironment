#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from ansible.plugins.action import ActionBase
from ansible.errors import AnsibleError, AnsibleFileNotFound
from ansible.module_utils._text import to_text
from ansible.utils.hashing import checksum

from minio import Minio
from minio.error import S3Error

from tempfile import TemporaryDirectory


import os
import shutil
import random
import string


class MinioUtils(object):
    """
    操作minio
    """

    def __init__(self, module) -> None:
        self.module = module
        self.name = module['name']
        self.state = module['state']
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
        except S3Error as e:
            print("[error]:", e)

    def CreateTmp(self):
        """
        创建临时目录
        """
        # self.filenameTmp = module['filenameTmp']
        # isExists = os.path.exists(self.filenameTmp)
        with TemporaryDirectory() as dirname:
            return dirname

    def fget_minio(self):
        """
        下载保存文件保存本地
        :param bucket_name:
        :param file:
        :param file_path:
        :return:
        """
        try:
            tmpdir = self.CreateTmp()
            lists = self.bucket_list_files()
            for obj in lists:
                if not self.client.bucket_exists(self.bucket):
                    return 'Bucket does not exist'

                ran_str = ''.join(random.sample(
                    string.ascii_letters + string.digits, 12))
                filenameTmp = "{}/{}".format(tmpdir, ran_str)

                fullname = "{}/{}".format(filenameTmp,
                                          obj.split("/")[-1])

                data = self.client.fget_object(
                    self.bucket, obj, fullname)
                result = {}
                result['size'] = data.size
                result['etag'] = data.etag
                result['content_type'] = data.content_type
                result['last_modified'] = data.last_modified
                result['metadata'] = data.metadata
                result['fullname'] = fullname
                result['tmpdir'] = tmpdir

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

    def _remote_copy(self, src, desc, rel, checksum, task_vars):
        """
        传输文件
        """

        # print("_remote_copy", task_vars)

        # 传输文件
        remote_path = None
        remote_path = self._transfer_file(src, desc)
        tmp = "/tmp"

        # 确保我们的文件具有执行权限
        if remote_path:
            self._fixup_perms2((tmp, remote_path))

        # 远程验证
        new_module_args = self._task.args.copy()
        new_module_args.update(
            dict(
                src=src,
                dest=desc,
                checksum=checksum,
                original_basename=rel,
            )
        )

        module_return = self._execute_module(module_name='my_minio',
                                             module_args=new_module_args, task_vars=task_vars
                                             )

        return module_return

    def run(self, tmp=None, task_vars=None):
        ''' handler for file transfer operations '''

        if task_vars is None:
            task_vars = dict()

        result = super(ActionModule, self).run(tmp, task_vars)
        del tmp  # tmp no longer has any effect

        if result.get('skipped'):
            return result

        # 获取参数
        module_args = self._task.args.copy()
        module_args['src'] = self._task.args.get(
            'src', None)

        module_args['dest'] = self._task.args.get(
            'dest', None)

        module_args['state'] = self._task.args.get(
            'state', None)

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

        # # 获取minio 文件
        mcli = MinioUtils(module_args)
        # print("开始下载")
        res = mcli.fget_minio()
        # print("res: ", res)

        # # 获取本地文件，不存在抛出异常
        try:
            source_full = self._loader.get_real_file(res['fullname'])
            source_rel = os.path.basename(res['fullname'])

            # print("source_full:", source_full)
            # print("source_rel:", source_rel)

        except AnsibleFileNotFound as e:
            result['failed'] = True
            result['msg'] = "could not find src=%s, %s" % (res['fullname'], e)
            self._remove_tmp_path(source_full)
            return result

        # # 定义拷贝到远程的文件路径
        # tmp_src = self._connection._shell.join_path("/tmp", source_rel)
        # print("tmp_src", source_full)

        # # 判断文件保存的路径
        descfull = ""
        if len(module_args['dest'].split(".")) == 1:
            descfull = "{}/{}".format(module_args['dest'], source_rel)
        else:
            descfull = module_args['dest']

        # print("descfull:", descfull)
        # # 校验码校验，一致这不传输文件，不一致这传输
        local_checksum = checksum(source_full)
        dest_status = self._execute_remote_stat(
            descfull, all_vars=task_vars, follow="yes", checksum="yes")

        module_return = None
        result["local_checksum"] = local_checksum
        result["dest_checksum"] = dest_status['checksum']

        if dest_status['exists']:

            if local_checksum == dest_status['checksum']:
                """
                文件相同，不传输文件
                """

                print("文件相同，不传输文件")
                print("local_checksum", local_checksum)
                print("desc_checksum", dest_status['checksum'])
                result['msg'] = "file already exists"
                result['skipped'] = True
                return result
            else:
                module_return = self._remote_copy(
                    source_full, descfull, source_rel, local_checksum,  task_vars)
        else:
            """
            目标服务没有这个文件
            """
            print("目标服务没有这个文件")
            module_return = self._remote_copy(
                source_full, descfull, source_rel, local_checksum, task_vars)

        # 清理临时文件
        # self._remove_tmp_path(res["fullname"])
        # os.remove(res["fullname"])
        shutil.rmtree(res["tmpdir"])
        result["msg"] = module_return["invocation"]["module_args"]

        # 返回结果
        return result
