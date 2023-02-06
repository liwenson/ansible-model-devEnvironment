#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function)
import re
__metaclass__ = type

import json
import os
import stat
import tempfile
import gitlab

from ansible.errors import AnsibleError, AnsibleFileNotFound
from ansible.module_utils._text import to_bytes, to_native, to_text
from ansible.plugins.action import ActionBase
from ansible.utils.hashing import checksum

from tempfile import TemporaryDirectory


class GitlabUtils(object):
    """
    操作Gitlab
    """

    def __init__(self, module) -> None:
        self.module = module
        self.url = module['url']
        self.access_token = module['accessToken']
        self.src = module['src']
        self.dest = module['dest']
        self.project_id = module['projectID']
        self.branch = module['branch']
        # self.filenameTmp = module['filenameTmp']
        self.tmp_fp = tempfile.TemporaryDirectory()
        self.git_gl = gitlab.Gitlab(
            self.url, self.access_token, api_version='4', ssl_verify=False)


    # 获得项目：projectID的格式随意，反正我就写了个数字进去
    def get_project(self) -> any:
        """ 获取项目列表"""
        projectss = self.git_gl.projects.get(self.project_id)
        return projectss

    def get_file(self):
        """
        获得project下单个文件
        """
        projects = self.get_project()
        result = {}

        # tmpdir = self.create_tmp()
        filename_tmp = "{}/{}".format(self.tmp_fp.name,
                                      self.src.split("/")[-1])
        print("filename_tmp:", filename_tmp)

        try:
            # 获得文件
            with open(filename_tmp, 'wb') as f:
                projects.files.raw(
                    file_path=self.src, ref=self.branch, streamed=True, action=f.write)

            result = {'changed': True, 'msg': "success", "file": filename_tmp}

        except Exception as err:
            print("Exception err: ", err)
            self._clean_fp_
            result = {'changed': False, 'msg': err}

        return result

    def _clean_fp_(self):
        self.tmp_fp.cleanup


class ActionModule(ActionBase):
    """
    ansible 类
    """

    def run(self, tmp=None, task_vars=None):
        ''' handler for file transfer operations '''
        if task_vars is None:
            task_vars = dict()
        # 执行父类的run方法
        result = super(ActionModule, self).run(tmp, task_vars)

        if result.get('skipped'):
            return result

        # 获取参数
        module_args = self._task.args.copy()

        module_args['url'] = self._task.args.get(
            'url', None)
        module_args['accessToken'] = self._task.args.get(
            'accessToken', None)
        module_args['src'] = self._task.args.get(
            'src', None)
        module_args['dest'] = self._task.args.get(
            'dest', None)
        module_args['projectID'] = self._task.args.get(
            'projectID', None)
        module_args['branch'] = self._task.args.get(
            'branch', None)

        # 判定参数
        result['failed'] = True
        if module_args['src'] is None or module_args['dest'] is None or module_args['url'] is None or module_args['accessToken'] is None or module_args['projectID'] is None:
            result['msg'] = "src and dest and url and accessToken and projectID are required"
        else:
            del result['failed']

        if module_args['branch'] is None:
            module_args['branch'] = 'master'

        if result.get('failed'):
            return result

        # 获取gitlab 文件
        gl = GitlabUtils(module_args)
        res = gl.get_file()

        # 找到source的路径地址
        try:
            if (res['changed']):
                print("true")
                source = self._find_needle('files', res['file'])
                print("source", source)
            else:
                print("false", res['msg'])
                result['failed'] = True
                result['msg'] = str(res['msg'])
                return result
        except AnsibleError as err:
            result['failed'] = True
            result['msg'] = to_text(err)
            return result

        # 获取本地文件，不存在抛出异常
        try:
            source_full = self._loader.get_real_file(source)
            source_rel = os.path.basename(source_full)

            print(os.path.isfile(source_full))

            print(source_full, source_rel)
        except AnsibleFileNotFound as err:
            result['failed'] = True
            result['msg'] = "could not find src=%s, %s" % (source, err)
            self._remove_tmp_path(self._connection._shell.tmpdir)

            return result

        try:
            # 定义拷贝到远程的文件路径
            tmp_src = self._connection._shell.join_path(
                "/tmp", 'source')
        except Exception as err:
            print(err)

        print("tmp_src: ", tmp_src)

        # 判断文件保存的路径
        dest = module_args['dest']
        if self._connection._shell.path_has_trailing_slash(dest):
            dest_file = self._connection._shell.join_path(dest, source_rel)
        else:
            dest_file = dest

        local_checksum = checksum(source_full)

        # 远程文件
        remote_path = None
        remote_path = self._transfer_file(source_full, tmp_src)

        # 确保我们的文件具有执行权限
        if remote_path:
            self._fixup_perms2(("/tmp", remote_path))

        # 运行remote_copy 模块
        new_module_args = self._task.args.copy()
        new_module_args.update(
            dict(
                src=tmp_src,
                dest=dest_file,
                original_basename=source_rel,
                checksum=local_checksum
            )
        )

        module_return = self._execute_module(module_name='my_gitlab',
                                             module_args=new_module_args, task_vars=task_vars,
                                             tmp=tmp)

        # 判断运行结果
        if module_return.get('failed'):
            result.update(module_return)
            return result
        if module_return.get('changed'):
            changed = True

        if module_return:
            result.update(module_return)
        else:
            result.update(
                dict(dest=module_args['dest'], src=module_args['src'], changed=changed))

        # 清理临时文件
        self._remove_tmp_path(tmp)

        # 返回结果
        return result
