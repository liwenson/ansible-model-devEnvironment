from ansible.plugins.action import ActionBase

class ActionModule(ActionBase):

    def run(self, tmp=None, task_vars=None):

        result = super(ActionModule, self).run(tmp, task_vars)

        module_args = self._task.args.copy()
        module_args['base_url'] = self._templar._available_variables.get('base_url')
        module_args['username'] = self._templar._available_variables.get('api_username')
        module_args['password'] = self._templar._available_variables.get('api_password')
        return self._execute_module(module_args=module_args, task_vars=task_vars, tmp=tmp)
