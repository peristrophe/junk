import os
import json
import datetime
import requests
from time import sleep
from modules.custom_exceptions import WorkflowFailedException

class WorkflowApi:
    ''' Treasure Workflow API Client '''

    BASE_URL = 'https://api-workflow.treasuredata.com/api'

    def __init__(self, key, timezone=None):
        self.key = key

        if timezone:
            self.timezone = timezone
        else:
            self.timezone = datetime.timezone(datetime.timedelta(hours=+9), 'JST')

    @property
    def _auth_header(self):
        return { 'Authorization': 'TD1 {}'.format(self.key) }

    def _just_get(self, endpoint):
        res = requests.get('{}/{}'.format(self.BASE_URL, endpoint), headers=self._auth_header)
        res.raise_for_status()
        return res

    def workflows(self):
        '''\
        Get list of registered workflow.
        HTTP(S) GET https://api-workflow.treasuredata.com/api/workflows
        '''

        return self._just_get('workflows')

    def sessions(self):
        '''\
        Get history of workflow sessions.
        HTTP(S) GET https://api-workflow.treasuredata.com/api/sessions
        '''

        return self._just_get('sessions')

    def attempts(self, workflow_id, session_time=None, **params):
        '''\
        Run workflow.
        HTTP(S) PUT https://api-workflow.treasuredata.com/api/attempts
        '''

        if session_time is None:
            session_time = datetime.datetime.now(self.timezone).isoformat()

        header = self._auth_header
        header['Content-Type'] = 'application/json'

        payload = {
            'workflowId': str(workflow_id),
            'params': params,
            'sessionTime': session_time
        }

        res = requests.put('{}/attempts'.format(self.BASE_URL), headers=header, data=json.dumps(payload))
        res.raise_for_status()
        return res


class WorkflowUtil:
    ''' Treasure Workflow utility for aya '''

    _api = WorkflowApi(os.getenv('TD_API_KEY'))
    _poling_interval = 60

    @classmethod
    def _get_session_info(cls, session_id):
        res = cls._api.sessions()
        resinfo = json.loads(res.text)

        session_info = filter(lambda x: x['id'] == str(session_id), resinfo['sessions'])

        try:
            return next(session_info)
        except StopIteration:
            return None

    @classmethod
    def _get_wfid_by_name(cls, project_name, workflow_name):
        res = cls._api.workflows()
        resinfo = json.loads(res.text)

        workflow_info = filter(lambda x: x['name'] == workflow_name and x['project']['name'] == project_name, resinfo['workflows'])

        try:
            return next(workflow_info)['id']
        except StopIteration:
            return None

    @classmethod
    def check_failed(cls, session_id):
        session = cls._get_session_info(session_id)
        if session['lastAttempt']['success'] is not True:
            raise WorkflowFailedException(**session)

    @classmethod
    def run_workflow(cls, *, _id=None, names=None, wait=False):
        '''\
        Run workflow.
        * you must specify argument either '_id' or 'names'.
        * workflow-id is renumbering when update a project, so i suggest specify 'names' if use this on ci/cd environment.

        Args:
            _id   (int|str):   identifier of workflow want to run.
            names (str):       names of workflow want to run. it is project-name and workflow-name separated by dot.
            wait  (bool):      True: Work synchronously / False: Work asynchronously (wait workflow complete when specify True)

        Returns:
            str: generated session-id of target workflow.
        '''

        if not _id and not names:
            raise ValueError('give me an argument either \'_id\' or \'names\' for identify workflow.')

        if _id:
            workflow_id = _id

        if names:
            namelist = names.split('.')
            if len(namelist) < 2:
                raise ValueError('\'names\' argument is must be separated by dot like following: \'project_name.workflow_name\'')

            project_name = namelist[0]
            workflow_name = namelist[1]
            workflow_id = cls._get_wfid_by_name(project_name, workflow_name)

        res = cls._api.attempts(workflow_id)
        resinfo = json.loads(res.text)

        if wait:
            session = cls._get_session_info(resinfo['sessionId'])
            while session['lastAttempt']['done'] is False:
                sleep(cls._poling_interval)
                session = cls._get_session_info(resinfo['sessionId'])

        return resinfo['sessionId']
