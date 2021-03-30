import inspect
import json
import logging
import traceback


def try_catch(event, callback):
    handler_name = inspect.stack()[1][3]
    try:
        logging.info(f'{handler_name}: Event - {event}')
        return callback()
    except Exception as e:
        print(e)
        traceback.print_exc()
        msg = {
            'exception': str(e),
            'lambda': handler_name,
            'event': event
        }
        raise Exception(json.dumps(msg))
