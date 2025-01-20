import json

import stockfish
from _version import __version__

DEPTH = 20


def handle(event, context):
    sf = stockfish.Stockfish('/home/app/function/package/stockfish_executable',
                             depth=DEPTH)
    body = json.loads(event['body'])
    fen = body['fen']
    sf.set_fen_position(fen)
    sf.get_best_move()
    body['result'] = sf.info
    body['depth'] = DEPTH
    body['cloud_function_version'] = __version__
    return {
        'body': {
            'message': json.dumps(body),
        },
        'statusCode': 200,
    }


if __name__ == '__main__':
    print(__version__)
