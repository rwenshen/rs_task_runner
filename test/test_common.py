import time
import asyncio
from pathlib import Path


def _printResult(r: str, *strs, **extras):
    for s in strs:
        r += '\n\t'
        r += s
    for i, es in extras.items():
        r += ' '.join(str(s) for s in es)
    print(r)
    return True, r

def printFn(sleepTime: float, taskName: str, *strs, **extras):
    time.sleep(sleepTime)
    r = f'Sleep {sleepTime} second(s) in "{taskName}".'
    _printResult(r, *strs, **extras)
    return True, r

async def printFnAsync(sleepTime: float, taskName: str, *strs, **extras):
    await asyncio.sleep(sleepTime)
    r = f'Sleep {sleepTime} second(s) in "{taskName}".'
    _printResult(r, *strs, **extras)
    return True, r

def visitFile(filePath: str):
    try:
        bstr = Path(filePath).read_bytes()
        print(f'File "{filePath}" size: {len(bstr)}.')
        return True
    except:
        return False
