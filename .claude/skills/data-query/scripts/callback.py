import json
from pathlib import Path


async def write_state2json(data: dict):
    """保存状态到JSON文件"""
    file = "state.json"
    p_file = Path(file)
    p_file.parent.mkdir(parents=True, exist_ok=True)
    state = {**json.loads(p_file.read_text()), **data} if p_file.exists() else data
    p_file.write_text(json.dumps(state, ensure_ascii=False, indent=2))
    print(f"{list(data.keys())} saved to: {p_file}")


async def read_state2json():
    """从JSON文件读取状态"""
    file = "state.json"
    p_file = Path(file)
    state = json.loads(p_file.read_text()) if p_file.exists() else {}
    return state


async def write_callback(data: dict):
    await write_state2json(data)


async def read_callback():
    await read_state2json()
