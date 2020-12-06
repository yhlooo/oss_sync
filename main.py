# -*- coding: utf-8 -*-

"""主模块

包含该项目的主函数

"""


import argparse
import json
import logging
import os
import sys
from typing import Callable, Dict, List, Optional, Union

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from oss import AliyunOssBucket, QcloudCosBucket
from utils import FileManager, OSSSynchronizer


# 日志配置
logger = logging.getLogger()

normal_formatter = logging.Formatter('%(levelname)s: %(message)s')
debug_formatter = logging.Formatter(
    '[%(threadName)s][%(module)s.%(funcName)s][%(filename)s:%(lineno)s] %(levelname)s: %(message)s'
)

normal_console_handler = logging.StreamHandler()
normal_console_handler.setFormatter(normal_formatter)
normal_console_handler.setLevel(logging.INFO)

debug_console_handler = logging.StreamHandler()
debug_console_handler.setFormatter(debug_formatter)
debug_console_handler.setLevel(logging.DEBUG)


# 定义一些类型别名
UnitConfig = Dict[str, str]
Config = Union[UnitConfig, List[UnitConfig]]
ConfigValidator = Callable[[Config], Config]


# 基础配置
default_main_config_path: str = 'config/config.json'
default_config_encoding: str = 'utf-8'


def main_config_validator(config: Config) -> Config:
    """主配置校验器

    校验主配置是否合法，以及尽可能合法化配置

    该方法会对每一个配置检查以下字段：

    - oss_type: OSS 类型，只能是 'tencent-cos' 或 'aliyun-oss' 。
    - oss_config: OSS 配置，必须是一个已经存在文件。
    - local_dir: 本地文件路径，必须是一个已经存在的文件夹。
    - direction: 同步方向，只能是 'local-to-remote' 或 'remote-to-local' 。

    Notes:
        - 如果配置是字典类型，会转换为列表方便统一处理
        - 路径配置中的相对路径会被转换为等价的绝对路径
        - 除了上述字段，多余字段会被丢弃

    Args:
        config: 输入配置

    Returns:
        合法化后的配置

    Raises:
        TypeError: 配置中存在类型错误
        KeyError: 必要的配置字段不存在
        ValueError: 配置中存在无法合法化的值

    """

    # 基本类型检查
    if not isinstance(config, dict) and not isinstance(config, list):
        raise TypeError(f'主配置的类型必须是 {dict.__class__} 或 {list.__class__} ，而非 {type(config)}')

    # 转成列表统一处理
    if isinstance(config, dict):
        config = [config, ]

    valid_config = []

    for config_item in config:
        oss_type = config_item.get('oss_type')
        oss_config = config_item.get('oss_config')
        local_dir = config_item.get('local_dir')
        direction = config_item.get('direction')

        if not oss_type:
            raise KeyError('主配置缺少必要字段： "oss_type"')

        if not oss_config:
            raise KeyError('主配置缺少必要字段： "oss_config_file"')

        if not local_dir:
            raise KeyError('主配置缺少必要字段： "local_dir"')

        if not direction:
            raise KeyError('主配置缺少必要字段： "direction"')

        valid_oss_type = str(oss_type).lower().strip()
        if valid_oss_type not in ['tencent-cos', 'aliyun-oss']:
            raise ValueError(
                f'主配置字段 "oss_type" 的值不符合预期： "{oss_type}" '
                '（预期值为 "tencent-cos" （腾讯云 COS ） 或 "aliyun-oss" （阿里云 OSS ））'
            )

        valid_oss_config = os.path.abspath(str(oss_config).strip())
        if not os.path.isfile(valid_oss_config):
            raise ValueError(
                f'主配置字段 "oss_config" 的值： "{oss_config}" （ "{valid_oss_config}" ）所指向的路径不是一个文件'
            )

        valid_local_dir = os.path.abspath(str(local_dir).strip())
        if not os.path.isdir(valid_local_dir):
            raise ValueError(
                f'主配置字段 "local_dir" 的值： "{local_dir}" （ "{valid_local_dir}" ）所指向的路径不是一个文件夹'
            )

        valid_direction = str(direction).lower().strip()
        if valid_direction not in ['local-to-remote', 'remote-to-local']:
            raise ValueError(
                f'主配置字段 "direction" 的值不符合预期： "{direction}" '
                '（预期值为 "local-to-remote" 或 "remote-to-local" ）'
            )

        # 有多余的字段
        if len(config_item) > 4:
            extra_keys = [
                key
                for key
                in config_item.keys()
                if key not in ['oss_type', 'oss_config', 'local_dir', 'direction']
            ]
            logger.warning(f'主配置中存在多余字段： {extra_keys}')

        valid_config.append({
            'oss_type': valid_oss_type,
            'oss_config': valid_oss_config,
            'local_dir': valid_local_dir,
            'direction': valid_direction
        })

    return valid_config


def load_configs(
        config_path: str,
        validator: Optional[ConfigValidator] = None,
        encoding: str = default_config_encoding
) -> Optional[Config]:
    """加载配置文件

    Args:
        config_path: 配置文件路径。可以是绝对路径，也可以是基于当前工作路径的相对路径
        validator: 配置校验器（可选）。若指定，则输出的配置会经过过配置校验器处理
        encoding: 配置文件的字符编码（可选）。默认值为 default_encoding

    Returns:
        如果加载成功，返回加载的配置，否则返回 None

    """

    # 一些参数检查
    assert config_path, '参数 `config_path` 的值不能为空。'
    assert os.path.isfile(config_path), f'路径 "{os.path.abspath(config_path)}" 上不是一个文件。'
    assert encoding, '参数 `encoding` 的值不能为空。'

    with open(config_path, 'rt', encoding=encoding) as file_obj:
        try:
            config = json.load(file_obj)
        except json.JSONDecodeError as err:
            logger.error(f'解析配置文件： {os.path.abspath(config_path)} 失败， error = {err}')
            return None

    if validator is not None:
        try:
            config = validator(config)
        except (TypeError, KeyError, ValueError) as err:
            logger.error(f'校验配置时发生错误：{err}')
            return None

    return config


def parser_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """解析参数

    定义并解析命令行参数

    Args:
        args: 待解析的命令行参数（可选）。若不指定则默认为 sys.argv

    Returns:
        解析完成的参数

    """

    parser = argparse.ArgumentParser(
        prog=f'python3 {__file__}',
        description='将本地文件夹与 OSS Bucket 进行同步。',
        add_help=True
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='开启调试模式（显示 DEBUG 日志）'
    )

    parser.add_argument(
        '--config',
        type=str,
        required=False,
        help=f'主配置文件路径（默认值： "{default_main_config_path}" ）',
        metavar='FILE'
    )

    parser.add_argument(
        '--config-encoding',
        type=str,
        required=False,
        help=f'配置文件的字符编码（默认值： "utf-8" ）',
        metavar='CHARSET'
    )

    if args is None:
        args = sys.argv[1:]

    return parser.parse_args(args)


def main() -> None:
    """主函数
    """

    # 解析命令行参数
    args = parser_args()

    # 开启调试模式
    if args.debug:
        logger.addHandler(debug_console_handler)
        logger.setLevel(logging.DEBUG)
        logger.debug('DEBUG 模式已开启')
    else:
        logger.addHandler(normal_console_handler)
        logger.setLevel(logging.INFO)
        logger.debug('DEBUG 模式关闭')

    main_config_path = args.config or default_main_config_path
    config_encoding = args.config_encoding or default_config_encoding

    # 加载主配置文件
    config = load_configs(
        config_path=main_config_path,
        validator=main_config_validator,
        encoding=config_encoding
    )

    if config is None:
        logger.error(f'加载主配置文件 "{main_config_path}" 失败。')
        exit(1)

    for config_item in config:
        oss_type = config_item['oss_type']
        oss_config_path = config_item['oss_config']
        local_dir = config_item['local_dir']
        direction = config_item['direction']

        # 加载 OSS 配置文件
        oss_config = load_configs(
            config_path=oss_config_path,
            validator=None,
            encoding=config_encoding
        )
        if oss_config is None:
            logger.error(f'加载 OSS 配置文件 "{oss_config_path}" 失败。')
            exit(1)

        if oss_type == 'tencent-cos':
            bucket = QcloudCosBucket(oss_config)
        else:
            bucket = AliyunOssBucket(oss_config)

        file_manager = FileManager(local_dir)
        oss_synchronizer = OSSSynchronizer(file_manager, bucket)

        if direction == 'local-to-remote':
            logger.info(f'开始同步 {local_dir}（本地）-> {oss_config.get("bucket", "Unknown Bucket")}（OSS）')
            oss_synchronizer.sync_from_local_to_oss()
        else:
            logger.info(f'开始同步 {oss_config.get("bucket", "Unknown Bucket")}（OSS） -> {local_dir}（本地）')
            oss_synchronizer.sync_from_oss_to_local()


if __name__ == '__main__':
    main()
