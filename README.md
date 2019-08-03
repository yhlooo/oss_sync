# oss_sync

该脚本用于将 **一个本地文件夹** 与 OSS 上的 **一个 Bucket** 单向同步（从本地同步到 OSS 或者从 OSS 同步到本地），支持阿里云 OSS 和腾讯云的 COS

由于腾讯云的 COS (Cloud Object Storage) 和 阿里云的 OSS (Object Storage Service) 其实都是同一种东西，即对象存储服务，只不过腾讯云的产品什么都喜欢在前面加一个 'C' 以显得和其它服务商的的产品不一样。所以下文都将使用 OSS 这一更为通用的名称表示对象存储这一技术，当需要特指某个具体产品时我会使用“腾讯云的 COS ”或“阿里云的 OSS ”。

## 快速入门

### 运行环境

该脚本基于 Python3 ，至少支持 Python3.6/3.7 。

与腾讯云 COS 交互依赖其 XML Python SDK ，可以使用 `pip` 安装

```bash
pip install cos-python-sdk-v5
```

与阿里云 OSS 交互则依赖 Requests ，可使用 `pip` 安装

```bash
pip install requests
```

### 全局配置文件

拷贝该项目目录下的 `config/config.json.template` 配置文件，将其更名为 `config/config.json` （即去掉 `.template` ，放在原目录）

```json
{
  "oss_type": "`tencent-cos` or `aliyun-oss`",
  "oss_config": "config/tencent-cos.json",
  "local_dir": "./test",
  "direction": "`local-to-remote` or `remote-to-local`"
}
```

修改其中字段的值

- `oss_type` ：OSS 的类型，如果是腾讯云 COS 则填写 `tencent-cos` ，如果是阿里云 OSS 则填写 `aliyun-oss`
- `oss_config` ：OSS 配置文件的路径，可以填绝对路径，也可以填写相对路径，相对路径是相对于项目根目录的，该文件填写具体看下面两节
- `local_dir` ：需要同步的本地目录的路径，可以填写相对路径或绝对路径，相对路径是相对于项目根目录的。所填路径必须是一个目录，目录内的内容将会与 OSS Bucket 内的内容同步，这个目录必须提前创建好。建议路径全部使用 `/` 而不是 `\` ，路径最后不要添加 `/` .
- `direction` ：同步的方向，如果需要让 OSS 上的文件与本地的文件相同，即从本地向 OSS 同步，则填写 `local-to-remote` 。反之，欲使本地文件与 OSS 上的文件相同，即从 OSS 向本地同步，则填写 `remote-to-local`

### OSS 配置文件

根据使用 OSS 类型的不同， OSS 配置文件的格式也不同

#### 腾讯云 COS

若使用的是腾讯云 COS ，首先需要获取账号 API 密钥，即 `SecretId` 和 `SecretKey` ，比如 `OUyuQCjojvqJFmBsugqKltJDOblCAxafnFdY` 和 `wuqeAxffUxcSJctfIrTMYZdrKIMLTVrR`

> 为了安全，建议为 COS 服务专门开通一个子用户，使用子用户 API 密钥

开通 COS 服务，创建好 Bucket （存储桶），获得 Bucket 名和 Region （地域），比如 `whatever-1234567890` 和 `ap-guangzhou`

拷贝该项目目录下的 `config/tencent-cos-config.json.template` 配置文件，放置位置和文件名是任意的，但是为了方便说明，以下例子中我将其更名为 `config/tencent-cos-config.json` （即去掉 `.template` ，放在原目录）

```json
{
  "secret_id": "your secret id",
  "secret_key": "your secret key",
  "bucket": "bucket-name",
  "region": "region",
  "scheme": "`https` or `http`"
}
```

修改其中字段的值

`secret_id` 、 `secret_key` 、 `bucket` （ Bucket 名）、 `region` 的含义和格式同该小节前面的描述和示例

`scheme` ：与 OSS 通信时使用什么类型的协议，选填 `https` 或 `http`

注意设置上一节 “全局配置文件” 中的 `oss_config` 字段为该配置文件路径，在我的例子中它应该是 `config/tencent-cos-config.json`

#### 阿里云 OSS

若使用阿里云 OSS ，首先需要获取账号 `AccessKey ID` 和 `AccessKey Secret` ，比如 `fQtvrWRyFwUWgubh` 和 `XXKXlYYvviQirBxZQakLsDqTpwVOVp`

> 为了安全，建议为 OOS 服务专门开通一个子用户，使用子用户 AccessKey

开通 OOS 服务，创建好 Bucket ，获得 Bucket 名和 Bucket 的外网域名（如果在阿里云云服务器等可以内网访问 OSS 的地方使用该脚本，也可以使用内网域名），比如 `whatever` 和 `whatever.oss-cn-hangzhou.aliyuncs.com`

拷贝该项目目录下的 `config/aliyun-oss-config.json.template` 配置文件，放置位置和文件名是任意的，但是为了方便说明，以下例子中我将其更名为 `config/aliyun-oss-config.json` （即去掉 `.template` ，放在原目录）

```json
{
    "host": "bucket-name.region.aliyuncs.com",
    "bucket": "bucket-name",
    "access_key_id": "your access key id",
    "access_key_secret": "your access key secret"
}
```

修改其中字段的值

`host` （ Bucket 访问域名）、 `bucket` （ Bucket 名）、 `access_key_id` 、 `access_key_secret` 的含义和格式同该小节前面的描述和示例

注意设置上一节 “全局配置文件” 中的 `oss_config` 字段为该配置文件路径，在我的例子中它应该是 `config/aliyun-oss-config.json`

### 运行

准备好后，可以直接在该项目根目录运行 `main.py`

```bash
python main.py
```

它会按照设定，进行同步，具体同步行为可以阅读源码理解或参考下节描述

## 同步行为

当运行脚本，脚本会按照配置文件的设定进行同步。

同步的双方分别是所设定的本地目录和 OSS Bucket ，同步后，本地目录内的内容和 OSS 上 Bucket 内的内容是几乎完全一致的。（为什么是几乎下面介绍）

同步的方向决定了同步的双方中以谁作为基准。如果 `direction` 设为 `local-to-remote` ，则本地目录内的文件不会有任何改变， OSS Bucket 内的内容会跟本地目录内的内容一致；如果设为 `remote-to-local` ，则 OSS 上的文件不会有任何改变，本地目录内的文件会跟 OSS Bucket 内的文件一致。

同步时，首先会列出 OSS Bucket 内的全部对象（可以理解为文件）的文件名（包括路径）和 MD5 校验，然后列出本地目录内所有文件的文件名（包括路径）。逐一检查 OSS 上的对象和本地文件，列出一份需要更新的内容的清单，包括 OSS 上有但是本地没有的文件、 OSS 上没有但是本地有的文件、 OSS 和本地都有但是它们的 MD5 校验不同的文件（表明文件被修改过）。

然后根据同步的方向，在需要变更的一侧，进行增加、删除、覆盖动作。每一个变更完成之后，会打印一行 log ，类似于这样

```text
OK   [+] whatever/hhh1.txt
OK   [-] whatever/hhh2.txt
OK   [M] whatever/hhh3.txt
Fail [+] whatever/hhh4.txt
```

每一行 log 指示了变更状态（成功或失败），变更类型（增加、删除、覆盖）和变更的文件名（包括路径）

同路径同名且校验一致的文件不会重复上传或下载

对于前面提到的 “几乎完全一致” ，是因为对于 “空文件夹” 的处理上逻辑有所不同。这里说的 “空文件夹” 并不是全空的才是空文件夹，只要一个文件夹内，所有子路径下都不含文件，则该文件夹是空文件夹。所以文件夹套一个全空文件夹，则两层文件夹都被算作空文件夹。

首先 OSS 只有对象的概念，即一个对象名对应一个对象，并没有文件和文件夹的概念。你在 Web 界面新建一个叫 `hhh` 的文件夹，只不过是新建了一个对象名是 `hhh/` 的 0 大小对象。 `hhh` 中的 `test.txt` 只不过是一个对象名是 `hhh/test.txt` 的对象。没有 `hhh/` 也可以有 `hhh/test.txt` 。

所以文件夹在 OSS 上没有任何意义，仅仅是为了在它是空的的时候，让你在 Web 界面上看到一个文件夹。所以检查本地文件列表时都只检查文件，这意味着如果从本地同步到 OSS ，则 OSS 上不会出现任何空文件夹，实际上也没有任何文件夹，当在 Web 界面将文件夹内的文件手动删除后，文件夹也会消失。从 OSS 同步到本地时，虽然会列出 OSS 上的文件夹（如果被你手动创建了），这个文件夹也会被下载并在本地被创建，但是由于从 OSS 同步到本地的操作结束后都会进行一次清理本地空文件夹的操作，所以你在本地也看不到任何空文件夹。

后一个情况其实并不是故意要将空文件夹赶尽杀绝，清理本地空文件夹是为了避免在删除了本地某文件夹内所有文件后留一个 OSS 上不存在的文件夹而设计的逻辑，无意中保证了两个方向同步表现的一致性，即空文件夹不会被同步。
