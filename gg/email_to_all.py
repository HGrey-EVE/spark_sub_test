import smtplib
import os
from collections import namedtuple
from email.message import EmailMessage
import mimetypes
from typing import List, Text
from datetime import date,datetime
import pandas
import subprocess
import time


# ------------- Models ------------------
EmailProvider = namedtuple("EmailProvider", "host port")
EmailAccount = namedtuple("EmailAccount", "username password")

# hdfs上csv文件存放路径
CSV_PATH = "/user/zszqyuqing/zszq/gg"
# 服务器上csv文件存放路径
STORE_PATH = "/data2/zszqyuqing/gg/data"
# 将今天时间格式化成指定形式
TODAY = date.today().strftime("%Y%m%d")
#TODAY = 20210124


class EmailAddress(namedtuple("EmailAddress", "title address")):
    def __repr__(self):
        return f"{self.title} <{self.address}>"

def smtp_client_factory(
    mail_provider: EmailProvider, account: EmailAccount
) -> smtplib.SMTP:
    client = smtplib.SMTP(host=mail_provider.host, port=mail_provider.port)
    client.login(account.username, account.password)
    return client


def assemble_email(
    mail_from: EmailAddress, mail_to: List[EmailAddress], mail_cc: List[EmailAddress],subject: Text, content: Text
) -> EmailMessage:
    msg = EmailMessage()
    msg.set_content(content)
    msg["Subject"] = subject
    msg["From"] = str(mail_from)
    msg["To"] = ", ".join(str(it) for it in mail_to)
    msg["CC"] = ", ".join(str(it) for it in mail_cc)
    msg
    # print(msg)
    return msg


def assemble_attachment(msg: EmailMessage, filename: Text, path: Text):
    if not os.path.isfile(path):
        raise ValueError(f"attachment is not a file")
    # Guess the content type based on the file's extension.  Encoding
    # will be ignored, although we should check for simple things like
    # gzip'd or compressed files.
    ctype, encoding = mimetypes.guess_type(path)
    if ctype is None or encoding is not None:
        # No guess could be made, or the file is encoded (compressed), so
        # use a generic bag-of-bits type.
        ctype = "application/octet-stream"
    maintype, subtype = ctype.split("/", 1)
    with open(path, "rb") as fp:
        msg.add_attachment(
            fp.read(), maintype=maintype, subtype=subtype, filename=filename
        )
    return msg

def send_mail(client: smtplib.SMTP, msg: EmailMessage):
    client.send_message(msg)

# 将hdfs上的文件拉取到服务器
def get_hdfs_csv():
    print('进入get_hdfs_csv...')
    os.system(f"hadoop fs -get {CSV_PATH}/{TODAY}/*.csv {STORE_PATH}/zszq_gg-{TODAY}.csv ")
    # os.system(f"hadoop fs -get {CSV_PATH}/20210106/*.csv {STORE_PATH}/zszq-test.csv ")

# 获取csv文件条数
def get_csv_count():
    path = f"{STORE_PATH}/zszq_gg-{TODAY}.csv"
    df = pandas.read_csv(path,error_bad_lines=False)
    return df.shape[0]

def get_hdfs_file_exist():
    filexistchk = f"hadoop fs -test -e {CSV_PATH}/{TODAY};echo $?"
    filexistchk_output = subprocess.Popen(filexistchk, shell=True, stdout=subprocess.PIPE).communicate()
    if '1' not in str(filexistchk_output[0]):
        return 1
    else:
        return 0

if __name__ == "__main__":
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(f'{TODAY}进入email...')

    for i in range(1, 30):  # 根据因子迭代
        print(f'第{i}')
        time.sleep(120)
        if get_hdfs_file_exist() == 1:  # 确定第一个因子
            print(f'第{i}次hdfs文件已存在')
            time.sleep(10)
            get_hdfs_csv()
            time.sleep(10)
            break

    # email_provider = EmailProvider("mail.bbdservice.com", 25)
    email_provider = EmailProvider("smtp.bbdservice.com", 25)
    # email_account = EmailAccount("collection_platform@bbdservice.com", "Password5")
    # email_account = EmailAccount("bigdata_system@bbdservice.com", "Bigdata_system")
    email_account = EmailAccount("bigdata_system@bbdservice.com", "Bigdata_system_2")

    # sender = EmailAddress("数据研发中心", "collection_platform@bbdservice.com")
    sender = EmailAddress("数据研发中心", "bigdata_system@bbdservice.com")
    receivers = [
        EmailAddress("黄尧", "huangyao@bbdservice.com"),
        EmailAddress("陈小伟", "chenxw@bbdservice.com"),
        EmailAddress("敖日格勒", "aorigele@bbdservice.com"),
        EmailAddress("叶胜兰", "yeshenglan@bbdservice.com"),
        # EmailAddress("唐华奎", "tanghuakui@bbdservice.com"),
    ]
    cc_receivers = [
        EmailAddress("唐华奎", "tanghuakui@bbdservice.com"),
    ]

    client = smtp_client_factory(email_provider, email_account)

    # get_hdfs_csv()
    csv_count = get_csv_count()

    subject = f"招商证券投研分析平台项目-{TODAY}-公告数据导出"
    content = "@黄尧、陈小伟、敖日格勒、叶胜兰:\n" \
              f"     今天的公告数据已导出，共{csv_count}条，详情见附件，请查收，谢谢"
    send_mail(
        client,
        assemble_attachment(assemble_email(
            sender, receivers,cc_receivers, subject=subject, content=content
        ), f'zszq_gg-{TODAY}.csv', f'{STORE_PATH}/zszq_gg-{TODAY}.csv'),
    )
