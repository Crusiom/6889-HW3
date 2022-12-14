import re
import json
import boto3
import email
from sms_spam_classifier_utilities import one_hot_encode
from sms_spam_classifier_utilities import vectorize_sequences

# Create session of each aws features
session = boto3.Session()
email_client = session.client('ses')
s3 = session.client('s3')
runtime = session.client('runtime.sagemaker')



def lambda_handler(event, context):
    # text = b'Return-Path: <dw3033@columbia.edu>\r\nReceived: from mx0a-00364e01.pphosted.com (mx0a-00364e01.pphosted.com [148.163.135.74])\r\n by inbound-smtp.us-east-1.amazonaws.com with SMTP id hgthai7gupn6cjterong3r3kea9imgcl0tqufm81\r\n for daniel.wei@crusiom.com;\r\n Tue, 06 Dec 2022 01:49:18 +0000 (UTC)\r\nReceived-SPF: pass (spfCheck: domain of columbia.edu designates 148.163.135.74 as permitted sender) client-ip=148.163.135.74; envelope-from=dw3033@columbia.edu; helo=mx0a-00364e01.pphosted.com;\r\nAuthentication-Results: amazonses.com;\r\n spf=pass (spfCheck: domain of columbia.edu designates 148.163.135.74 as permitted sender) client-ip=148.163.135.74; envelope-from=dw3033@columbia.edu; helo=mx0a-00364e01.pphosted.com;\r\n dkim=pass header.i=@columbia.edu;\r\n dmarc=none header.from=columbia.edu;\r\nX-SES-RECEIPT: AEFBQUFBQUFBQUFGaVpSTEJRS0Y5VDBOU0RqQXFwWFpNUHdSTXdjL3ZsWFQ3bzRzWkhvZmcvT3dWdmFtRHhaY24vNXdYQmpDUkZkMFVDdTVtUVI4OXhHWEd0Y1R3YWFaR1crQXRLVmlVRUhUNFIvU2d6Y2lVNlU2aDBLVlIwckhIUTdiazRieVUwRUhhZ1Q0V0pqVFE3aHUyT0x2eElMQlBMaDlDRlA1Y25QMlcwckRBcUpKN1BhNndGN2VVUGlmWlVBM0psTnFwM3ptRFk1eWd3QnI4VjROMzlWLzR3U1hPQTc3YzlSVUxNNUg2UDFNWU00RXR4UHBqMVpsTXZsT0dlVkJhc0pwbFd6RzZ4dlcrbVVYMkFsOWViS1JxNE94eGtCNDFuVnR1bTJYdS8vbHhQbHlOV0E9PQ==\r\nX-SES-DKIM-SIGNATURE: a=rsa-sha256; q=dns/txt; b=IzfIuJwlEbQSOGyj5fQ8iaqSR4jMBRzUTi4qpUjaQJ8zpDxoh9DHE38vjGs2CKQ7C14YZj5htTOXjTUuXu0V0Ia/pFEn85hMWSri/CX4MvOfOzVfwFv1hp0VfnGlahH3nfSdcsaU75XOqdYv+brlduWm/P4xwyjGOSmqJDG37UE=; c=relaxed/simple; s=6gbrjpgwjskckoa6a5zn6fwqkn67xbtw; d=amazonses.com; t=1670291358; v=1; bh=E5e/tPsGez25P3XCC9SiKkFAEKypTXetgeJqB1KszLw=; h=From:To:Cc:Bcc:Subject:Date:Message-ID:MIME-Version:Content-Type:X-SES-RECEIPT;\r\nReceived: from pps.filterd (m0167068.ppops.net [127.0.0.1])\r\n\tby mx0a-00364e01.pphosted.com (8.17.1.19/8.17.1.19) with ESMTP id 2B61lm0A001856\r\n\tfor <daniel.wei@crusiom.com>; Mon, 5 Dec 2022 20:49:17 -0500\r\nDKIM-Signature: v=1; a=rsa-sha256; c=relaxed/relaxed; d=columbia.edu; h=mime-version : from\r\n : date : message-id : subject : to : content-type; s=pps01;\r\n bh=E5e/tPsGez25P3XCC9SiKkFAEKypTXetgeJqB1KszLw=;\r\n b=C7wkG5HikcoV3daLBKa0/GGHoh7kos/Vxd8kIKztmZuVHN3/JPx/17as8U3Pp+lYMZd5\r\n I/ok4GuRHoVXuIm8vYjsb4r351MZCfpLTilWjNmoRElpum0AuSYoNj09IyrDjkPvyzav\r\n /owMYezOBurYSmxG2OZu4+xT9qFTo29klzPZSS2Xyl5WDMqvpeQkZjURPDITJup3dTIj\r\n 8skGBEIEPNssPFsMQnzM/XiCF1h3N2m9un8d+w0uwaNeQhoY+HSV/ULhYqk4AF99x8Hc\r\n qL0Dn3Q2U1wcany/zHGk/7Pr647fLsWoyKa3yZek/m5FIOVEuqz5KU4+OjA1UxAU0yfK Qg== \r\nReceived: from sendprdmail20.cc.columbia.edu (sendprdmail20.cc.columbia.edu [128.59.72.22])\r\n\tby mx0a-00364e01.pphosted.com (PPS) with ESMTPS id 3m9ptrtquv-1\r\n\t(version=TLSv1.2 cipher=ECDHE-RSA-AES256-GCM-SHA384 bits=256 verify=NOT)\r\n\tfor <daniel.wei@crusiom.com>; Mon, 05 Dec 2022 20:49:17 -0500\r\nReceived: from mail-pl1-f198.google.com (mail-pl1-f198.google.com [209.85.214.198])\r\n\tby sendprdmail20.cc.columbia.edu (8.14.7/8.14.4) with ESMTP id 2B61n6pY117102\r\n\t(version=TLSv1/SSLv3 cipher=ECDHE-RSA-AES128-GCM-SHA256 bits=128 verify=NOT)\r\n\tfor <daniel.wei@crusiom.com>; Mon, 5 Dec 2022 20:49:15 -0500\r\nReceived: by mail-pl1-f198.google.com with SMTP id m1-20020a170902db0100b00188eec2726cso15028018plx.18\r\n        for <daniel.wei@crusiom.com>; Mon, 05 Dec 2022 17:49:15 -0800 (PST)\r\nX-Google-DKIM-Signature: v=1; a=rsa-sha256; c=relaxed/relaxed;\r\n        d=1e100.net; s=20210112;\r\n        h=to:subject:message-id:date:from:mime-version:x-gm-message-state\r\n         :from:to:cc:subject:date:message-id:reply-to;\r\n        bh=E5e/tPsGez25P3XCC9SiKkFAEKypTXetgeJqB1KszLw=;\r\n        b=Wp+uzuV8Ahgv0m18L4sEo/sN7vJfQtfz8ypHEFfd+m4NB2KnijC+L/qZ2z3yDwv/n1\r\n         UdfFaWioCnI6b1IpSTgA328t/ecJ3wJHggEbvpRiynIajUed0+ZI86bQPx2ix9EX/HOv\r\n         AgHA6RweycD82+Xese4HXr5wFfFuaWWuSHwQ7xQ19TpMAksc/2ZDo943N1/SVjgPXTaU\r\n         DxiB0Q/g4npcdnyeT3poMZXHSuQm4/Igv2MKCjg/Ga7Vl53Y/87h3TJ8jd30y+FvD0Fc\r\n         Zrz0ygu76sHHuuDpEtaeJKx7fCQz8T8m88Ug82Q+ydIq2XSHPnw/QW2RkI+DP/JqiIXt\r\n         nsKQ==\r\nX-Gm-Message-State: ANoB5pksnyI9udqTSZeilPbNVdm+wbjHf+BOhbaMF2xa11C2hZC6uGSZ\r\n\tqxrQ0aBf3IQfLFqPZe7YM1y5NCxRu8Djc7kFJ5q94OcYjtPZV3VgKUKHEbSdkYhb75ALksNVVO+\r\n\tYnrPY5Suomz4hIp5EQ9j6LnxWqiNnK7v1KiU/Cxvg\r\nX-Received: by 2002:a17:902:ce04:b0:187:3a54:9b93 with SMTP id k4-20020a170902ce0400b001873a549b93mr77161169plg.2.1670291355415;\r\n        Mon, 05 Dec 2022 17:49:15 -0800 (PST)\r\nX-Google-Smtp-Source: AA0mqf5WMlG6LtbPFJ2DAuS+ACv5Ruyp/bqyfqkUYV0QxIMK3y6dgc6TglMWu2w8a1em8sdewou7CvxblakL8fykPJQ=\r\nX-Received: by 2002:a17:902:ce04:b0:187:3a54:9b93 with SMTP id\r\n k4-20020a170902ce0400b001873a549b93mr77161145plg.2.1670291354947; Mon, 05 Dec\r\n 2022 17:49:14 -0800 (PST)\r\nMIME-Version: 1.0\r\nFrom: Danling Wei <dw3033@columbia.edu>\r\nDate: Mon, 5 Dec 2022 20:49:04 -0500\r\nMessage-ID: <CAMbApXq_AEZhebCApcMG_Vunn5F-Z1iOCLPUNuYJL8DPhj8-kA@mail.gmail.com>\r\nSubject: test\r\nTo: daniel.wei@crusiom.com\r\nContent-Type: multipart/alternative; boundary="000000000000e22a8405ef1f0219"\r\nX-Proofpoint-GUID: Yas7tp08ub2Rzr82UY-8Z8XUWu8cH0Cq\r\nX-Proofpoint-ORIG-GUID: Yas7tp08ub2Rzr82UY-8Z8XUWu8cH0Cq\r\nX-CU-OB: Yes\r\nX-Proofpoint-Virus-Version: vendor=baseguard\r\n engine=ICAP:2.0.205,Aquarius:18.0.923,Hydra:6.0.545,FMLib:17.11.122.1\r\n definitions=2022-12-05_01,2022-12-05_01,2022-06-22_01\r\nX-Proofpoint-Spam-Details: rule=outbound_notspam policy=outbound score=0 clxscore=1015 adultscore=0\r\n mlxscore=0 suspectscore=0 priorityscore=1501 lowpriorityscore=10\r\n spamscore=0 phishscore=0 mlxlogscore=359 malwarescore=0 impostorscore=10\r\n bulkscore=10 classifier=spam adjust=0 reason=mlx scancount=1\r\n engine=8.12.0-2210170000 definitions=main-2212060008\r\n\r\n--000000000000e22a8405ef1f0219\r\nContent-Type: text/plain; charset="UTF-8"\r\n\r\ntest\r\nasdadasd\r\nsa\r\nda\r\ns\r\nda\r\nsd\r\nas\r\n\r\nDanling Wei\r\nM.S. in Electrical Engineering\r\nColumbia University, 2023\r\ndw3033@columbia.edu | 646.634.7777\r\n\r\n--000000000000e22a8405ef1f0219\r\nContent-Type: text/html; charset="UTF-8"\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n<div dir=3D"ltr">test<div>asdadasd</div><div>sa</div><div>da</div><div>s</d=\r\niv><div>da</div><div>sd</div><div>as</div><div><br clear=3D"all"><div><div =\r\ndir=3D"ltr" class=3D"gmail_signature" data-smartmail=3D"gmail_signature"><d=\r\niv dir=3D"ltr">Danling Wei<div>M.S. in Electrical Engineering</div><div>Col=\r\numbia University, 2023</div><div><a href=3D"mailto:dw3033@columbia.edu" tar=\r\nget=3D"_blank">dw3033@columbia.edu</a> | 646.634.7777</div></div></div></di=\r\nv></div></div>\r\n\r\n--000000000000e22a8405ef1f0219--\r\n'

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # # Get email object of raw binary email.
    
    raw_email = s3.get_object(Bucket=bucket, Key=key)
    email_obj = email.message_from_bytes(raw_email['Body'].read())
    
    # According to the traning tutorial, deal with the raw data
    mail_content = [email_obj.get_payload()[0].get_payload().strip()]

    temp_content = one_hot_encode(mail_content, 9013)
    raw_data = vectorize_sequences(temp_content, 9013)
    
    # Raw data is changed to final data, then send it to endpoint
    data = json.dumps(raw_data.tolist())
    
    res = runtime.invoke_endpoint(EndpointName="sms-spam-classifier-mxnet-2022-12-09-01-54-28-510",
                                      ContentType="application/json", Body=data)
    res = json.loads(res["Body"].read())
    believe = res['predicted_probability'][0][0] * 100
    
    if res['predicted_label'][0][0] == 0:
        # Not spam
        ml_result = "NOT spam"
    else:
        # Spam
        ml_result = "SPAM"

    message = "We received your email sent at " +  str(email_obj.get('Date')) +  " with the subject " + str(email_obj.get('Subject')) + ".\n Here is a 240 character sample of the email body:"+  mail_content[0][:240] +" \nThe email was categorized as " + ml_result + " with a " + str(believe) + "% confidence."
    response = email_client.send_email(
        Destination={'ToAddresses': [email_obj.get('From')]}, 
        Message={
            "Body": {
                "Text": {
                    "Charset": "UTF-8",
                    "Data": message
                },
            },
            "Subject": {
                "Charset": "UTF-8",
                "Data": "Spam Detecting Result"
            },
        },
        Source = str(email_obj.get('To'))
    )
    return {
        'statusCode': 200,
        'body': response
    }