import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import pandas as pd
from jsoncustom.jsontagncommonvariables import JsonTagAndCommonVariables as js_var
from os import path


class EMailSummaryLogAttachments():

    def __init__(self,
                 smtp_server_host,
                 port,
                 sender_email_id,
                 receiver_emails,
                 email_subject
                 ):
        self.sender_email_id = sender_email_id
        self.receivers_email_id = receiver_emails
        self.email_subject = email_subject
        self.smtp_server_host = smtp_server_host
        self.port = port

    def __initialise_attachment_NBFEmail_Content(self,
                                                 attachment_file,
                                                 email_message
                                                 ):
        attachment = MIMEBase("application", "octet-stream")
        attachment.set_payload(open(attachment_file, "rb").read())
        encoders.encode_base64(attachment)
        file_name = path.basename(attachment_file)
        attachment.add_header("Content-Disposition", 'Attachements', filename=file_name)
        email_message.attach(attachment)

    def __analyse_status_of_load(self, body_of_content, log_input):
        status = list()
        for status_input in log_input:
            status.append(status_input[js_var.result_of_load])
        status_result = 'success' if js_var.failed_load not in status else 'failed'
        status_message = body_of_content.format(status_result)
        return status_message

    def __construct_body_Fmail(self, input_csv):
        csv_file = pd.read_csv(input_csv)
        html_file = csv_file.to_html(index=False)
        return html_file

    def send_load_status_viamail(self, summary_log_input, attachment_file, log_directory):
        boc_csv_file = log_directory + attachment_file[js_var.overall_summary]
        email_attachment_csv_file = log_directory + attachment_file[js_var.individual_summary]
        final_email_message = MIMEMultipart()
        final_email_message['From'] = self.sender_email_id
        final_email_message['To'] = ",".join(self.receivers_email_id)

        final_email_message['Subject'] = self.__analyse_status_of_load(self.email_subject, summary_log_input)
        boc = self.__construct_body_Fmail(boc_csv_file)
        final_email_message.attach(MIMEText(boc, 'html'))
        self.__initialise_attachment_NBFEmail_Content(email_attachment_csv_file, final_email_message)
        self.send_mail(final_email_message['From'], self.receivers_email_id, final_email_message.as_string())

    def send_load_start_viamail(self, load_start_subject):
        loadstart_email_message = MIMEMultipart()
        loadstart_email_message['From'] = self.sender_email_id
        loadstart_email_message['To'] = ",".join(self.receivers_email_id)
        loadstart_email_message['Subject'] = load_start_subject
        self.send_mail(loadstart_email_message['From'], self.receivers_email_id, loadstart_email_message.as_string())

    def send_mail(self, sender_mail_id, receivers_email_id, message):
        smtp_server = smtplib.SMTP()
        #    s.set_debuglevel(1)
        smtp_server.connect(host=self.smtp_server_host, port=self.port)
        smtp_server.sendmail(sender_mail_id, receivers_email_id, message)
        smtp_server.close()
