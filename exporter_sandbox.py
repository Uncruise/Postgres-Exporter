"""Export Module for ODBC"""
def main():
    """Main entry point"""

    import sys
    from os.path import join

    salesforce_type = str(sys.argv[1])
    client_type = str(sys.argv[2])
    client_subtype = str(sys.argv[3])
    client_emaillist = str(sys.argv[4])

    if len(sys.argv) < 4:
        print ("Calling error - missing inputs.  Expecting " +
               "salesforce_type client_type client_emaillist\n")
        return

    exporter_root = "C:\\repo\\ODBC-Exporter-Private\\Clients\\{}\\ODBC-Exporter".format(client_type)
    if '-rootdir' in sys.argv:
        exporter_root = sys.argv[sys.argv.index('-rootdir') + 1]

    emailattachments = False
    if '-emailattachments' in sys.argv:
        emailattachments = True

    emailonsuccess = False
    if '-emailonsuccess' in sys.argv:
        emailonsuccess = True

    interactivemode = False
    if '-interactivemode' in sys.argv:
        interactivemode = True

    # Setup Logging to File
    sys_stdout_previous_state = sys.stdout
    if not interactivemode:
        sys.stdout = open(join(exporter_root, '..\\exporter.log'), 'w')

    print('ODBC Exporter Startup')

    exporter_directory = join(exporter_root, "Clients\\" + client_type)
    print "Setting ODBC Exporter Directory: " + exporter_directory

    # Export Data
    print "\n\nODBC Exporter - Export Data Process\n\n"
    status_export = process_data(exporter_directory, salesforce_type, client_type, client_subtype, client_emaillist, sys_stdout_previous_state, emailattachments, emailonsuccess)

    print "ODBC Exporter process completed\n"

    if "Error" in status_export:
        sys.exit()

def process_data(exporter_directory, salesforce_type, client_type, client_subtype, client_emaillist, sys_stdout_previous_state, emailattachments, emailonsuccess):
    """Process Data based on data_mode"""

    import sys
    from os import makedirs
    from os.path import exists, join

    sendto = client_emaillist.split(";")
    user = 'db.powerbi@501commons.org'
    smtpsrv = "smtp.office365.com"
    subject = "{} Export ODBC Data Results -".format(client_type)
    file_path = exporter_directory + "\\Status"
    if not exists(file_path):
        makedirs(file_path)
    export_path = exporter_directory + "\\Export"
    if not exists(export_path):
        makedirs(export_path)

    output_log = "Export Data\n\n"

    status_export = ""
    
    # Export data from Salesforce
    try:
        if not "Error" in subject:
            status_export = export_dataloader(exporter_directory,
                                              client_type, client_subtype,
                                              salesforce_type)
        else:
            status_export = "Error detected so skipped"
    except Exception as ex:
        subject += " Error ODBC Export"
        output_log += "\n\nUnexpected export error:" + str(ex)
    else:
        output_log += "\n\nExport\n" + status_export

    # Restore stdout
    sys.stdout = sys_stdout_previous_state

    with open(join(exporter_directory, "..\\..\\..\\exporter.log"), 'r') as exportlog:
        output_log += exportlog.read()

    import datetime
    date_tag = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with open(join(file_path, "ODBC-Exporter-Log-{}.txt".format(date_tag)),
              "w") as text_file:
        text_file.write(output_log)

    #Write log to stdout
    print output_log

    if not "Error" in subject:
        subject += " Successful"

        if not emailonsuccess:
            return status_export

    # Send email results
    send_email(user, sendto, subject, file_path, smtpsrv, emailattachments)

    return status_export

def contains_data(file_name):
    """Check if file contains data after header"""

    line_index = 1
    with open(file_name) as file_open:
        for line in file_open:
            # Check if line empty
            line_check = line.replace(",", "")
            line_check = line_check.replace('"', '')
            if (line_index == 2 and line_check != "\n"):
                return True
            elif line_index > 2:
                return True

            line_index += 1

    return False

def export_dataloader(exporter_directory, client_type, client_subtype, salesforce_type):
    """Export out of ODBC using SQL Query files"""

    import csv
    import unicodedata
    import pyodbc
    import os
    from os import listdir
    from os import makedirs
    from os.path import exists
    from os.path import join

    connectionType = client_subtype
    query_path = exporter_directory + "\\Queries"
    csv_path = exporter_directory + "\\Export\\"
    if not exists(csv_path):
        makedirs(csv_path)

    return_status = ""

    for file_name in listdir(query_path):
        if not (salesforce_type + ".sql") in file_name:
            continue

        export_name = os.path.splitext(file_name)[0]
        csv_name = join(csv_path, export_name + ".csv")

        message = "Starting Export Process: " + file_name
        print message

        # Read SQL Query
        with open(join(query_path, file_name), 'r') as sqlqueryfile:
            sqlquery=sqlqueryfile.read().replace('\n', ' ')

        # Get ODBC Connection
        if not client_subtype in file_name:
            continue

        with open(join(query_path, "..\\odbc_connect_" + connectionType + ".dat"), 'r') as odbcconnectfile:
            odbc_connect=odbcconnectfile.read().replace('\n', '').rstrip()

        # Query ODBC and write to CSV
        conn = pyodbc.connect(odbc_connect)
        crsr = conn.cursor()
        rows = crsr.execute(sqlquery)

        with open(csv_name, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([x[0] for x in crsr.description])  # column headers
            for row in rows:
                
                updated_row = list()
                for column in row:
                    if not column is None and isinstance(column, basestring):

                        # Check for newline in string
                        column = column.replace("\r", "")

                        # Left Double Quotation Mark and Right Double Quoation Mark
                        column = column.replace(u"\u201c", "(").replace(u"\u201d", ")")

                        # Left Single Quotation Mark and Right Single Quotation Mark
                        column = column.replace(u"\u2018", "(").replace(u"\u2019", ")")

                        # Quotation Mark
                        column = column.replace(u"\u0022", "")

                        # Apostrophe
                        column = column.replace(u"\u0027", "")

                        # Grave Accent
                        column = column.replace(u"\u0060", "")

                        # Acute Accent
                        column = column.replace(u"\u00B4", "")

                        # Normalize to Ascii
                        column = unicodedata.normalize('NFKD', column).encode('ascii','ignore')

                    elif not column is None and isinstance(column, float):

                        # Convert float to integer
                        column = int(column)

                    updated_row.append(column)

                writer.writerow(updated_row)

        if "error" in return_status:
            raise Exception("error export file",
                            ("ODBC Export Error: " + return_status))

    return return_status

def file_linecount(file_name):
    """Count how many lines after the header"""

    # set index to -1 so the header is not counted
    line_index = -1
    with open(file_name) as file_open:
        for line in file_open:
            if line:
                line_index += 1

    return line_index

def send_email(send_from, send_to, subject, file_path, server, emailattachments):
    """Send email via O365"""

    #https://stackoverflow.com/questions/3362600/how-to-send-email-attachments
    import base64
    import os
    import smtplib
    from os.path import basename
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.utils import COMMASPACE, formatdate

    msg = MIMEMultipart()

    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    from os import listdir
    from os.path import isfile, join

    msgbody = subject + "\n\n"
    if not emailattachments:
        msgbody += "Attachments disabled: Result files can be accessed on the import server.\n\n"

    onlyfiles = [join(file_path, f) for f in listdir(file_path)
                 if isfile(join(file_path, f))]

    for file_name in onlyfiles:
        if contains_data(file_name):

            msgbody += "\t{}, with {} rows\n".format(file_name, file_linecount(file_name))

            if emailattachments or ("error" in subject.lower() and "log" in file_name.lower()):
                with open(file_name, "rb") as file_name_open:
                    part = MIMEApplication(
                        file_name_open.read(),
                        Name=basename(file_name)
                        )

                # After the file is closed
                part['Content-Disposition'] = 'attachment; filename="%s"' % basename(file_name)
                msg.attach(part)

    msg.attach(MIMEText(msgbody))

    server = smtplib.SMTP(server, 587)
    server.starttls()
    server_password = os.environ['SERVER_EMAIL_PASSWORD']
    server.login(send_from, base64.b64decode(server_password))
    text = msg.as_string()
    server.sendmail(send_from, send_to, text)
    server.quit()

def send_salesforce():
    """Send results to Salesforce to handle notifications"""
    #Future update to send to salesforce to handle notifications instead of send_email
    #https://developer.salesforce.com/blogs/developer-relations/2014/01/python-and-the-force-com-rest-api-simple-simple-salesforce-example.html

if __name__ == "__main__":
    main()
