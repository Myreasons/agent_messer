import win32com.client
import Settings as s
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)


def adress_security_checker(adress):
    rtk = [] #List of secured addresses
    for i in rtk:
        if i in adress:
            return True
    return False


def looking_for_mails():
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6).Folders['Системные отчеты'] #'Системные отчеты' or any folder you want
    # "6" refers to the index of a folder - in this case,
    # the inbox. You can change that number to reference
    # any other folder
    messages = inbox.Items
    message = messages.GetLast()

    # Looking patterns in Settings in Outlook mail subjects
    # On succes adding params into task list
    tasks = []

    for item in inbox.Items:
        print(item.body)
        for pattern in s.functions_map.keys():
            subj = item.subject.lower()

            if (pattern in subj) and adress_security_checker(item.Sender.Address):
                attach = item.Attachments

                if item.SenderEmailType == 'EX':
                    task_customer = item.Sender.GetExchangeUser().PrimarySmtpAddress
                else:
                    task_customer = item.SenderEmailAddress

                tasks.insert(0, s.Task(name=pattern,
                                       func=s.functions_map[pattern],
                                       customer=task_customer,
                                       system='Outlook',
                                       params=[item.subject, attach, item.body, item.SentOn])
                             )
                item.delete()
                break

    return tasks
