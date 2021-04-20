import win32com.client as win32

class Mail():
	"""Outlook mail sender"""
	def __init__(self, to, copy = '', theme = '', body = '', attach = False):
		super(Mail, self).__init__()
		self.outlook = win32.Dispatch('outlook.application')
		self.mail = self.outlook.CreateItem(0)
		self.mail.To = to
		self.mail.CC = str(copy)
		self.mail.Body = str(body) + '''Группа отчётности и аналитики\nОтдел отчётности, аналитики и управления эксплуатацией\nРостелеком Контакт-центр'''

		self.mail.Subject = theme
		if attach:
			for att in attach:
				self.mail.Attachments.Add(att)

	def send_mail(self):
		self.mail.Send()
