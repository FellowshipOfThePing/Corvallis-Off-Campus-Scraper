from mako.template import Template
import json


class ScrapeReport:
	"""Renders HTML page populated with Scrape Data"""

	def __init__(self, json_data):
		"""Initializes data members"""
		self._json_data = json_data
		self._rendered_html = None

	def render_html(self):
		"""Renders template based on json data"""
		mytemplate = Template(filename="template.html")
		self._rendered_html = mytemplate.render(data=self._json_data)

	def write_to_file(self):
		"""Writes rendered html to file"""
		with open("ScrapeReport.html", 'w') as sr:
			if self._rendered_html:
				sr.write(self._rendered_html)

	def generate_report(self):
		"""Execute rendering"""
		self.render_html()
		self.write_to_file()


with open("stats_log.json", 'r') as jf:
	data = json.load(jf)

report = ScrapeReport(data["skipped_by_type"])
report.generate_report()