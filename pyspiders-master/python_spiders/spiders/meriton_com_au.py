# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

# from tkinter.font import ROMAN
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'meriton_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Meriton_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        headers={
            "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Mobile Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }

        start_urls = [
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD0x"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD0y"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD0z"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD00"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD01"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD02"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD03"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD04"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD05"},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD0xMA=="},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD0xMQ=="},
            {"url": "https://www.meriton.com.au/wp-admin/admin-ajax.php?action=postfilter&args=cmVudGJ1eT1yZW50JnBzdGF0ZT0mcHN1YnVyYj0mcGZyb21wcmljZT0mcHRvcHJpY2U9JnBiZWRyb29tcz0mcGNhcnNwYWNlcz0mcHNvcnQ9dW5kZWZpbmVkJnBwYWdlZD0xMg=="},
        ] 
         # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,headers=headers)
    # 1. FOLLOWING
    def parse(self, response):
        data=str(response.body).replace('{"html":',"{").replace("\n","").split(',"status_total":"')[0].replace("\\n        \\n\\t\\t\\t\\n            \\n            \\n        \\n            ","")
        for item in data.split("mobile_show apt_read_more_link"):
            item=item.split("More information")[0].split("href")[-1].replace('=\\"',"").replace('">-',"").replace("\\","").replace('="',"").replace('"',"").strip().split("?i_beds")[0]
            yield Request(item, callback=self.populate_item)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres="".join(response.xpath("//div[@class='ad-address']//text()").getall())
        if adres:
            item_loader.add_value("address",adres)
        rent=response.xpath("//div[@class='detail-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("From")[-1].split("$")[-1].strip())
        item_loader.add_value("currency","USD")
        description=response.xpath("//h3[contains(.,'About')]/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//li//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        parking=response.xpath("//span[@class='i-parking14']").get()
        if parking:
            item_loader.add_value("parking",True)
        name=response.xpath("//div[@class=' select_agent']/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class=' select_agent']/span/a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)

        yield item_loader.load_item()