# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'azetapm_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source="Azetapm_Com_PySpider_australia"
    def start_requests(self):
        start_url = "https://app.inspectrealestate.com.au/External/ROL/QuickWeb.aspx?AgentAccountName=azetapm&HidePrice=&UsePriceView=&HideAppOffer=&Sort="
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='divBorder' and not(@id) and not(@style)]"):
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_source", self.external_source)
            property_type = "".join(item.xpath(".//div[@class='divHouseDescription']//text()").getall())
            if property_type:
                if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
                else: continue
            item_loader.add_value("external_link", response.url)
            
            title = item.xpath(".//div[@class='divHeadline']/text()").get()
            if title:
                item_loader.add_value("title", title.strip())
            
            address = item.xpath("./div[@id='divTitle']/span/text()").get()
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("city", address.split(",")[-1].strip())
            
            rent = item.xpath(".//span[contains(@id,'lblPrice_')]/text()").get()
            if rent:
                price = rent.replace("$","").replace(",","").strip()
                item_loader.add_value("rent", int(float(price))*4)
            item_loader.add_value("currency", "AUD")
            
            room_count = item.xpath(".//td/img[contains(@src,'bed')]/parent::td/following-sibling::td[1]/span/text()").get()
            item_loader.add_value("room_count", room_count)
            
            bathroom_count = item.xpath(".//td/img[contains(@src,'Shower')]/parent::td/following-sibling::td[1]/span/text()").get()
            item_loader.add_value("bathroom_count", bathroom_count)
            
            desc = " ".join(item.xpath(".//div[@class='divHouseDescription']//text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)
            
            images = [x for x in item.xpath(".//div[@class='imgPropertyCrop']//@src").getall()]
            if images:
                item_loader.add_value("images", images)
            
            parking = item.xpath(".//td/img[contains(@src,'Car')]/parent::td/following-sibling::td[1]/span/text()[.!='0']").get()
            if parking:
                item_loader.add_value("parking", True)

            from datetime import datetime
            import dateparser
            available_date = item.xpath(".//td[contains(.,'Available')]/span/text()").get()
            if available_date:
                if "now" in available_date.lower():
                    item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
            item_loader.add_value("landlord_name", "Azeta Property Management")
            item_loader.add_value("landlord_phone", "03 9999 7990")
            
            yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None