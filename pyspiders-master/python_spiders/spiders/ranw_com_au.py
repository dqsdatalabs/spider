# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'ranw_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source="Ranw_Com_PySpider_australia"
    url = "https://ranw.com.au/property.html"
    headers = {
        'authority': 'ranw.com.au',
        'content-type': 'application/x-www-form-urlencoded',
        'accept': '*/*',
        'origin': 'https://ranw.com.au',
        'referer': 'https://ranw.com.au/property.html?Type=rent&SalesCategoryID=RESIDENTIAL_LEASE&RegionID=&Address=&PropertyTypeID=3&Bedrooms=&Bathrooms=&CarSpaces=&MinPrice=&MaxPrice=&history[0]=/&act=act_index',
        'accept-language': 'tr,en;q=0.9'
    }
    formdata = {
        "act": "act_fgxml",
        "25[offset]": "0",
        "25[perpage]": "1", # max_page
        "SalesStageID[0]": "LISTED_PT",
        "SalesStageID[1]": "LISTED_LEASE",
        "SalesStageID[2]": "LISTED_AUCTION",
        "SalesStageID[3]": "DEPOSIT",
        "SalesStageID[4]": "BOND",
        "SalesStageID[5]": "EXCHANGED_UNREPORTED",
        "SalesCategoryID": "RESIDENTIAL_LEASE",
        "Type": "rent",
        "require": "0",
        "fgpid": "25",
        "ajax": "1"
    }

    def start_requests(self):
        yield FormRequest(self.url, headers=self.headers, formdata=self.formdata, dont_filter=True, callback=self.reload)
    
    def reload(self, response):
        max_page = response.xpath("//totalrows/text()").get()
        self.formdata["25[perpage]"] = str(max_page)
        yield FormRequest(self.url, headers=self.headers, formdata=self.formdata, dont_filter=True, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//row/url/text()").getall():
            follow_url = "https://ranw.com.au" + item
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//title/text()").get()
        if property_type:
            if get_p_type_string(property_type.split('-')[0]): item_loader.add_value("property_type", get_p_type_string(property_type.split('-')[0]))
            elif get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("/")[-1].split(".")[0])

        title = response.xpath("//h1[contains(@class,'heading heading-wrap')]//text()").get()
        item_loader.add_value("title",title)

        address = response.xpath("//div[contains(@class,'box')]//h2//text()").get()
        if address:
            item_loader.add_value("address", address)
            
            if "street" in address.lower():
                city = address.lower().split("street")[1].strip()
                item_loader.add_value("city", city)
            else:
                item_loader.add_value("city", address.strip().split(" ")[-1])

        rent = response.xpath("//div[@class='label' and contains(.,'Price')]/following-sibling::div[@class='text'][1]/text()").get()
        if rent and not "price" in rent.lower():
            rent = rent.split("$")[1].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//div[contains(text(),'Bond')]/following-sibling::div[1]/text()").get()
        if deposit: item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit.split('.')[0])))

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[@class='label' and contains(.,'Available')]/following-sibling::div[@class='text'][1]/text()").get())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.split(",")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        desc = "".join(response.xpath("//div[@class='fluidgrid-cell fluidgrid-cell-4 tablet-fluidgrid-cell-2']//text()").getall())
        item_loader.add_value("description", desc)

        features = response.xpath("//div[contains(@class,'box')]//h2/following-sibling::div[@class='text'][1]/text()").get()
        if "bed" in features:
            room_count = features.split("bed")[0].strip()
            item_loader.add_value("room_count", room_count)
        elif "studio" in features.lower():
            item_loader.add_value("room_count","1")

        if "bath" in features:
            bathroom_count = features.split("bath")[0].strip().split(" ")[-1]
            item_loader.add_value("bathroom_count", bathroom_count)
        if "car" in features:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//text()[contains(.,'Furnished/Unfurnished:')]").get()
        if furnished:
            if 'unfurnished' in furnished.split(':')[-1].strip().lower(): item_loader.add_value("furnished", False)
            elif 'furnished' in furnished.split(':')[-1].strip().lower(): item_loader.add_value("furnished", True)
        
        images = [x for x in response.xpath("//div[contains(@class,'imagecarousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = response.xpath("//div[contains(@class,'box-light-inner')]//h3//text()").get()
        item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[contains(@class,'box-light-inner')]//a//text()").get()
        item_loader.add_value("landlord_phone", landlord_phone)

        item_loader.add_value("landlord_email", "parramatta@ranw.com.au")


        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None