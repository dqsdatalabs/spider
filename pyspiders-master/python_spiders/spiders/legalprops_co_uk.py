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
    name = 'legalprops_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['http://www.legalprops.co.uk/rental_list.cfm?offset=1&searchPrice=&searchBedrooms=&searchType=']  # LEVEL 1
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        seen = False
        for item in response.xpath("//div[contains(@class,'property-listing')]"):
            follow_url = response.urljoin(item.xpath(".//div[@class='property-list-image']/a/@href").get())
            prop_type = item.xpath(".//div[@class='propertyAddress'][2]/text()").get()
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})
            seen = True
        
        if page == 1 or seen:
            url = f"http://www.legalprops.co.uk/rental_list.cfm?offset={page}1&searchPrice=&searchBedrooms=&searchType="
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("PropertyID=")[1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Legalprops_Co_PySpider_united_kingdom")
        
        title = response.xpath("//td/div[@class='propertyAddress']/text()").getall()
        if title:
            item_loader.add_value("title", f"{title[0]} {title[1]}")
            
        room_count = response.xpath("//td[@class='tblTitle'][contains(.,'Bed')]/following-sibling::td/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        address = response.xpath("//tr[contains(.,'Address')]/following-sibling::tr//text()").getall()
        if address:
            addr = "".join(address)
            item_loader.add_value("address", re.sub('\s{2,}', ' ', addr.strip()))
            item_loader.add_value("city", address[-3].strip())
            item_loader.add_value("zipcode", address[-2].strip())
        
        rent = response.xpath("//div[@class='propertyPrice']/text()").get()
        if rent:
            rent = rent.split("pcm")[0].split("£")[-1].replace(",","").strip()
            item_loader.add_value("rent", rent)
        
        item_loader.add_value("currency", "GBP")
        deposit = response.xpath("//li[contains(.,'Deposit') or contains(.,'deposit')]//text()").get()
        if deposit:
            deposit = deposit.split(" ")[0].replace("£","")
            item_loader.add_value("deposit", int(float(deposit)))
        
        import dateparser
        available_date = response.xpath("//td[@class='tblTitle'][contains(.,'Available')]/following-sibling::td/text()").get()
        if available_date and "now" not in available_date.lower():
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        else:
            available_date = " ".join(response.xpath("//li[contains(.,'Available')]//text()").getall())
            if available_date:
                available_date = available_date.split("Available")[-1].replace("from","").strip()
                if " " in available_date:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2) 
        
        
        floor = response.xpath("//li[contains(.,'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip().split(" ")[-1])
        
        description = " ".join(response.xpath("//div[@class='pdBody']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//div[@class='slidey']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        terrace = response.xpath("//td[@class='tblValue'][contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//td[@class='tblTitle'][contains(.,'Furnished')]/following-sibling::td/text()[not(contains(.,'Un'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        item_loader.add_value("landlord_name", "Legal Property Services")
        item_loader.add_value("landlord_phone", "+44 (0)121 423 2301")
        item_loader.add_value("landlord_email", "info@legalprops.co.uk")
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None