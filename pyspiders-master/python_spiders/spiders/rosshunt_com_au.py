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
    name = 'rosshunt_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    
    custom_settings={"HTTPCACHE_ENABLED":False}

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.rosshunt.com.au/properties-for-rent/?property_category=House",
                    "https://www.rosshunt.com.au/properties-for-rent/?property_category=Townhouse",
                    "https://www.rosshunt.com.au/properties-for-rent/?property_category=Villa"
                    
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.rosshunt.com.au/properties-for-rent/?property_category=Unit",
                    "https://www.rosshunt.com.au/properties-for-rent/?property_category=Apartment"
                    
                ],
                "property_type": "apartment"
            },
        ]
        for item in start_urls:
            for url in item.get('url'):
                yield Request(
                    url,
                    callback=self.parse,
                    meta={
                        "property_type": item.get('property_type')
                    }
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='item']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
            seen = True
            
        if page==2 or seen:
            f_url = response.url.replace(f"rent/page/{page-1}/", "rent/").replace("rent/", f"rent/page/{page}/")
            yield Request(f_url, self.parse, meta={"property_type": response.meta.get('property_type')})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Rosshunt_Com_PySpider_australia")

        title = response.xpath("//div[@class='main_title']/text()").get()
        item_loader.add_value("title", title)

        deposit = response.xpath("//span[contains(.,'Bond')]/text()").get()
        if deposit:
            deposit = deposit.split("$")[1].strip().replace(",","")
            item_loader.add_value("deposit", deposit)
        
        rent = response.xpath("//span[@class='page-price']/text()").get()
        if rent:
            try:
                price = rent.split("$")[1].split(" ")[0].replace(",","")
                item_loader.add_value("rent", int(price)*4)
            except: pass
        else:
            rent = response.xpath("//h2[contains(@class,'clearfix')]//text()[contains(.,'$')]").get()
            if rent:
                price = rent.split("$")[1].split(" ")[0].replace(",","")
                item_loader.add_value("rent", price)
       
        item_loader.add_value("currency", "USD")

        address = response.xpath("//span[@class='h1']/text()").get()
        if address:
            city = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
        
        room_count = response.xpath("//li[img[contains(@src,'icon1')]]/span/text()").get()
        item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[img[contains(@src,'icon2')]]/span/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//li[img[contains(@src,'icon4')]]/span/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        desc = " ".join(response.xpath("//div[contains(@id,'1_container')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude_longitude:
            latitude_longitude = "{"+latitude_longitude.split('geo":{')[1].split('},')[0].strip()+"}"
            data = json.loads(latitude_longitude)
            item_loader.add_value("longitude", data["longitude"])
            item_loader.add_value("latitude", data["latitude"])
        
        import dateparser
        available_date = response.xpath("//span[contains(.,'Available from')]/text()").get()
        if available_date:
            match = re.search(r'(\d+/\d+/\d+)', available_date)
            if match:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        images = [x for x in response.xpath("//div[@id='detail_slider']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        landlord_name = "".join(response.xpath("//div[contains(@class,'detail_client_box')][2]//a[contains(@class,'client')]/text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        else:
            item_loader.add_xpath("landlord_name", "//a[@class='detail_client_title bodoni-font']/text()")
        
        landlord_phone = response.xpath("//div[contains(@class,'detail_client_box')][2]//a[contains(@href,'tel')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_xpath("landlord_phone", "//p/a[contains(@href,'tel')]/@title")
            
        landlord_email = response.xpath("//div[contains(@class,'detail_client_box')][2]//a[contains(@href,'mail')]/@href").get()
        if landlord_email:
            landlord_email = landlord_email.split(":")[1].strip()
            item_loader.add_value("landlord_email", landlord_email)
        else:
            landlord_email = response.xpath("//p/a[contains(@href,'mail')]/@href").get()
            if landlord_email: item_loader.add_value("landlord_email", landlord_email.split(":")[1])
            
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