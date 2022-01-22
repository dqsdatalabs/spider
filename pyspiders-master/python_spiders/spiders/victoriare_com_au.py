# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

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
    name = 'victoriare_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Victoriare_Com_PySpider_australia"
    start_urls = ["https://vicrea.com.au/?s=&qodef-property-search=yes&qodef-search-type=&qodef-search-city=&qodef-search-status="]
    
    custom_settings = {
        "PROXY_ON": True
    }

    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://vicrea.com.au/property-type/apartment/",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://vicrea.com.au/property-type/house/"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )
    
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article[contains(@class,'qodef-pl-item')]"):
            status = item.xpath(".//span[@class='qodef-property-status']/text()").get()
            data = {}
            if status and "rent" in status.lower():
                f_url = response.urljoin(item.xpath(".//a[contains(@class,'drag-link')]/@href").get())
                rent = item.xpath(".//span[@class='qodef-property-price-value']/text()").get()
                city = item.xpath(".//span[@class='qodef-item-city']/text()").get()
                city2 = " ".join(item.xpath(".//span[@class='qodef-item-city']/text()").getall())
                
                address = "".join(item.xpath(".//div[@class='qodef-item-id-title']/h4/text()").getall())
                if address:
                    address = re.sub('\s{2,}', ' ', address.strip())
                
                external_id = item.xpath(".//h4[@class='qodef-property-id']/text()").get()
                
                data["rent"] = rent
                data["city"] = city
                data["address"] = f"{address} {city2}"
                data["external_id"] = external_id
                
                yield Request(
                    f_url,
                    callback=self.populate_item,
                    meta={"property_type": response.meta.get('property_type'), "data": data}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        data = response.meta.get('data')
        
        
        
        from python_spiders.helper import ItemClear
        item_loader.add_value("external_source", self.external_source)
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value=data["address"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value=data["address"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value=data["city"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value=data["rent"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=data["external_id"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="USD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[div[contains(@class,'title_detail') and contains(.,'Bedroom')]]/text()[last()]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[div[contains(@class,'title_detail') and contains(.,'Bathroom')]]/text()[last()]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//i[contains(.,'Bond')]/@data-hover", input_type="F_XPATH", get_num=True, per_week=True, split_list={".":0})
        
        available_date = response.xpath("//h3[@class='d']/span/text()").get()
        if available_date:
            if "now" not in available_date.lower():
                date_parsed = dateparser.parse(available_date)
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
                
        desc = " ".join(response.xpath("//div[contains(@class,'entry-content')]/text() | //div[contains(@class,'entry-content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[div[contains(@class,'title_detail') and contains(.,'Garage')]]/text()[last()][not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//a[@data-fancybox='gallery']/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="VICTORIA REAL ESTATE AGENCY", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="(+613) 9380 5380", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@vicrea.com.au", input_type="VALUE")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residence" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    else:
        return None