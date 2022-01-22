# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re
import dateparser

class MySpider(Spider):
    name = 'galwaypropertymanagement_ie'
    execution_type='testing'
    country='ireland'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    external_source= "Galwaypropertymanagement_PySpider_ireland"    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://galwaypropertymanagement.ie/available-accommodation/",
                ],
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//span[@class='es-read-wrap']//a[contains(.,'Details')]/@href").extract():
            yield Request(response.urljoin(item), callback=self.populate_item)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='container']//h1[@class='entry-title']/text()").get()
        if status and "let agreed" in status.lower():
            return  
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        # item_loader.add_value("external_id", response.url.split("id=")[1])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//div[@class='elive_property_addetail_header'][1]/h2//text()")
        address = " ".join(response.xpath("//div[@class='elive_property_addetail_header'][1]/h2//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].replace("Co.","").strip())

        room_count = response.xpath("//div[@class='elive_property_addetail_rest_header']/span[contains(.,'Bedroom')]/text()").get()
        if room_count:                   
            item_loader.add_value("room_count",room_count.split("Bedroom")[0].strip())
        bathroom_count = response.xpath("//div[@class='elive_property_addetail_rest_header']/span[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:                   
            item_loader.add_value("bathroom_count",bathroom_count.split("Bathroom")[0].strip())
        rent = "".join(response.xpath("//span[@class='elive_property_addetail_price']//text()").getall())
        if rent:              
            item_loader.add_value("rent_string", rent) 
    
        description = " ".join(response.xpath("//div[@class='elive_property_addetail_propdescr_text']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        energy_label = response.xpath("//div[@class='elive_property_addetail_rest_header']/span[contains(.,'BER Rating:')]/strong/text()[.!='Exempt']").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())
        lat_lng = response.xpath("//div[@class='elive_property_addetail_map']/a/@href").get()
        if lat_lng:
            lat_lng = lat_lng.split("/?q=")[1].split("&")[0].strip()
            item_loader.add_value("latitude", lat_lng.split(",")[0].strip())
            item_loader.add_value("longitude", lat_lng.split(",")[1].strip())

        available_date = response.xpath("//div[@class='elive_property_addetail_overview']//div[span[contains(.,'Available to ')]]/span[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Immediately","now").strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@class='elive_property_addetail_thumbnails_list_imgcont']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//div[@class='elive_property_addetail_overview']//li[contains(.,'Furnished') or contains(.,'furnished')]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
  
        item_loader.add_value("landlord_phone", "091 531500")
        item_loader.add_value("landlord_name", "Galway Property Management")
        item_loader.add_value("landlord_email", "info@galwaypm.com")
        
        yield item_loader.load_item()