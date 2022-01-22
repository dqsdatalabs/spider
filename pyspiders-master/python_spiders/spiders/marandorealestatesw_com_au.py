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
    name = 'marandorealestatesw_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    
    url = "http://marandorealestatesw.com.au/results"
    formdata = {
        'pagenum': '0',
        'paramstype': 'rental',
        'order': 'desc',
        'searchorder': 'modtime',
        'propertytype': ''
    }
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'Origin': 'http://marandorealestatesw.com.au',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': 'http://marandorealestatesw.com.au/results',
        'Accept-Language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        property_types = [
            {
                "types" : ["Apartment", "Unit", "Flat"],
                "property_type" : "apartment",
            },
            {
                "types" : ["House", "Townhouse", "Villa", "DuplexSemi-detached", "Terrace"],
                "property_type" : "house"
            },
            {
                "types" : ["Studio"],
                "property_type" : "studio"
            },
        ]
        for item in property_types:
            for item_type in item.get("types"):
                self.formdata["propertytype"] = item_type
                yield FormRequest(self.url, 
                                    headers=self.headers, 
                                    formdata=self.formdata, 
                                    dont_filter=True, 
                                    callback=self.parse, 
                                    meta={'property_type': item.get('property_type'), 'item_type': item_type})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)
        seen = False

        for item in response.xpath("//div[@class='listing-item']//a[contains(.,'Read more')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 1 or seen:
            self.formdata["propertytype"] = response.meta["item_type"]
            self.formdata["pagenum"] = str(page)
            yield FormRequest(self.url, 
                                headers=self.headers, 
                                formdata=self.formdata, 
                                dont_filter=True, 
                                callback=self.parse, 
                                meta={'property_type': response.meta["property_type"], 'item_type': response.meta["item_type"], 'page': page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Marandorealestatesw_Com_PySpider_australia")
        
        title = response.xpath("//h2//text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        location = response.xpath("//script[contains(.,'locations')]/text()").get()
        if location:
            address = location.split("['")[1].split("'")[0].strip()
            item_loader.add_value("address", address)
            
            city = address.split(" ")
            item_loader.add_value("city", city[-2]+" "+city[-1])
            
            latitude = location.split("['")[1].split("',")[1].split(",")[0].strip()
            item_loader.add_value("latitude", latitude)
            
            longitude = location.split("['")[1].split("',")[1].split(",")[1].strip()
            item_loader.add_value("longitude", longitude)

        rent = response.xpath("//h3//text()").get()
        if rent:
            price = rent.strip().split(" ")[0].replace("$","")
            item_loader.add_value("rent", int(price)*4)
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//img[contains(@src,'bed')]/preceding-sibling::h6//text()").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//img[contains(@src,'bath')]/preceding-sibling::h6[1]//text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = " ".join(response.xpath("//div[@class='property_description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        not_list = ["timb","stunn","and"]
        if "floor " in desc.lower():
            floor = desc.lower().split("floor ")[0].strip().split(" ")[-1]
            status = True
            for i in not_list:
                if i in floor:
                    status = False
            if status:
                item_loader.add_value("floor", floor)
        
        images = [x for x in response.xpath("//div[@id='gallerychange']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = response.xpath("//div[@class='property_description']//p//text()[contains(.,'ID')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        balcony = response.xpath("//div[@class='property_description']//p//text()[contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//img[contains(@src,'car')]/preceding-sibling::h6//text()").get()
        if parking and "0" not in parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_xpath("landlord_name", "//div[@class='agent_details']/span/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent_details']/text()[2]")
        item_loader.add_value("landlord_email", "sales@marandorealestatesw.com.au")

        yield item_loader.load_item()