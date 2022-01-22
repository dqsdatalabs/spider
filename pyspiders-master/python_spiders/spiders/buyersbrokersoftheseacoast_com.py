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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'buyersbrokersoftheseacoast_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.neren.com/search/ajaxresults/?sortOption=created%20desc&mapChanged=0&defaultPhoto=Default&class=Rental&PropertyTypeIn=Apartment&PropertyTypeIn=Duplex&BedsTotalMin=0&BathsTotalMin=0&CarsTotalMin=0&ListDateRelDategte=0&MLSStatusIn=Active&map=1&userClickedPagingLink=false",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.neren.com/search/ajaxresults/?sortOption=created%20desc&mapChanged=0&defaultPhoto=Default&class=Rental&PropertyTypeIn=House&PropertyTypeIn=Multi-Family&BedsTotalMin=0&BathsTotalMin=0&CarsTotalMin=0&ListDateRelDategte=0&MLSStatusIn=Active&map=1&userClickedPagingLink=false",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        seen = False
        data = json.loads(response.body)    
        body_html = Selector(text=data["Detail"], type="html")
 
        for url in body_html.xpath("//div[@class='card-media Listingcard__media']//a/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 1 or seen:            
            p_url = response.meta["base"].replace("MLSStatusIn=Active",f"MLSStatusIn=Active&page={page}")
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base": response.meta["base"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Buyersbrokersoftheseacoast_PySpider_united_kingdom") 
        
        item_loader.add_css("title", "title")
        
        street = response.xpath("//h1[@itemprop='streetAddress']/text()").get()
        city = response.xpath("//span[@itemprop='addressLocality']/text()").get()
        state = response.xpath("//span[@itemprop='addressRegion']/text()").get()
        zipcode = response.xpath("//span[@itemprop='postalCode']/text()").get()
        
        address = f"{street} {city} {state} {zipcode}"
        if address:
            item_loader.add_value("address", address.strip())
        
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//strong[@class='price']/text()").get()
        if rent:
            rent = rent.split("$")[1].strip().replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")
        
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        room_count = response.xpath("//div[@class='feature'][contains(.,'bed')]/span[1]/text()[.!='0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        if "studio" in description.lower():
            item_loader.add_value("room_count", "1")
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        bathroom_count = response.xpath("//div[@class='feature'][contains(.,'bath')]/span[1]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split("/")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//i[contains(@class,'housesqf')]/parent::div/span[1]/text()").get()
        if square_meters:
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        external_id = response.xpath("substring-after(//p[contains(@class,'LDP-mlsid')]/text()[1],'#')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        garage = response.xpath("//div[@class='feature'][contains(.,'garage')]/span[1]/text()[.!='0']").get()
        if garage:
            item_loader.add_value("parking", True)
        
        floor = response.xpath("//strong[contains(.,'Level')]/following-sibling::span/text()").get()
        if floor:
            if floor.isdigit():
                item_loader.add_value("floor", floor)
            else: item_loader.add_value("floor", floor.split(".")[0])
        
        images = response.xpath("//script[contains(.,'srcThumb:')]/text()").get()
        if images:
            img = images.split("srcThumb: '")
            for i in range(1,len(img)):
                item_loader.add_value("images", response.urljoin(img[i].split("'")[0]))
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split(',')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'middle ]')]//h6[@class='name']/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'middle ]')]//p/text()")
        
        yield item_loader.load_item()