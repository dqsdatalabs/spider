# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re


class MySpider(Spider):
    name = 'gesthome_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://gesthome.com/trouver-un-bien/?sort=newest&search_city=&search_lat=&search_lng=&search_category=3&search_type=4&search_min_price=&search_max_price=&search_min_area=&search_max_area=&search_bedrooms=0&ref=&ref_comparison=equal",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://gesthome.com/trouver-un-bien/?sort=newest&search_city=&search_lat=&search_lng=&search_category=5&search_type=4&search_min_price=&search_max_price=&search_min_area=&search_max_area=&search_bedrooms=0&ref=&ref_comparison=equal",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "url":item})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='card']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["url"]
            p_url = f"https://gesthome.com/trouver-un-bien/page/{page}/?" + base_url.split("?")[1]
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,
                meta={'property_type': response.meta['property_type'], "page":page+1, "url":base_url}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Gesthome_PySpider_"+ self.country)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        
        title = response.xpath("//div[@class='summaryItem']//h1//text()").get()
        if title:
            item_loader.add_value("title",title.strip())
            if "BALCON" in title.upper():
                item_loader.add_value("balcony",True)
            if "PARKING" in title.upper():
                item_loader.add_value("parking",True)
            if "meublé" in title.lower():
                item_loader.add_value("furnished",True)
        else:
            title = response.xpath("//title/text()").get()
            if title:
                item_loader.add_value("title",title.strip())
                
        address = "".join(response.xpath("//div[@class='summaryItem']/div[@class='address']/text()").getall())
        if address:
            address = address.strip().strip(",")
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",")
            if "," in city:
                item_loader.add_value("city", city.split(",")[-1].strip())
            else:
                item_loader.add_value("city", city.strip())
                
            item_loader.add_value("zipcode", zipcode)
        
        rent = "".join(response.xpath("//div[@class='summaryItem']//div[@class='listPrice']//text()[not(contains(.,'Location'))][normalize-space()]").getall())
        if rent:
            price = rent.split("€")[0].strip()
            if price:
                item_loader.add_value("rent", price.replace(" ",""))
        
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//div[@class='summaryItem']/ul[@class='features']/li[contains(.,'Pièce')]//text()").get()
        if room_count:
            if room_count.split(" ")[0].strip() != "0":
                item_loader.add_value("room_count", room_count.split(" ")[0].strip())
        
        square_meters = response.xpath("//div[@class='summaryItem']/ul[@class='features']/li[contains(.,'m²')]//text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", math.ceil(float(square_meters)))
        
        external_id = response.xpath("//div/strong[contains(.,'Référence')]/parent::div/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        desc = "".join(response.xpath("//div[@class='entry-content']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//div/strong[contains(.,'Classe Energétique')]/parent::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip())
        
        utilities = response.xpath("//div/strong[contains(.,'charges')]/parent::div/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip())
        
        images = [ x for x in response.xpath("//a[contains(@class,'galleryItem')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "GESTHOME")
        
        phone = response.xpath("//ul/li[@class='widget-phone']//text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        
        
        yield item_loader.load_item()