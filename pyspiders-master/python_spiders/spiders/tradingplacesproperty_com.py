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
from word2number import w2n

class MySpider(Spider):
    name = 'tradingplacesproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.tradingplacesproperty.com/search?limit=20&includeDisplayAddress=No&auto-lat=&auto-lng=&p_department=RL&location=&propertyType=7%2C8%2C9%2C11%2C28&minimumRent=&maximumRent=&minimumBedrooms=0&searchRadius=&orderBy=price%2Bdesc&recentlyAdded=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.tradingplacesproperty.com/search?limit=20&includeDisplayAddress=No&active=&auto-lat=&auto-lng=&p_department=RL&propertyAge=&national=false&location=&propertyType=1%2C2%2C23&minimumRent=&maximumRent=&minimumBedrooms=0&maximumBedrooms=0&searchRadius=",
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='search-results']//div[@class='search-results-gallery-property']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            status = item.xpath(".//div[@class='corner_flash']/h2/text()[contains(.,'Let Agreed')]").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Tradingplacesproperty_PySpider_united_kingdom")
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("/")[-1])

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//div/h1/text()").get()
        if address:
            city = address.split(",")[-1]
            # zipcode = address.split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            # item_loader.add_value("zipcode", zipcode)
        zipcode=response.url
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("/")[-2].upper().replace("-"," "))

        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.split("£")[1].replace(",","").replace("pcm","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[@class='full_description_large']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[@class='type']/text()").get()
        if room_count:
            room_count = room_count.lower().split("bed")[0].strip()
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except :
                    pass

        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.lower().split("bath")[0]
            try:               
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
            except :
                pass
        
        images = response.xpath("//script[contains(.,'property-details-slider')]/text()").get()
        if images:
            images = images.split("null,")[1].split(', ["fade"],')[0].strip()
            data = json.loads(images)
            for d in data:
                item_loader.add_value("images", d["image"])

        parking = response.xpath("//li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        # floor = response.xpath("//ul[contains(@id,'property-features-list')]//li[contains(.,'Floor')]//text()").get()
        # if floor:
        #     floor = floor.split("Floor")[0].strip()
        #     item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//li[contains(.,'EPC')]/text()").get()
        if energy_label:
            energy_label = energy_label.split("-")[-1].strip()
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//div/@data-location").get()
        if latitude_longitude:     
            item_loader.add_value("longitude", latitude_longitude.split(",")[0])
            item_loader.add_value("latitude", latitude_longitude.split(",")[1])

        item_loader.add_value("landlord_name", "Trading Places")
        item_loader.add_value("landlord_phone", "020 8558 1147")
        item_loader.add_value("landlord_email", "info@tradingplacesproperty.com")
        
        yield item_loader.load_item()