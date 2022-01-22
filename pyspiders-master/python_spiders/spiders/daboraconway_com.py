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
    name = 'daboraconway_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Daboraconway_PySpider_united_kingdom'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.daboraconway.com/search/1.html?instruction_type=Letting&showstc=on&showsold=on&address_keyword=&bid=&minprice=&maxprice=&property_type=Apartment%2CFlat%2CGround+Flat%2CStudio%2CGround+Floor+Flat"
                ], 
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.daboraconway.com/search/1.html?instruction_type=Letting&showstc=on&showsold=on&address_keyword=&bid=&minprice=&maxprice=&property_type=Detached",
                    "https://www.daboraconway.com/search/1.html?instruction_type=Letting&showstc=on&showsold=on&address_keyword=&bid=&minprice=&maxprice=&property_type=Semi-Detached",
                    "https://www.daboraconway.com/search/1.html?instruction_type=Letting&showstc=on&showsold=on&address_keyword=&bid=&minprice=&maxprice=&property_type=Terraced%2CEnd+Terrace",
                    "https://www.daboraconway.com/search/1.html?instruction_type=Letting&showstc=on&showsold=on&address_keyword=&bid=&minprice=&maxprice=&property_type=Maisonette",
                    "https://www.daboraconway.com/search/1.html?instruction_type=Letting&showstc=on&showsold=on&address_keyword=&bid=&minprice=&maxprice=&property_type=Bungalow"
                ], 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(url=item,
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//div[@class='property']//h4//@href[contains(.,'property')]").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"search/{page-1}.html", f"search/{page}.html")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[contains(@id,'property-carousel')]//text()[contains(.,'LET')]").get()
        if status:
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1/text()").get()
        if address:
            address = address.split("-")[0]
            if "," in address:
                city = address.split(",")[0].strip()
                zipcode = address.split(",")[-1].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
            else:
                item_loader.add_value("city", address)
            item_loader.add_value("address", address.strip())

        rent = response.xpath("//h1/text()").get()
        if rent:
            rent = rent.split("-")[-1].replace("£","").strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@id,'property-details-tab-panes')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip().replace("*",""))
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'room-icons')]//span//img[contains(@src,'bed')]//parent::span/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'room-icons')]//span//img[contains(@src,'bath')]//parent::span/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'property-carousel')]//@src[contains(.,'resize')]").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//script[contains(.,'googlemap')]//text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('googlemap", "')[1].split('"')[0]
            latitude = latitude_longitude.split('&q=')[1].split('%2C')[0]
            longitude = latitude_longitude.split('&q=')[1].split("%2C")[1].split('"')[0].replace("-","")    
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "DABORA CONWAY")
        item_loader.add_value("landlord_phone", "020 8530 7200")
        item_loader.add_value("landlord_email", "E18@daboraconway.com")

        yield item_loader.load_item()