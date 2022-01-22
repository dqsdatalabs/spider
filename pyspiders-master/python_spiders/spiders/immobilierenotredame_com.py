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


class MySpider(Spider):
    name = 'immobilierenotredame_com'
    execution_type='testing'
    country='france'
    locale='fr'

    custom_settings = {
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "FEED_EXPORT_ENCODING" : "utf-8",
 
    }
    
    def start_requests(self):
        start_urls = [
            {"url": "http://www.immobilierenotredame.com/recherche/"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='listingUL']/li"):
            follow_url = response.urljoin(item.xpath(".//h1/a/@href").get())
            prop_type = item.xpath(".//h2/text()").get()
            property_type = ""
            if "appartement" in prop_type.lower():
                property_type = "apartment"
            elif "maison" in prop_type.lower():
                property_type = "house"
            elif "studio" in prop_type.lower():
                property_type = "apartment"
            elif "duplex" in prop_type.lower():
                property_type = "apartment"
            elif "villa" in prop_type.lower():
                property_type = "house"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        detail_head = response.xpath("//head/title/text()").get()
        if "location" in detail_head:
            title = "".join(response.xpath("//div[@class='container']//h1//text()").extract())
            item_loader.add_value("title", title.strip())
            item_loader.add_value("property_type", response.meta.get('property_type'))

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", "Immobilierenotredame_PySpider_"+ self.country + "_" + self.locale)

            price = response.xpath("//span[@class='prix']/text()[normalize-space()]").extract_first()
            if price:
                item_loader.add_value("rent_string", price.replace(" ",""))

            external_id = response.xpath("//span[@class='ref']/text()").extract_first()
            if external_id:
                item_loader.add_value("external_id", external_id.split("Ref")[1].strip())
            
            room_count = response.xpath("//p[@class='data']/span[contains(.,'chambre')]/following-sibling::span/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())
            
            bathroom_count = response.xpath("//p[@class='data']/span[contains(.,'salle ')]/following-sibling::span/text()").extract_first()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip())
        
            square = response.xpath("//p[@class='data']/span[contains(.,'Surface habitable')]/following-sibling::span/text()").extract_first()
            if square:
                square_meters=square.split("m")[0]
                item_loader.add_value("square_meters",square_meters.strip() )
            
            floor = response.xpath("//p[@class='data']/span[contains(.,'Etage')]/following-sibling::span/text()").extract_first()
            if floor:
                item_loader.add_value("floor",floor.strip() )
            
            deposit = response.xpath("//p[@class='data']/span[contains(.,'Dépôt de garantie')]/following-sibling::span/text()").extract_first()
            if deposit :
                item_loader.add_value("deposit",deposit.split("€")[0].strip() )

            utilities = response.xpath("//p[@class='data']/span[contains(.,'charge')]/following-sibling::span/text()").extract_first()
            if utilities :
                item_loader.add_value("utilities",utilities.split("€")[0].strip() )
                
            desc = "".join(response.xpath("//p[@itemprop='description']//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())
            
            zipcode = ""
            zipcode = response.xpath("//p[@class='data']/span[contains(.,'Code postal')]/following-sibling::span/text()").extract_first()
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())
            
            city = response.xpath("normalize-space(//p[@class='data']/span[contains(.,'Ville')]/following-sibling::span/text())").extract_first()
            if city:
                item_loader.add_value("city", city.strip())
                item_loader.add_value("address",city.strip())
            else:
                item_loader.add_xpath("address" , "//ol//a[contains(@href,'ville')]/text()")

            parking = response.xpath("//p[@class='data']/span[contains(.,'parking')]/following-sibling::span/text()").extract_first()
            if parking:
                item_loader.add_value("parking", True)
            
            furnished = response.xpath("//p[@class='data']/span[contains(.,'Meublé')]/following-sibling::span/text()").extract_first()
            if furnished:
                if "NON" in furnished.upper():
                    item_loader.add_value("furnished", False)
                else:
                    item_loader.add_value("furnished", True)
            elevator = response.xpath("//p[@class='data']/span[contains(.,'Ascenseur')]/following-sibling::span/text()").extract_first()
            if elevator:
                if "NON" in elevator:
                    item_loader.add_value("elevator", False)
                else:
                    item_loader.add_value("elevator", True)
            
            images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'imageGallery')]/li/img/@src").extract()]
            if images is not None:
                item_loader.add_value("images", images)      

            item_loader.add_value("landlord_phone", "01 39 51 48 22")
            item_loader.add_value("landlord_email", "ndame@club-internet.fr")
            item_loader.add_value("landlord_name", "Agence immobilière Notre-Dame")
            
            yield item_loader.load_item()